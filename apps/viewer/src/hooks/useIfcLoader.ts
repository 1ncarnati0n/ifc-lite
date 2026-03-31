/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

/**
 * Hook for loading and processing IFC files (single-model path)
 * Handles format detection, WASM geometry streaming, IFC parsing,
 * cache management, and server-side parsing delegation
 *
 * Extracted from useIfc.ts for better separation of concerns
 */

import { useCallback, useRef } from 'react';
import { useShallow } from 'zustand/react/shallow';
import { useViewerStore } from '../store.js';
import { IfcParser, detectFormat, parseIfcx, type IfcDataStore } from '@ifc-lite/parser';
import { GeometryProcessor, GeometryQuality, type MeshData, type CoordinateInfo } from '@ifc-lite/geometry';
import { buildSpatialIndexGuarded } from '../utils/loadingUtils.js';
import { type GeometryData, loadGLBToMeshData } from '@ifc-lite/cache';

import { SERVER_URL, USE_SERVER, CACHE_SIZE_THRESHOLD, CACHE_MAX_SOURCE_SIZE, getDynamicBatchConfig } from '../utils/ifcConfig.js';
import {
  calculateMeshBounds,
  createCoordinateInfo,
  getRenderIntervalMs,
  calculateStoreyHeights,
  normalizeColor,
} from '../utils/localParsingUtils.js';
import { applyColorUpdatesToMeshes } from './meshColorUpdates.js';
import { readNativeFile, type NativeFileHandle } from '../services/file-dialog.js';
import { finalizeActiveHarnessRun, getActiveHarnessRequest } from '../services/desktop-harness.js';
import { logToDesktopTerminal } from '../services/desktop-logger.js';

// Cache hook
import { useIfcCache, getCached } from './useIfcCache.js';

// Server hook
import { useIfcServer } from './useIfcServer.js';

// Import IfcxDataStore type from federation hook
import type { IfcxDataStore } from './useIfcFederation.js';

/**
 * Compute a fast content fingerprint from the first and last 4KB of a buffer.
 * Uses FNV-1a hash for speed — no crypto overhead, sufficient to distinguish
 * files with identical name and byte length.
 */
function computeFastFingerprint(buffer: ArrayBuffer): string {
  const CHUNK_SIZE = 4096;
  const view = new Uint8Array(buffer);
  const len = view.length;

  // FNV-1a hash
  let hash = 2166136261; // FNV offset basis (32-bit)
  const firstEnd = Math.min(CHUNK_SIZE, len);
  for (let i = 0; i < firstEnd; i++) {
    hash ^= view[i];
    hash = Math.imul(hash, 16777619); // FNV prime
  }
  if (len > CHUNK_SIZE) {
    const lastStart = Math.max(CHUNK_SIZE, len - CHUNK_SIZE);
    for (let i = lastStart; i < len; i++) {
      hash ^= view[i];
      hash = Math.imul(hash, 16777619);
    }
  }
  return (hash >>> 0).toString(16);
}

function isNativeFileHandle(file: File | NativeFileHandle): file is NativeFileHandle {
  return typeof (file as NativeFileHandle).path === 'string';
}

/**
 * Hook providing file loading operations for single-model path
 * Includes binary cache support for fast subsequent loads
 */
export function useIfcLoader() {
  // Guard against stale async writes when user loads a new file before previous completes.
  // Incremented on each loadFile call; deferred callbacks check their captured session.
  const loadSessionRef = useRef(0);

  const {
    setLoading,
    setError,
    setProgress,
    setIfcDataStore,
    setGeometryResult,
    appendGeometryBatch,
    updateMeshColors,
    updateCoordinateInfo,
  } = useViewerStore(useShallow((s) => ({
    setLoading: s.setLoading,
    setError: s.setError,
    setProgress: s.setProgress,
    setIfcDataStore: s.setIfcDataStore,
    setGeometryResult: s.setGeometryResult,
    appendGeometryBatch: s.appendGeometryBatch,
    updateMeshColors: s.updateMeshColors,
    updateCoordinateInfo: s.updateCoordinateInfo,
  })));

  // Cache operations from extracted hook
  const { loadFromCache, saveToCache } = useIfcCache();

  // Server operations from extracted hook
  const { loadFromServer } = useIfcServer();

  const loadFile = useCallback(async (file: File | NativeFileHandle) => {
    const { resetViewerState, clearAllModels } = useViewerStore.getState();
    const currentSession = ++loadSessionRef.current;

    // Track total elapsed time for complete user experience
    const totalStartTime = performance.now();

    try {
      // Reset all viewer state before loading new file
      // Also clear models Map to ensure clean single-file state
      resetViewerState();
      clearAllModels();

      setLoading(true);
      setError(null);
      setProgress({ phase: 'Loading file', percent: 0 });

      const fileName = file.name;
      const fileSize = file.size;
      const fileSizeMB = fileSize / (1024 * 1024);

      // Desktop fast path: keep huge IFC files out of browser memory and Tauri IPC.
      if (isNativeFileHandle(file) && fileName.toLowerCase().endsWith('.ifc')) {
        const harnessRequest = getActiveHarnessRequest();
        console.log(`[useIfc] Native path load: ${fileName}, size: ${fileSizeMB.toFixed(2)}MB`);
        void logToDesktopTerminal('info', `[useIfc] Native path load start: ${fileName} (${fileSizeMB.toFixed(2)} MB) path=${file.path}`);
        setIfcDataStore(null);
        setProgress({ phase: 'Starting native geometry streaming', percent: 10 });

        const geometryProcessor = new GeometryProcessor({
          quality: GeometryQuality.Balanced
        });
        await geometryProcessor.init();
        void logToDesktopTerminal('info', `[useIfc] GeometryProcessor.init complete for ${fileName}`);

        let estimatedTotal = 0;
        let totalMeshes = 0;
        const allMeshes: MeshData[] = [];
        let finalCoordinateInfo: CoordinateInfo | null = null;
        let batchCount = 0;
        let modelOpenMs: number | null = null;
        let firstGeometryTime = 0;
        let firstAppendGeometryBatchMs: number | null = null;
        let firstVisibleGeometryMs: number | null = null;
        let jsFirstChunkReceivedMs: number | null = null;
        let lastTotalMeshes = 0;
        let pendingMeshes: MeshData[] = [];
        let lastRenderTime = 0;
        let streamCompleteMs: number | null = null;
        let metadataStartMs: number | null = null;
        let spatialReadyMs: number | null = null;
        let metadataCompleteMs: number | null = null;
        let metadataFailedMs: number | null = null;
        let metadataParsingPromise: Promise<void> | null = null;
        let firstNativeBatchTelemetry: {
          batchSequence: number;
          payloadKind: string;
          meshCount: number;
          positionsLen: number;
          normalsLen: number;
          indicesLen: number;
          chunkReadyTimeMs: number;
          packTimeMs: number;
          emittedTimeMs: number;
          emitTimeMs: number;
          jsReceivedTimeMs?: number;
        } | null = null;
        let nativeStats: {
          parseTimeMs?: number;
          entityScanTimeMs?: number;
          lookupTimeMs?: number;
          preprocessTimeMs?: number;
          geometryTimeMs?: number;
          totalTimeMs?: number;
          firstChunkReadyTimeMs?: number;
          firstChunkPackTimeMs?: number;
          firstChunkEmittedTimeMs?: number;
          firstChunkEmitTimeMs?: number;
        } | null = null;
        const RENDER_INTERVAL_MS = getRenderIntervalMs(fileSizeMB);
        const NATIVE_PENDING_MESH_THRESHOLD = fileSizeMB > 512 ? 4000 : fileSizeMB > 256 ? 2000 : fileSizeMB > 100 ? 1000 : 400;
        let metadataParsingStarted = false;
        let geometryCompleted = false;
        let fullNativeDataStore: IfcDataStore | null = null;

        setGeometryResult(null);

        const maybeBuildNativeSpatialIndex = () => {
          if (!geometryCompleted || !fullNativeDataStore || allMeshes.length === 0 || loadSessionRef.current !== currentSession) {
            return;
          }
          buildSpatialIndexGuarded(allMeshes, fullNativeDataStore, setIfcDataStore);
        };

        const markFirstVisibleGeometry = () => {
          if (firstVisibleGeometryMs !== null) return;
          requestAnimationFrame(() => {
            if (firstVisibleGeometryMs !== null || loadSessionRef.current !== currentSession) return;
            firstVisibleGeometryMs = performance.now() - totalStartTime;
            void logToDesktopTerminal(
              'info',
              `[useIfc] Native first visible geometry for ${fileName}: ${firstVisibleGeometryMs.toFixed(0)}ms`
            );
          });
        };

        const startNativeMetadataParsing = (): Promise<void> | null => {
          if (metadataParsingStarted) return metadataParsingPromise;
          metadataParsingStarted = true;
          const metadataStartTime = performance.now();
          metadataStartMs = metadataStartTime - totalStartTime;
          void logToDesktopTerminal('info', `[useIfc] Native metadata parse start for ${fileName}`);

          const parser = new IfcParser();
          metadataParsingPromise = readNativeFile(file.path)
            .then((bytes) => {
              const metadataBuffer = new Uint8Array(bytes.byteLength);
              metadataBuffer.set(bytes);
              return parser.parseColumnar(metadataBuffer.buffer, {
                onSpatialReady: (partialStore) => {
                  if (loadSessionRef.current !== currentSession) return;
                  if (spatialReadyMs === null) {
                    spatialReadyMs = performance.now() - totalStartTime;
                  }
                  if (partialStore.spatialHierarchy && partialStore.spatialHierarchy.storeyHeights.size === 0 && partialStore.spatialHierarchy.storeyElevations.size > 1) {
                    const calculatedHeights = calculateStoreyHeights(partialStore.spatialHierarchy.storeyElevations);
                    for (const [storeyId, height] of calculatedHeights) {
                      partialStore.spatialHierarchy.storeyHeights.set(storeyId, height);
                    }
                  }
                  setIfcDataStore(partialStore);
                  void logToDesktopTerminal(
                    'info',
                    `[useIfc] Native spatial tree ready for ${fileName} at ${(performance.now() - totalStartTime).toFixed(0)}ms`
                  );
                },
              });
            })
            .then((dataStore) => {
              if (loadSessionRef.current !== currentSession) return;
              metadataCompleteMs = performance.now() - totalStartTime;
              if (dataStore.spatialHierarchy && dataStore.spatialHierarchy.storeyHeights.size === 0 && dataStore.spatialHierarchy.storeyElevations.size > 1) {
                const calculatedHeights = calculateStoreyHeights(dataStore.spatialHierarchy.storeyElevations);
                for (const [storeyId, height] of calculatedHeights) {
                  dataStore.spatialHierarchy.storeyHeights.set(storeyId, height);
                }
              }
              fullNativeDataStore = dataStore;
              setIfcDataStore(dataStore);
              maybeBuildNativeSpatialIndex();
              void logToDesktopTerminal(
                'info',
                `[useIfc] Native metadata parse complete for ${fileName}: ${(performance.now() - metadataStartTime).toFixed(0)}ms`
              );
            })
            .catch((error) => {
              if (loadSessionRef.current !== currentSession) return;
              metadataFailedMs = performance.now() - totalStartTime;
              console.warn('[useIfc] Native metadata parsing failed:', error);
              void logToDesktopTerminal(
                'warn',
                `[useIfc] Native metadata parse failed for ${fileName}: ${error instanceof Error ? error.message : String(error)}`
              );
            });
          return metadataParsingPromise;
        };

        for await (const event of geometryProcessor.processStreamingPath(file.path, file.size)) {
          const eventReceived = performance.now();

          switch (event.type) {
            case 'start':
              estimatedTotal = event.totalEstimate;
              void logToDesktopTerminal('info', `[useIfc] Native stream start for ${fileName}: estimate=${Math.round(estimatedTotal)}`);
              break;
            case 'model-open':
              setProgress({ phase: 'Processing geometry (native precompute)', percent: 50, indeterminate: true });
              modelOpenMs = performance.now() - totalStartTime;
              console.log(`[useIfc] Native model opened at ${modelOpenMs.toFixed(0)}ms`);
              void logToDesktopTerminal('info', `[useIfc] Native model opened for ${fileName} at ${modelOpenMs.toFixed(0)}ms`);
              break;
            case 'batch': {
              batchCount++;

              if (batchCount === 1) {
                firstGeometryTime = performance.now() - totalStartTime;
                jsFirstChunkReceivedMs = event.nativeTelemetry?.jsReceivedTimeMs ?? firstGeometryTime;
                firstNativeBatchTelemetry = event.nativeTelemetry ?? null;
                console.log(`[useIfc] Native batch #1: ${event.meshes.length} meshes, wait: ${firstGeometryTime.toFixed(0)}ms`);
                void logToDesktopTerminal('info', `[useIfc] Native first batch for ${fileName}: meshes=${event.meshes.length}, wait=${firstGeometryTime.toFixed(0)}ms`);
                if (event.nativeTelemetry) {
                  const transferLagMs = (event.nativeTelemetry.jsReceivedTimeMs ?? 0) - event.nativeTelemetry.emittedTimeMs;
                  void logToDesktopTerminal(
                    'info',
                    `[useIfc] Native first batch transport for ${fileName}: rustReady=${event.nativeTelemetry.chunkReadyTimeMs.toFixed(0)}ms pack=${event.nativeTelemetry.packTimeMs.toFixed(0)}ms emit=${event.nativeTelemetry.emitTimeMs.toFixed(0)}ms rustEmitted=${event.nativeTelemetry.emittedTimeMs.toFixed(0)}ms jsReceived=${(event.nativeTelemetry.jsReceivedTimeMs ?? 0).toFixed(0)}ms transfer=${transferLagMs.toFixed(0)}ms`
                  );
                }
                startNativeMetadataParsing();
              } else if (batchCount % 20 === 0) {
                void logToDesktopTerminal('info', `[useIfc] Native batch milestone for ${fileName}: batch=${batchCount}, totalMeshes=${event.totalSoFar}`);
              }

              for (let i = 0; i < event.meshes.length; i++) allMeshes.push(event.meshes[i]);
              finalCoordinateInfo = event.coordinateInfo ?? null;
              totalMeshes = event.totalSoFar;
              lastTotalMeshes = event.totalSoFar;

              for (let i = 0; i < event.meshes.length; i++) pendingMeshes.push(event.meshes[i]);

              const timeSinceLastRender = eventReceived - lastRenderTime;
              const shouldRender =
                batchCount === 1 ||
                pendingMeshes.length >= NATIVE_PENDING_MESH_THRESHOLD ||
                timeSinceLastRender >= RENDER_INTERVAL_MS;

              if (shouldRender && pendingMeshes.length > 0) {
                if (firstAppendGeometryBatchMs === null) {
                  firstAppendGeometryBatchMs = performance.now() - totalStartTime;
                  void logToDesktopTerminal(
                    'info',
                    `[useIfc] Native first appendGeometryBatch for ${fileName}: ${firstAppendGeometryBatchMs.toFixed(0)}ms`
                  );
                }
                appendGeometryBatch(pendingMeshes, event.coordinateInfo);
                pendingMeshes = [];
                lastRenderTime = eventReceived;
                markFirstVisibleGeometry();

                const progressPercent = 50 + Math.min(45, (totalMeshes / Math.max(estimatedTotal / 10, totalMeshes || 1)) * 45);
                setProgress({
                  phase: `Rendering geometry (${totalMeshes} meshes)`,
                  percent: progressPercent,
                  indeterminate: false,
                });
              }
              break;
            }
            case 'complete':
              geometryCompleted = true;
              streamCompleteMs = performance.now() - totalStartTime;
              if (pendingMeshes.length > 0) {
                if (firstAppendGeometryBatchMs === null) {
                  firstAppendGeometryBatchMs = performance.now() - totalStartTime;
                  void logToDesktopTerminal(
                    'info',
                    `[useIfc] Native first appendGeometryBatch for ${fileName}: ${firstAppendGeometryBatchMs.toFixed(0)}ms`
                  );
                }
                appendGeometryBatch(pendingMeshes, event.coordinateInfo);
                pendingMeshes = [];
                markFirstVisibleGeometry();
              }

              finalCoordinateInfo = event.coordinateInfo;
              updateCoordinateInfo(finalCoordinateInfo);
              maybeBuildNativeSpatialIndex();
              setProgress({ phase: 'Complete', percent: 100 });
              console.log(`[useIfc] Native geometry streaming complete: ${batchCount} batches, ${lastTotalMeshes} meshes`);
              void logToDesktopTerminal('info', `[useIfc] Native stream complete for ${fileName}: batches=${batchCount}, meshes=${lastTotalMeshes}`);
              break;
          }
        }

        nativeStats = geometryProcessor.getLastNativeStats();

        const totalElapsedMs = performance.now() - totalStartTime;
        const totalVertices = allMeshes.reduce((sum, m) => sum + m.positions.length / 3, 0);
        console.log(
          `[useIfc] ✓ ${fileName} (${fileSizeMB.toFixed(1)}MB) → ` +
          `${allMeshes.length} meshes, ${(totalVertices / 1000).toFixed(0)}k vertices | ` +
          `first: ${firstGeometryTime.toFixed(0)}ms, total: ${totalElapsedMs.toFixed(0)}ms`
        );
        if (nativeStats) {
          void logToDesktopTerminal(
            'info',
            `[useIfc] Native timings for ${fileName}: scan=${nativeStats.entityScanTimeMs ?? 0}ms lookup=${nativeStats.lookupTimeMs ?? 0}ms preprocess=${nativeStats.preprocessTimeMs ?? 0}ms parse=${nativeStats.parseTimeMs ?? 0}ms geometry=${nativeStats.geometryTimeMs ?? 0}ms total=${nativeStats.totalTimeMs ?? 0}ms`
          );
        }
        if (!metadataParsingStarted) {
          console.warn('[useIfc] Native large-file mode completed without metadata parsing');
          void logToDesktopTerminal('warn', `[useIfc] Native large-file mode completed without metadata parsing for ${fileName}`);
        }
        if (harnessRequest?.waitForMetadataCompletion) {
          if (!metadataParsingStarted) {
            startNativeMetadataParsing();
          }
          if (metadataParsingPromise) {
            await metadataParsingPromise;
          }
        }
        if (firstVisibleGeometryMs === null && firstAppendGeometryBatchMs !== null) {
          await new Promise<void>((resolve) => {
            const fallbackTimer = globalThis.setTimeout(() => {
              if (firstVisibleGeometryMs === null && loadSessionRef.current === currentSession) {
                firstVisibleGeometryMs = firstAppendGeometryBatchMs;
              }
              resolve();
            }, 250);
            requestAnimationFrame(() => {
              globalThis.clearTimeout(fallbackTimer);
              if (firstVisibleGeometryMs === null && loadSessionRef.current === currentSession) {
                firstVisibleGeometryMs = performance.now() - totalStartTime;
              }
              resolve();
            });
          });
        }
        const telemetryElapsedMs = performance.now() - totalStartTime;
        await finalizeActiveHarnessRun({
          schemaVersion: 1,
          source: 'desktop-native',
          mode: harnessRequest ? 'startup-harness' : 'manual',
          success: true,
          runLabel: harnessRequest?.runLabel,
          file: {
            path: file.path,
            name: file.name,
            sizeBytes: file.size,
            sizeMB: fileSizeMB,
          },
          timings: {
            modelOpenMs,
            firstBatchWaitMs: firstGeometryTime || null,
            firstAppendGeometryBatchMs,
            firstVisibleGeometryMs,
            streamCompleteMs,
            totalWallClockMs: telemetryElapsedMs,
            metadataStartMs,
            spatialReadyMs,
            metadataCompleteMs,
            metadataFailedMs,
          },
          batches: {
            estimatedTotal,
            totalBatches: batchCount,
            totalMeshes: lastTotalMeshes,
            firstBatchMeshes: firstNativeBatchTelemetry?.meshCount ?? null,
            firstPayloadKind: firstNativeBatchTelemetry?.payloadKind ?? null,
          },
          nativeStats: nativeStats
            ? {
                parseTimeMs: nativeStats.parseTimeMs ?? null,
                entityScanTimeMs: nativeStats.entityScanTimeMs ?? null,
                lookupTimeMs: nativeStats.lookupTimeMs ?? null,
                preprocessTimeMs: nativeStats.preprocessTimeMs ?? null,
                geometryTimeMs: nativeStats.geometryTimeMs ?? null,
                totalTimeMs: nativeStats.totalTimeMs ?? null,
                firstChunkReadyTimeMs: nativeStats.firstChunkReadyTimeMs ?? null,
                firstChunkPackTimeMs: nativeStats.firstChunkPackTimeMs ?? null,
                firstChunkEmittedTimeMs: nativeStats.firstChunkEmittedTimeMs ?? null,
                firstChunkEmitTimeMs: nativeStats.firstChunkEmitTimeMs ?? null,
              }
            : null,
          metadata: {
            started: metadataParsingStarted,
            metadataStartMs,
            spatialReadyMs,
            metadataCompleteMs,
            metadataFailedMs,
          },
          firstBatchTelemetry: firstNativeBatchTelemetry
            ? {
                batchSequence: firstNativeBatchTelemetry.batchSequence,
                payloadKind: firstNativeBatchTelemetry.payloadKind,
                meshCount: firstNativeBatchTelemetry.meshCount,
                positionsLen: firstNativeBatchTelemetry.positionsLen,
                normalsLen: firstNativeBatchTelemetry.normalsLen,
                indicesLen: firstNativeBatchTelemetry.indicesLen,
                rustChunkReadyMs: firstNativeBatchTelemetry.chunkReadyTimeMs,
                rustPackMs: firstNativeBatchTelemetry.packTimeMs,
                rustEmittedMs: firstNativeBatchTelemetry.emittedTimeMs,
                rustEmitMs: firstNativeBatchTelemetry.emitTimeMs,
                jsReceivedMs: jsFirstChunkReceivedMs,
                transportToJsMs:
                  jsFirstChunkReceivedMs !== null
                    ? jsFirstChunkReceivedMs - firstNativeBatchTelemetry.emittedTimeMs
                    : null,
                appendAfterReceiveMs:
                  jsFirstChunkReceivedMs !== null && firstAppendGeometryBatchMs !== null
                    ? firstAppendGeometryBatchMs - jsFirstChunkReceivedMs
                    : null,
                visibleAfterAppendMs:
                  firstVisibleGeometryMs !== null && firstAppendGeometryBatchMs !== null
                    ? firstVisibleGeometryMs - firstAppendGeometryBatchMs
                    : null,
              }
            : null,
        });
        setLoading(false);
        return;
      }

      if (isNativeFileHandle(file)) {
        throw new Error(`Native path loading currently supports .ifc files only: ${fileName}`);
      }

      // Read file from disk
      const fileReadStart = performance.now();
      const buffer = await file.arrayBuffer();
      const fileReadMs = performance.now() - fileReadStart;
      console.log(`[useIfc] File: ${file.name}, size: ${fileSizeMB.toFixed(2)}MB, read in ${fileReadMs.toFixed(0)}ms`);

      // Detect file format (IFCX/IFC5 vs IFC4 STEP vs GLB)
      const format = detectFormat(buffer);

      // IFCX files must be parsed client-side (server only supports IFC4 STEP)
      if (format === 'ifcx') {
        setProgress({ phase: 'Parsing IFCX (client-side)', percent: 10 });

        try {
          const ifcxResult = await parseIfcx(buffer, {
            onProgress: (prog: { phase: string; percent: number }) => {
              setProgress({ phase: `IFCX ${prog.phase}`, percent: 10 + (prog.percent * 0.8) });
            },
          });

          // Convert IFCX meshes to viewer format
          // Note: IFCX geometry extractor already handles Y-up to Z-up conversion
          // and applies transforms correctly in Z-up space, so we just pass through

          const meshes: MeshData[] = ifcxResult.meshes.map((m: { expressId?: number; express_id?: number; id?: number; positions: Float32Array | number[]; indices: Uint32Array | number[]; normals: Float32Array | number[]; color?: [number, number, number, number] | [number, number, number]; ifcType?: string; ifc_type?: string }) => {
            // IFCX MeshData has: expressId, ifcType, positions (Float32Array), indices (Uint32Array), normals (Float32Array), color
            const positions = m.positions instanceof Float32Array ? m.positions : new Float32Array(m.positions || []);
            const indices = m.indices instanceof Uint32Array ? m.indices : new Uint32Array(m.indices || []);
            const normals = m.normals instanceof Float32Array ? m.normals : new Float32Array(m.normals || []);

            // Normalize color to RGBA format (4 elements)
            const color = normalizeColor(m.color);

            return {
              expressId: m.expressId ?? m.express_id ?? m.id ?? 0,
              positions,
              indices,
              normals,
              color,
              ifcType: m.ifcType || m.ifc_type || 'IfcProduct',
            };
          }).filter((m: MeshData) => m.positions.length > 0 && m.indices.length > 0); // Filter out empty meshes

          // Check if this is an overlay-only file (no geometry)
          if (meshes.length === 0) {
            console.warn(`[useIfc] IFCX file "${file.name}" has no geometry - this appears to be an overlay file that adds properties to a base model.`);
            console.warn('[useIfc] To use this file, load it together with a base IFCX file (select both files at once).');

            // Check if file has data references that suggest it's an overlay
            const hasReferences = ifcxResult.entityCount > 0;
            if (hasReferences) {
              setError(`"${file.name}" is an overlay file with no geometry. Please load it together with a base IFCX file (select all files at once).`);
              setLoading(false);
              return;
            }
          }

          // Calculate bounds and statistics
          const { bounds, stats } = calculateMeshBounds(meshes);
          const coordinateInfo = createCoordinateInfo(bounds);

          setGeometryResult({
            meshes,
            totalVertices: stats.totalVertices,
            totalTriangles: stats.totalTriangles,
            coordinateInfo,
          });

          // Convert IFCX data model to IfcDataStore format
          // IFCX already provides entities, properties, quantities, relationships, spatialHierarchy
          const dataStore = {
            fileSize: ifcxResult.fileSize,
            schemaVersion: 'IFC5' as const,
            entityCount: ifcxResult.entityCount,
            parseTime: ifcxResult.parseTime,
            source: new Uint8Array(buffer),
            entityIndex: {
              byId: new Map(),
              byType: new Map(),
            },
            strings: ifcxResult.strings,
            entities: ifcxResult.entities,
            properties: ifcxResult.properties,
            quantities: ifcxResult.quantities,
            relationships: ifcxResult.relationships,
            spatialHierarchy: ifcxResult.spatialHierarchy,
          } as IfcxDataStore;

          // IfcxDataStore extends IfcDataStore (with schemaVersion: 'IFC5'), so this is safe
          setIfcDataStore(dataStore);

          setProgress({ phase: 'Complete', percent: 100 });
          setLoading(false);
          return;
        } catch (err: unknown) {
          console.error('[useIfc] IFCX parsing failed:', err);
          const message = err instanceof Error ? err.message : String(err);
          setError(`IFCX parsing failed: ${message}`);
          setLoading(false);
          return;
        }
      }

      // GLB files: parse directly to MeshData (no data model, geometry only)
      if (format === 'glb') {
        setProgress({ phase: 'Parsing GLB', percent: 10 });

        try {
          const meshes = loadGLBToMeshData(new Uint8Array(buffer));

          if (meshes.length === 0) {
            setError('GLB file contains no geometry');
            setLoading(false);
            return;
          }

          const { bounds, stats } = calculateMeshBounds(meshes);
          const coordinateInfo = createCoordinateInfo(bounds);

          setGeometryResult({
            meshes,
            totalVertices: stats.totalVertices,
            totalTriangles: stats.totalTriangles,
            coordinateInfo,
          });

          // GLB files have no IFC data model - set a minimal store
          setIfcDataStore(null);

          setProgress({ phase: 'Complete', percent: 100 });

          setLoading(false);
          return;
        } catch (err: unknown) {
          console.error('[useIfc] GLB parsing failed:', err);
          const message = err instanceof Error ? err.message : String(err);
          setError(`GLB parsing failed: ${message}`);
          setLoading(false);
          return;
        }
      }

      // Cache key uses filename + size + content fingerprint + format version
      // Fingerprint prevents collisions for different files with the same name and size
      const fingerprint = computeFastFingerprint(buffer);
      // Desktop Tauri cache commands only accept [A-Za-z0-9_-], so keep the
      // persisted key filename-safe and independent of the original filename.
      const cacheKey = `ifc-${buffer.byteLength}-${fingerprint}-v4`;

      if (buffer.byteLength >= CACHE_SIZE_THRESHOLD) {
        setProgress({ phase: 'Checking cache', percent: 5 });
        const cacheResult = await getCached(cacheKey);
        if (cacheResult) {
          const success = await loadFromCache(cacheResult, file.name, cacheKey);
          if (success) {
            console.log(`[useIfc] TOTAL LOAD TIME (from cache): ${(performance.now() - totalStartTime).toFixed(0)}ms`);
            setLoading(false);
            return;
          }
        }
      }

      // Try server parsing first (enabled by default for multi-core performance)
      // Only for IFC4 STEP files (server doesn't support IFCX)
      if (format === 'ifc' && USE_SERVER && SERVER_URL && SERVER_URL !== '') {
        // Pass buffer directly - server uses File object for parsing, buffer is only for size checks
        const serverSuccess = await loadFromServer(file, buffer, () => loadSessionRef.current !== currentSession);
        if (serverSuccess) {
          console.log(`[useIfc] TOTAL LOAD TIME (server): ${(performance.now() - totalStartTime).toFixed(0)}ms`);
          setLoading(false);
          return;
        }
        // Server not available - continue with local WASM (no error logging needed)
      } else if (format === 'unknown') {
        console.warn('[useIfc] Unknown file format - attempting to parse as IFC4 STEP');
      }

      // Using local WASM parsing
      setProgress({ phase: 'Starting geometry streaming', percent: 10 });

      // Initialize geometry processor first (WASM init is fast if already loaded)
      const geometryProcessor = new GeometryProcessor({
        quality: GeometryQuality.Balanced
      });
      await geometryProcessor.init();

      // Data model parsing runs IN PARALLEL with geometry streaming.
      // Entity scanning uses a Web Worker (non-blocking, ~1.2s).
      // Columnar parse uses time-sliced yielding (~2.3s, 60fps maintained).
      // Neither depends on geometry output — both just need the raw buffer.
      let resolveDataStore: (dataStore: IfcDataStore) => void;
      let rejectDataStore: (err: unknown) => void;
      const dataStorePromise = new Promise<IfcDataStore>((resolve, reject) => {
        resolveDataStore = resolve;
        rejectDataStore = reject;
      });

      const startDataModelParsing = () => {
        const parser = new IfcParser();
        metadataStartMs = performance.now() - totalStartTime;
        console.log(`[useIfc] Data model parsing start for ${file.name}: ${metadataStartMs.toFixed(0)}ms`);
        // wasmApi as fallback if Web Worker unavailable
        const wasmApi = geometryProcessor.getApi();
        parser.parseColumnar(buffer, {
          wasmApi,
          // Emit spatial hierarchy EARLY — lets the panel render while
          // property/association parsing continues (~0.5-1s earlier).
          onSpatialReady: (partialStore) => {
            if (loadSessionRef.current !== currentSession) return;
            if (spatialReadyMs === null) {
              spatialReadyMs = performance.now() - totalStartTime;
              console.log(`[useIfc] Spatial tree ready for ${file.name} at ${spatialReadyMs.toFixed(0)}ms`);
            }
            if (partialStore.spatialHierarchy && partialStore.spatialHierarchy.storeyHeights.size === 0 && partialStore.spatialHierarchy.storeyElevations.size > 1) {
              const calculatedHeights = calculateStoreyHeights(partialStore.spatialHierarchy.storeyElevations);
              for (const [storeyId, height] of calculatedHeights) {
                partialStore.spatialHierarchy.storeyHeights.set(storeyId, height);
              }
            }
            setIfcDataStore(partialStore);
          },
        }).then(dataStore => {
          if (loadSessionRef.current !== currentSession) return;
          metadataCompleteMs = performance.now() - totalStartTime;
          // Calculate storey heights from elevation differences if not already populated
          if (dataStore.spatialHierarchy && dataStore.spatialHierarchy.storeyHeights.size === 0 && dataStore.spatialHierarchy.storeyElevations.size > 1) {
            const calculatedHeights = calculateStoreyHeights(dataStore.spatialHierarchy.storeyElevations);
            for (const [storeyId, height] of calculatedHeights) {
              dataStore.spatialHierarchy.storeyHeights.set(storeyId, height);
            }
          }

          // Update with full data (includes property/association maps)
          setIfcDataStore(dataStore);
          console.log(`[useIfc] Data model parsing complete for ${file.name}: ${metadataCompleteMs.toFixed(0)}ms`);
          resolveDataStore(dataStore);
        }).catch(err => {
          metadataFailedMs = performance.now() - totalStartTime;
          console.error('[useIfc] Data model parsing failed:', err);
          console.log(`[useIfc] Data model parsing failed for ${file.name}: ${metadataFailedMs.toFixed(0)}ms`);
          rejectDataStore(err);
        });
      };

      // Start data model parsing IMMEDIATELY — runs in parallel with geometry.
      // Entity scan uses Web Worker (off main thread), columnar parse yields
      // every ~4ms to maintain 60fps navigation during geometry streaming.
      setTimeout(startDataModelParsing, 0);

      // Use adaptive processing: sync for small files, streaming for large files
      let estimatedTotal = 0;
      let totalMeshes = 0;
      const allMeshes: MeshData[] = []; // Collect all meshes for BVH building
      let finalCoordinateInfo: CoordinateInfo | null = null;
      // Capture RTC offset from WASM for proper multi-model alignment
      let capturedRtcOffset: { x: number; y: number; z: number } | null = null;
      // Track all deferred style updates so cache data always uses final colors.
      const cumulativeColorUpdates = new Map<number, [number, number, number, number]>();
      let firstAppendGeometryBatchMs: number | null = null;
      let firstVisibleGeometryMs: number | null = null;
      let streamCompleteMs: number | null = null;
      let metadataStartMs: number | null = null;
      let spatialReadyMs: number | null = null;
      let metadataCompleteMs: number | null = null;
      let metadataFailedMs: number | null = null;

      // Clear existing geometry result
      setGeometryResult(null);

      // Timing instrumentation
      let batchCount = 0;
      let firstGeometryTime = 0; // Time to first rendered geometry
      let modelOpenMs = 0;
      let lastTotalMeshes = 0;

      // OPTIMIZATION: Accumulate meshes and batch state updates
      // First batch renders immediately, then accumulate for throughput
      // Adaptive interval: larger files get less frequent updates to reduce React re-render overhead
      let pendingMeshes: MeshData[] = [];
      let lastRenderTime = 0;
      const RENDER_INTERVAL_MS = getRenderIntervalMs(fileSizeMB);
      const markFirstVisibleGeometry = () => {
        if (firstVisibleGeometryMs !== null) return;
        requestAnimationFrame(() => {
          if (firstVisibleGeometryMs !== null || loadSessionRef.current !== currentSession) return;
          firstVisibleGeometryMs = performance.now() - totalStartTime;
          console.log(`[useIfc] First visible geometry for ${file.name}: ${firstVisibleGeometryMs.toFixed(0)}ms`);
        });
      };

      try {
        // Use dynamic batch sizing for optimal throughput
        const dynamicBatchConfig = getDynamicBatchConfig(fileSizeMB);

        for await (const event of geometryProcessor.processAdaptive(new Uint8Array(buffer), {
          sizeThreshold: 2 * 1024 * 1024, // 2MB threshold
          batchSize: dynamicBatchConfig, // Dynamic batches: small first, then large
        })) {
          const eventReceived = performance.now();

          switch (event.type) {
            case 'start':
              estimatedTotal = event.totalEstimate;
              break;
            case 'model-open':
              setProgress({ phase: 'Processing geometry', percent: 50 });
              modelOpenMs = performance.now() - totalStartTime;
              console.log(`[useIfc] Model opened at ${modelOpenMs.toFixed(0)}ms`);
              break;
            case 'colorUpdate': {
              // Accumulate color updates locally during streaming.
              // We apply them in a single pass at 'complete' instead of
              // calling updateMeshColors() per event (which triggers a
              // React reconciliation each time + O(n) scan over all meshes).
              for (const [expressId, color] of event.updates) {
                cumulativeColorUpdates.set(expressId, color);
              }
              // Keep local mesh snapshots in sync for cache serialization.
              applyColorUpdatesToMeshes(allMeshes, event.updates);
              applyColorUpdatesToMeshes(pendingMeshes, event.updates);
              break;
            }
            case 'rtcOffset': {
              // Capture RTC offset from WASM for multi-model alignment
              if (event.hasRtc) {
                capturedRtcOffset = event.rtcOffset;
              }
              break;
            }
            case 'batch': {
              batchCount++;

              // Track time to first geometry
              if (batchCount === 1) {
                firstGeometryTime = performance.now() - totalStartTime;
                console.log(`[useIfc] Batch #1: ${event.meshes.length} meshes, wait: ${firstGeometryTime.toFixed(0)}ms`);
              }


              // Collect meshes for BVH building (use loop to avoid stack overflow with large batches)
              for (let i = 0; i < event.meshes.length; i++) allMeshes.push(event.meshes[i]);
              finalCoordinateInfo = event.coordinateInfo ?? null;
              totalMeshes = event.totalSoFar;
              lastTotalMeshes = event.totalSoFar;

              // Accumulate meshes for batched rendering
              for (let i = 0; i < event.meshes.length; i++) pendingMeshes.push(event.meshes[i]);

              // FIRST BATCH: Render immediately for fast first frame
              // SUBSEQUENT: Throttle to reduce React re-renders
              const timeSinceLastRender = eventReceived - lastRenderTime;
              const shouldRender = batchCount === 1 || timeSinceLastRender >= RENDER_INTERVAL_MS;

              if (shouldRender && pendingMeshes.length > 0) {
                if (firstAppendGeometryBatchMs === null) {
                  firstAppendGeometryBatchMs = performance.now() - totalStartTime;
                  console.log(`[useIfc] First appendGeometryBatch for ${file.name}: ${firstAppendGeometryBatchMs.toFixed(0)}ms`);
                }
                appendGeometryBatch(pendingMeshes, event.coordinateInfo);
                pendingMeshes = [];
                lastRenderTime = eventReceived;
                markFirstVisibleGeometry();

                // Update progress
                const progressPercent = 50 + Math.min(45, (totalMeshes / Math.max(estimatedTotal / 10, totalMeshes)) * 45);
                setProgress({
                  phase: `Rendering geometry (${totalMeshes} meshes)`,
                  percent: progressPercent
                });
              }

              break;
            }
            case 'complete':
              streamCompleteMs = performance.now() - totalStartTime;
              // Flush any remaining pending meshes
              if (pendingMeshes.length > 0) {
                if (firstAppendGeometryBatchMs === null) {
                  firstAppendGeometryBatchMs = performance.now() - totalStartTime;
                  console.log(`[useIfc] First appendGeometryBatch for ${file.name}: ${firstAppendGeometryBatchMs.toFixed(0)}ms`);
                }
                appendGeometryBatch(pendingMeshes, event.coordinateInfo);
                pendingMeshes = [];
                markFirstVisibleGeometry();
              }

              finalCoordinateInfo = event.coordinateInfo ?? null;

              // Data model parsing already started in parallel (see above).
              // No need to start it here — it runs concurrently with geometry.

              // Apply all accumulated color updates in a single store update
              // instead of one updateMeshColors() call per colorUpdate event.
              if (cumulativeColorUpdates.size > 0) {
                updateMeshColors(cumulativeColorUpdates);
              }

              // Store captured RTC offset in coordinate info for multi-model alignment
              if (finalCoordinateInfo && capturedRtcOffset) {
                finalCoordinateInfo.wasmRtcOffset = capturedRtcOffset;
              }

              // Update geometry result with final coordinate info
              updateCoordinateInfo(finalCoordinateInfo);

              setProgress({ phase: 'Complete', percent: 100 });
              console.log(`[useIfc] Geometry streaming complete: ${batchCount} batches, ${lastTotalMeshes} meshes`);
              console.log(`[useIfc] Stream complete for ${file.name}: ${streamCompleteMs.toFixed(0)}ms`);

              // Build spatial index and cache in background (non-blocking)
              // Wait for data model to complete first
              dataStorePromise.then(dataStore => {
                // Guard: skip if user loaded a new file since this load started
                if (loadSessionRef.current !== currentSession) return;
                // Build spatial index from meshes in time-sliced chunks (non-blocking).
                // Previously this was synchronous inside requestIdleCallback, blocking
                // the main thread for seconds on 200K+ mesh models (190M+ float reads
                // for bounds computation alone).
                buildSpatialIndexGuarded(allMeshes, dataStore, setIfcDataStore);

                // Cache the result in the background (files between 10 MB and 150 MB).
                // Files above CACHE_MAX_SOURCE_SIZE are not cached because the
                // source buffer is required for on-demand property/quantity
                // extraction, spatial hierarchy elevations, and IFC re-export.
                // Caching without it would silently degrade those features.
                if (
                  buffer.byteLength >= CACHE_SIZE_THRESHOLD &&
                  buffer.byteLength <= CACHE_MAX_SOURCE_SIZE &&
                  allMeshes.length > 0 &&
                  finalCoordinateInfo
                ) {
                  // Final safety pass so cache always contains post-style colors.
                  applyColorUpdatesToMeshes(allMeshes, cumulativeColorUpdates);
                  const geometryData: GeometryData = {
                    meshes: allMeshes,
                    totalVertices: allMeshes.reduce((sum, m) => sum + m.positions.length / 3, 0),
                    totalTriangles: allMeshes.reduce((sum, m) => sum + m.indices.length / 3, 0),
                    coordinateInfo: finalCoordinateInfo,
                  };
                  saveToCache(cacheKey, dataStore, geometryData, buffer, file.name);
                }
              }).catch(err => {
                // Data model parsing failed - spatial index and caching skipped
                console.warn('[useIfc] Skipping spatial index/cache - data model unavailable:', err);
              });
              break;
          }

        }
      } catch (err) {
        if (loadSessionRef.current !== currentSession) return;
        console.error('[useIfc] Error in processing:', err);
        setError(err instanceof Error ? err.message : 'Unknown error during geometry processing');
      }

      if (loadSessionRef.current !== currentSession) return;

      if (firstVisibleGeometryMs === null && firstAppendGeometryBatchMs !== null) {
        await new Promise<void>((resolve) => {
          const fallbackTimer = globalThis.setTimeout(() => {
            if (firstVisibleGeometryMs === null && loadSessionRef.current === currentSession) {
              firstVisibleGeometryMs = firstAppendGeometryBatchMs;
              console.log(`[useIfc] First visible geometry for ${file.name}: ${firstVisibleGeometryMs.toFixed(0)}ms`);
            }
            resolve();
          }, 250);
          requestAnimationFrame(() => {
            globalThis.clearTimeout(fallbackTimer);
            if (firstVisibleGeometryMs === null && loadSessionRef.current === currentSession) {
              firstVisibleGeometryMs = performance.now() - totalStartTime;
              console.log(`[useIfc] First visible geometry for ${file.name}: ${firstVisibleGeometryMs.toFixed(0)}ms`);
            }
            resolve();
          });
        });
      }

      const totalElapsedMs = performance.now() - totalStartTime;
      const totalVertices = allMeshes.reduce((sum, m) => sum + m.positions.length / 3, 0);
      console.log(
        `[useIfc] ✓ ${file.name} (${fileSizeMB.toFixed(1)}MB) → ` +
        `${allMeshes.length} meshes, ${(totalVertices / 1000).toFixed(0)}k vertices | ` +
        `first: ${firstGeometryTime.toFixed(0)}ms, total: ${totalElapsedMs.toFixed(0)}ms`
      );
      console.log(`[useIfc] TOTAL LOAD TIME (local): ${totalElapsedMs.toFixed(0)}ms (${(totalElapsedMs / 1000).toFixed(1)}s)`);
      setLoading(false);
    } catch (err) {
      if (loadSessionRef.current !== currentSession) return;
      if (isNativeFileHandle(file)) {
        const harnessRequest = getActiveHarnessRequest();
        await finalizeActiveHarnessRun({
          schemaVersion: 1,
          source: 'desktop-native',
          mode: harnessRequest ? 'startup-harness' : 'manual',
          success: false,
          runLabel: harnessRequest?.runLabel,
          file: {
            path: file.path,
            name: file.name,
            sizeBytes: file.size,
            sizeMB: file.size / (1024 * 1024),
          },
          timings: {
            totalWallClockMs: performance.now() - totalStartTime,
          },
          batches: {},
          nativeStats: null,
          metadata: null,
          firstBatchTelemetry: null,
          error: err instanceof Error ? err.message : String(err),
        });
      }
      void logToDesktopTerminal('error', `[useIfc] Load failed: ${err instanceof Error ? err.message : String(err)}`);
      setError(err instanceof Error ? err.message : 'Unknown error');
      setLoading(false);
    }
  }, [setLoading, setError, setProgress, setIfcDataStore, setGeometryResult, appendGeometryBatch, updateMeshColors, updateCoordinateInfo, loadFromCache, saveToCache, loadFromServer]);

  return { loadFile };
}

export default useIfcLoader;
