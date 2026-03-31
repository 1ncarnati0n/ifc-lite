/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

/**
 * Native Bridge Implementation
 *
 * Uses Tauri commands for geometry processing in desktop apps.
 * Provides native Rust performance with multi-threading support.
 */

import type {
  IPlatformBridge,
  GeometryProcessingResult,
  GeometryStats,
  StreamingOptions,
  GeometryBatch,
  NativeBatchTelemetry,
} from './platform-bridge.js';
import type { MeshData, CoordinateInfo } from './types.js';

// Tauri API types - dynamically imported to avoid issues in web builds
type InvokeFn = <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>;
type ListenFn = <T>(event: string, handler: (event: { payload: T }) => void) => Promise<() => void>;

// Tauri internals interface (set by Tauri runtime)
interface TauriInternals {
  invoke: InvokeFn;
}

interface NativeStreamingProgress {
  processed: number;
  total: number;
  currentType: string;
}

interface NativeBatchTelemetryPayload {
  batchSequence: number;
  payloadKind: string;
  meshCount: number;
  positionsLen: number;
  normalsLen: number;
  indicesLen: number;
  chunkReadyTimeMs: number;
  packTimeMs: number;
  emitTimeMs: number;
  emittedTimeMs: number;
}

interface NativeColorUpdatePayload {
  updates: Array<{
    expressId: number;
    color: [number, number, number, number];
  }>;
}

/**
 * Native Tauri bridge for desktop apps
 *
 * This uses Tauri's invoke() to call native Rust commands that use
 * ifc-lite-core and ifc-lite-geometry directly (no WASM overhead).
 */
export class NativeBridge implements IPlatformBridge {
  private initialized = false;
  private invoke: InvokeFn | null = null;
  private listen: ListenFn | null = null;

  async init(): Promise<void> {
    if (this.initialized) return;

    // Access Tauri internals directly to avoid bundler issues
    // This is set by Tauri runtime and is always available in Tauri apps
    const win = globalThis as unknown as { __TAURI_INTERNALS__?: TauriInternals };
    if (!win.__TAURI_INTERNALS__?.invoke) {
      throw new Error('Tauri API not available - this bridge should only be used in Tauri apps');
    }

    this.invoke = win.__TAURI_INTERNALS__.invoke;

    // For event listening, we still need the event module
    // Use dynamic import with try-catch for better error handling
    try {
      const event = await import('@tauri-apps/api/event');
      this.listen = event.listen;
    } catch {
      // Event listening is optional - streaming will fall back to non-streaming
      console.warn('[NativeBridge] Event API not available, streaming will be limited');
    }

    this.initialized = true;
  }

  isInitialized(): boolean {
    return this.initialized;
  }

  async processGeometry(content: string): Promise<GeometryProcessingResult> {
    if (!this.initialized || !this.invoke) {
      await this.init();
    }

    // Convert string to buffer for Tauri command
    const encoder = new TextEncoder();
    const buffer = Array.from(encoder.encode(content));

    // Call native Rust command
    const result = await this.invoke!<{
      meshes: NativeMeshData[];
      totalVertices: number;
      totalTriangles: number;
      coordinateInfo: NativeCoordinateInfo;
    }>('get_geometry', { buffer });

    // Convert native format to TypeScript format
    const meshes: MeshData[] = result.meshes.map(convertNativeMesh);
    const coordinateInfo = convertNativeCoordinateInfo(result.coordinateInfo);

    return {
      meshes,
      totalVertices: result.totalVertices,
      totalTriangles: result.totalTriangles,
      coordinateInfo,
    };
  }

  async processGeometryPath(path: string): Promise<GeometryProcessingResult> {
    if (!this.initialized || !this.invoke) {
      await this.init();
    }

    const result = await this.invoke!<{
      meshes: NativeMeshData[];
      totalVertices: number;
      totalTriangles: number;
      coordinateInfo: NativeCoordinateInfo;
    }>('get_geometry_from_path', { path });

    return {
      meshes: result.meshes.map(convertNativeMesh),
      totalVertices: result.totalVertices,
      totalTriangles: result.totalTriangles,
      coordinateInfo: convertNativeCoordinateInfo(result.coordinateInfo),
    };
  }

  async processGeometryStreaming(
    content: string,
    options: StreamingOptions
  ): Promise<GeometryStats> {
    if (!this.initialized || !this.invoke) {
      await this.init();
    }

    // If event API not available, fall back to non-streaming processing
    if (!this.listen) {
      console.warn('[NativeBridge] Event API unavailable, falling back to non-streaming mode');
      const result = await this.processGeometry(content);
      const stats: GeometryStats = {
        totalMeshes: result.meshes.length,
        totalVertices: result.totalVertices,
        totalTriangles: result.totalTriangles,
        parseTimeMs: 0,
        entityScanTimeMs: 0,
        lookupTimeMs: 0,
        preprocessTimeMs: 0,
        geometryTimeMs: 0,
        totalTimeMs: 0,
        firstChunkReadyTimeMs: 0,
        firstChunkPackTimeMs: 0,
        firstChunkEmittedTimeMs: 0,
        firstChunkEmitTimeMs: 0,
      };
      // Emit single batch with all meshes
      options.onBatch?.({
        meshes: result.meshes,
        progress: { processed: result.meshes.length, total: result.meshes.length, currentType: 'complete' },
      });
      options.onComplete?.(stats);
      return stats;
    }

    // Convert string to buffer for Tauri command
    const encoder = new TextEncoder();
    const buffer = Array.from(encoder.encode(content));

    // Listen for geometry batch events
    const streamStartTime = performance.now();
    const unlisten = await this.listen<{
      meshes: NativeMeshData[];
      progress: NativeStreamingProgress;
      telemetry?: NativeBatchTelemetryPayload;
    }>('geometry-batch', (event) => {
      const batch: GeometryBatch = {
        meshes: event.payload.meshes.map(convertNativeMesh),
        progress: {
          processed: event.payload.progress.processed,
          total: event.payload.progress.total,
          currentType: event.payload.progress.currentType,
        },
        nativeTelemetry: convertNativeBatchTelemetry(
          event.payload.telemetry,
          performance.now() - streamStartTime
        ),
      };
      options.onBatch?.(batch);
    });
    const unlistenPacked = await this.listen<NativePackedGeometryBatch>('geometry-packed-batch', (event) => {
      const batch: GeometryBatch = {
        meshes: convertPackedNativeBatch(event.payload),
        progress: {
          processed: event.payload.progress.processed,
          total: event.payload.progress.total,
          currentType: event.payload.progress.currentType,
        },
        nativeTelemetry: convertNativeBatchTelemetry(
          event.payload.telemetry,
          performance.now() - streamStartTime
        ),
      };
      options.onBatch?.(batch);
    });
    const unlistenColorUpdate = await this.listen<NativeColorUpdatePayload>('geometry-color-update', (event) => {
      const updates = new Map<number, [number, number, number, number]>();
      for (const entry of event.payload.updates) {
        updates.set(entry.expressId, entry.color);
      }
      if (updates.size > 0) {
        options.onColorUpdate?.(updates);
      }
    });

    try {
      // Call native streaming command
      const stats = await this.invoke!<{
        totalMeshes: number;
        totalVertices: number;
        totalTriangles: number;
        parseTimeMs: number;
        entityScanTimeMs?: number;
        lookupTimeMs?: number;
        preprocessTimeMs?: number;
        geometryTimeMs: number;
        totalTimeMs?: number;
        firstChunkReadyTimeMs?: number;
        firstChunkPackTimeMs?: number;
        firstChunkEmittedTimeMs?: number;
        firstChunkEmitTimeMs?: number;
      }>('get_geometry_streaming', { buffer });

      const result: GeometryStats = {
        totalMeshes: stats.totalMeshes,
        totalVertices: stats.totalVertices,
        totalTriangles: stats.totalTriangles,
        parseTimeMs: stats.parseTimeMs,
        entityScanTimeMs: stats.entityScanTimeMs,
        lookupTimeMs: stats.lookupTimeMs,
        preprocessTimeMs: stats.preprocessTimeMs,
        geometryTimeMs: stats.geometryTimeMs,
        totalTimeMs: stats.totalTimeMs,
        firstChunkReadyTimeMs: stats.firstChunkReadyTimeMs,
        firstChunkPackTimeMs: stats.firstChunkPackTimeMs,
        firstChunkEmittedTimeMs: stats.firstChunkEmittedTimeMs,
        firstChunkEmitTimeMs: stats.firstChunkEmitTimeMs,
      };

      options.onComplete?.(result);
      return result;
    } catch (error) {
      options.onError?.(error instanceof Error ? error : new Error(String(error)));
      throw error;
    } finally {
      // Clean up event listener
      unlisten();
      unlistenPacked();
      unlistenColorUpdate();
    }
  }

  async processGeometryStreamingPath(
    path: string,
    options: StreamingOptions
  ): Promise<GeometryStats> {
    if (!this.initialized || !this.invoke) {
      await this.init();
    }

    if (!this.listen) {
      console.warn('[NativeBridge] Event API unavailable, falling back to non-streaming file-path mode');
      const result = await this.processGeometryPath(path);
      const stats: GeometryStats = {
        totalMeshes: result.meshes.length,
        totalVertices: result.totalVertices,
        totalTriangles: result.totalTriangles,
        parseTimeMs: 0,
        entityScanTimeMs: 0,
        lookupTimeMs: 0,
        preprocessTimeMs: 0,
        geometryTimeMs: 0,
        totalTimeMs: 0,
        firstChunkReadyTimeMs: 0,
        firstChunkPackTimeMs: 0,
        firstChunkEmittedTimeMs: 0,
        firstChunkEmitTimeMs: 0,
      };
      options.onBatch?.({
        meshes: result.meshes,
        progress: { processed: result.meshes.length, total: result.meshes.length, currentType: 'complete' },
      });
      options.onComplete?.(stats);
      return stats;
    }

    const streamStartTime = performance.now();
    const unlisten = await this.listen<{
      meshes: NativeMeshData[];
      progress: NativeStreamingProgress;
      telemetry?: NativeBatchTelemetryPayload;
    }>('geometry-batch', (event) => {
      const batch: GeometryBatch = {
        meshes: event.payload.meshes.map(convertNativeMesh),
        progress: {
          processed: event.payload.progress.processed,
          total: event.payload.progress.total,
          currentType: event.payload.progress.currentType,
        },
        nativeTelemetry: convertNativeBatchTelemetry(
          event.payload.telemetry,
          performance.now() - streamStartTime
        ),
      };
      options.onBatch?.(batch);
    });
    const unlistenPacked = await this.listen<NativePackedGeometryBatch>('geometry-packed-batch', (event) => {
      const batch: GeometryBatch = {
        meshes: convertPackedNativeBatch(event.payload),
        progress: {
          processed: event.payload.progress.processed,
          total: event.payload.progress.total,
          currentType: event.payload.progress.currentType,
        },
        nativeTelemetry: convertNativeBatchTelemetry(
          event.payload.telemetry,
          performance.now() - streamStartTime
        ),
      };
      options.onBatch?.(batch);
    });
    const unlistenColorUpdate = await this.listen<NativeColorUpdatePayload>('geometry-color-update', (event) => {
      const updates = new Map<number, [number, number, number, number]>();
      for (const entry of event.payload.updates) {
        updates.set(entry.expressId, entry.color);
      }
      if (updates.size > 0) {
        options.onColorUpdate?.(updates);
      }
    });

    try {
      const stats = await this.invoke!<{
        totalMeshes: number;
        totalVertices: number;
        totalTriangles: number;
        parseTimeMs: number;
        entityScanTimeMs?: number;
        lookupTimeMs?: number;
        preprocessTimeMs?: number;
        geometryTimeMs: number;
        totalTimeMs?: number;
        firstChunkReadyTimeMs?: number;
        firstChunkPackTimeMs?: number;
        firstChunkEmittedTimeMs?: number;
        firstChunkEmitTimeMs?: number;
      }>('get_geometry_streaming_from_path', { path });

      const result: GeometryStats = {
        totalMeshes: stats.totalMeshes,
        totalVertices: stats.totalVertices,
        totalTriangles: stats.totalTriangles,
        parseTimeMs: stats.parseTimeMs,
        entityScanTimeMs: stats.entityScanTimeMs,
        lookupTimeMs: stats.lookupTimeMs,
        preprocessTimeMs: stats.preprocessTimeMs,
        geometryTimeMs: stats.geometryTimeMs,
        totalTimeMs: stats.totalTimeMs,
        firstChunkReadyTimeMs: stats.firstChunkReadyTimeMs,
        firstChunkPackTimeMs: stats.firstChunkPackTimeMs,
        firstChunkEmittedTimeMs: stats.firstChunkEmittedTimeMs,
        firstChunkEmitTimeMs: stats.firstChunkEmitTimeMs,
      };

      options.onComplete?.(result);
      return result;
    } catch (error) {
      options.onError?.(error instanceof Error ? error : new Error(String(error)));
      throw error;
    } finally {
      unlisten();
      unlistenPacked();
      unlistenColorUpdate();
    }
  }

  getApi(): null {
    // Native bridge doesn't expose an API object
    return null;
  }
}

// Native types from Rust (camelCase due to serde rename)
interface NativeMeshData {
  expressId: number;
  ifcType?: string;
  positions: number[];
  normals: number[];
  indices: number[];
  color: [number, number, number, number];
}

interface NativePackedMeshRange {
  expressId: number;
  ifcType?: string;
  positionsOffset: number;
  positionsLen: number;
  normalsOffset: number;
  normalsLen: number;
  indicesOffset: number;
  indicesLen: number;
  color: [number, number, number, number];
}

interface NativePackedGeometryBatch {
  meshes: NativePackedMeshRange[];
  positions: number[];
  normals: number[];
  indices: number[];
  progress: NativeStreamingProgress;
  telemetry?: NativeBatchTelemetryPayload;
}

interface NativePoint3 {
  x: number;
  y: number;
  z: number;
}

interface NativeBounds {
  min: NativePoint3;
  max: NativePoint3;
}

interface NativeCoordinateInfo {
  originShift: NativePoint3;
  originalBounds: NativeBounds;
  shiftedBounds: NativeBounds;
  hasLargeCoordinates: boolean;
}

// Conversion functions
function convertNativeMesh(native: NativeMeshData): MeshData {
  return {
    expressId: native.expressId,
    ifcType: native.ifcType,
    positions: new Float32Array(native.positions),
    normals: new Float32Array(native.normals),
    indices: new Uint32Array(native.indices),
    color: native.color,
  };
}

function convertPackedNativeBatch(native: NativePackedGeometryBatch): MeshData[] {
  return native.meshes.map((mesh) => ({
    expressId: mesh.expressId,
    ifcType: mesh.ifcType,
    positions: new Float32Array(
      native.positions.slice(mesh.positionsOffset, mesh.positionsOffset + mesh.positionsLen)
    ),
    normals: new Float32Array(
      native.normals.slice(mesh.normalsOffset, mesh.normalsOffset + mesh.normalsLen)
    ),
    indices: new Uint32Array(
      native.indices.slice(mesh.indicesOffset, mesh.indicesOffset + mesh.indicesLen)
    ),
    color: mesh.color,
  }));
}

function convertNativeBatchTelemetry(
  telemetry: NativeBatchTelemetryPayload | undefined,
  jsReceivedTimeMs: number
): NativeBatchTelemetry | undefined {
  if (!telemetry) {
    return undefined;
  }

  return {
    batchSequence: telemetry.batchSequence,
    payloadKind: telemetry.payloadKind,
    meshCount: telemetry.meshCount,
    positionsLen: telemetry.positionsLen,
    normalsLen: telemetry.normalsLen,
    indicesLen: telemetry.indicesLen,
    chunkReadyTimeMs: telemetry.chunkReadyTimeMs,
    packTimeMs: telemetry.packTimeMs,
    emitTimeMs: telemetry.emitTimeMs,
    emittedTimeMs: telemetry.emittedTimeMs,
    jsReceivedTimeMs,
  };
}

function convertNativeCoordinateInfo(native: NativeCoordinateInfo): CoordinateInfo {
  return {
    originShift: native.originShift,
    originalBounds: native.originalBounds,
    shiftedBounds: native.shiftedBounds,
    hasLargeCoordinates: native.hasLargeCoordinates,
  };
}
