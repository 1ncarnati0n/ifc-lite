/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

type InvokeFn = <T>(cmd: string, args?: Record<string, unknown>) => Promise<T>;

export interface NativeFileHandle {
  path: string;
  name: string;
  size: number;
}

async function loadInvokeFromTauriModule(): Promise<InvokeFn | null> {
  try {
    const core = await import('@tauri-apps/api/core');
    return typeof core.invoke === 'function' ? core.invoke as InvokeFn : null;
  } catch {
    return null;
  }
}

async function loadDialogModule(): Promise<{
  open: (options?: {
    multiple?: boolean;
    directory?: boolean;
    filters?: Array<{ name: string; extensions: string[] }>;
    title?: string;
  }) => Promise<string | string[] | null>;
} | null> {
  try {
    return await import('@tauri-apps/plugin-dialog');
  } catch {
    return null;
  }
}

async function loadFsModule(): Promise<{
  stat: (path: string) => Promise<{ size: number }>;
} | null> {
  try {
    return await import('@tauri-apps/plugin-fs');
  } catch {
    return null;
  }
}

async function getInvoke(): Promise<InvokeFn> {
  const win = globalThis as unknown as { __TAURI_INTERNALS__?: { invoke: InvokeFn } };
  if (win.__TAURI_INTERNALS__?.invoke) {
    return win.__TAURI_INTERNALS__.invoke;
  }
  const moduleInvoke = await loadInvokeFromTauriModule();
  if (moduleInvoke) {
    return moduleInvoke;
  }
  throw new Error('Tauri API not available');
}

export async function openIfcFileDialog(): Promise<NativeFileHandle | null> {
  try {
    const invoke = await getInvoke();
    return await invoke<NativeFileHandle | null>('open_ifc_file');
  } catch (error) {
    console.warn('[FileDialog] Command-based native dialog unavailable, trying plugin fallback:', error);
  }

  try {
    const dialog = await loadDialogModule();
    const fs = await loadFsModule();
    if (!dialog || !fs) {
      return null;
    }

    const selected = await dialog.open({
      multiple: false,
      directory: false,
      title: 'Open IFC File',
      filters: [
        { name: 'IFC Files', extensions: ['ifc', 'ifczip', 'ifcxml'] },
        { name: 'All Files', extensions: ['*'] },
      ],
    });
    if (!selected || Array.isArray(selected)) {
      return null;
    }

    const metadata = await fs.stat(selected);
    const normalizedPath = selected.toString();
    const pathSegments = normalizedPath.split(/[\\/]/);
    const name = pathSegments[pathSegments.length - 1] || 'unknown.ifc';

    return {
      path: normalizedPath,
      name,
      size: metadata.size,
    };
  } catch (error) {
    console.warn('[FileDialog] Failed to open native IFC dialog via plugin fallback:', error);
    return null;
  }
}

export async function readNativeFile(path: string): Promise<Uint8Array> {
  const fs = await import('@tauri-apps/plugin-fs');
  return fs.readFile(path);
}
