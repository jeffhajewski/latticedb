/**
 * Library finding and loading for koffi FFI.
 *
 * Search order mirrors Python bindings:
 * 1. LATTICE_LIB_PATH environment variable
 * 2. Bundled in package (lib/<platform>/)
 * 3. Development build (zig-out/lib/)
 * 4. System paths
 */

import koffi from 'koffi';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

/**
 * Get the library filename for the current platform.
 */
function getLibName(): string {
  switch (process.platform) {
    case 'darwin':
      return 'liblattice.dylib';
    case 'win32':
      return 'lattice.dll';
    default:
      return 'liblattice.so';
  }
}

/**
 * Get the platform-architecture directory name.
 */
function getPlatformDir(): string {
  const platform = process.platform;
  const arch = process.arch; // 'x64', 'arm64', etc.
  return `${platform}-${arch}`;
}

/**
 * Find the lattice shared library.
 *
 * @returns Path to the library, or null if not found.
 */
function findLibrary(): string | null {
  const libName = getLibName();

  // 1. Environment variable override (explicit path)
  const envPath = process.env.LATTICE_LIB_PATH;
  if (envPath) {
    if (fs.existsSync(envPath)) {
      // Could be direct path to library or directory containing it
      const stats = fs.statSync(envPath);
      if (stats.isFile()) {
        return envPath;
      }
      if (stats.isDirectory()) {
        const libPath = path.join(envPath, libName);
        if (fs.existsSync(libPath)) {
          return libPath;
        }
      }
    }
  }

  // 2. Bundled in package (for npm installs)
  // Goes up from src/ffi/ to package root, then into lib/
  const bundledPath = path.join(__dirname, '../../lib', getPlatformDir(), libName);
  if (fs.existsSync(bundledPath)) {
    return bundledPath;
  }

  // 3. Development build (zig-out/lib)
  // Goes up from bindings/typescript/src/ffi/ to repo root
  const devPath = path.join(__dirname, '../../../../zig-out/lib', libName);
  if (fs.existsSync(devPath)) {
    return devPath;
  }

  // 4. System paths
  const systemPaths: string[] = [
    '/usr/local/lib',
    '/usr/lib',
    path.join(os.homedir(), '.local/lib'),
  ];

  // Add platform-specific paths
  if (process.platform === 'darwin') {
    systemPaths.unshift('/opt/homebrew/lib');
    systemPaths.unshift('/usr/local/opt/latticedb/lib');
  }

  for (const dir of systemPaths) {
    const libPath = path.join(dir, libName);
    if (fs.existsSync(libPath)) {
      return libPath;
    }
  }

  return null;
}

// Cached library instance
let _lib: koffi.IKoffiLib | null = null;
let _libPath: string | null = null;

/**
 * Get the loaded koffi library instance.
 *
 * @throws Error if library cannot be found or loaded.
 */
export function getLibrary(): koffi.IKoffiLib {
  if (_lib) {
    return _lib;
  }

  const libPath = findLibrary();
  if (!libPath) {
    throw new Error(
      'Could not find liblattice shared library. ' +
        'Set LATTICE_LIB_PATH environment variable, ' +
        'or install lattice-db with bundled binaries, ' +
        'or build from source with "zig build shared".'
    );
  }

  try {
    _lib = koffi.load(libPath);
    _libPath = libPath;
    return _lib;
  } catch (err) {
    throw new Error(`Failed to load liblattice from ${libPath}: ${err}`);
  }
}

/**
 * Check if the native library is available.
 */
export function isLibraryAvailable(): boolean {
  try {
    if (_lib) return true;
    return findLibrary() !== null;
  } catch {
    return false;
  }
}

/**
 * Get the path to the loaded library, or null if not loaded.
 */
export function getLibraryPath(): string | null {
  return _libPath;
}
