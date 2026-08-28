/**
 * Library finding and loading for koffi FFI.
 *
 * Search order mirrors Python bindings:
 * 1. LATTICE_LIB_PATH environment variable
 * 2. Bundled in package (lib/<platform>/ or libc-aware linux variants)
 * 3. Electron resource directories (asar.unpacked, extraResources, extraFiles)
 * 4. LATTICE_PREFIX environment variable
 * 5. Development build (zig-out/lib)
 * 6. pkg-config lattice libdir
 * 7. System paths
 *
 * Electron note: `fs.existsSync()` is asar-aware but `dlopen()`/`LoadLibraryW()`
 * are not, so a candidate that resolves inside an `.asar` archive is unusable
 * even though it appears to exist. Such candidates are rewritten to their
 * `.asar.unpacked` twin, and reported explicitly when no twin was unpacked.
 */

import { execFileSync, spawnSync } from 'child_process';
import koffi from 'koffi';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

/**
 * Package name, used to locate the bundled library inside an Electron
 * `app.asar.unpacked` tree when `__dirname` has been rewritten by a bundler.
 */
const PACKAGE_NAME = '@hajewski/latticedb';

/**
 * Glob that Electron packagers must unpack so the native loader can read the
 * bundled shared library. Use it in electron-builder's `asarUnpack`, or in
 * `@electron/packager`'s `asar.unpack`.
 */
export const ELECTRON_ASAR_UNPACK_GLOB = `**/node_modules/${PACKAGE_NAME}/lib/**`;

/** Matches an asar archive path segment, but not its `.asar.unpacked` sibling. */
const ASAR_SEGMENT = /\.asar[\\/]/i;

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
 * Check whether the current process is an Electron runtime (main, renderer, or
 * utility process, including `ELECTRON_RUN_AS_NODE`).
 */
export function isElectronRuntime(): boolean {
  const version = process.versions.electron;
  return typeof version === 'string' && version.length > 0;
}

/**
 * Check whether a path resolves inside a packed asar archive.
 *
 * Paths under `.asar.unpacked/` are extracted to the real filesystem and are
 * therefore not considered archived.
 */
export function isInsideAsarArchive(candidate: string): boolean {
  return ASAR_SEGMENT.test(candidate);
}

/**
 * Rewrite an archived asar path to its `.asar.unpacked` twin.
 *
 * Returns the path unchanged when it is not inside an archive.
 */
export function fixPathForAsarUnpack(candidate: string): string {
  return candidate.replace(/\.asar([\\/])/i, '.asar.unpacked$1');
}

/**
 * Get Electron's `process.resourcesPath`, or null outside Electron.
 */
function getResourcesPath(): string | null {
  const resourcesPath = (process as NodeJS.Process & { resourcesPath?: string }).resourcesPath;
  return typeof resourcesPath === 'string' && resourcesPath.length > 0 ? resourcesPath : null;
}

/**
 * Detect the libc variant for Linux package bundles.
 */
function detectLinuxLibc(): 'gnu' | 'musl' | null {
  if (process.platform !== 'linux') {
    return null;
  }

  const report = process.report?.getReport?.() as
    | { header?: { glibcVersionRuntime?: string } }
    | undefined;
  if (report?.header?.glibcVersionRuntime) {
    return 'gnu';
  }

  const ldd = spawnSync('ldd', ['--version'], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const lddOutput = `${ldd.stdout ?? ''}\n${ldd.stderr ?? ''}`.toLowerCase();
  if (lddOutput.includes('musl')) {
    return 'musl';
  }
  if (lddOutput.includes('glibc') || lddOutput.includes('gnu libc')) {
    return 'gnu';
  }

  if (fs.existsSync('/etc/alpine-release')) {
    return 'musl';
  }

  return null;
}

/**
 * Get package bundle directory candidates for the current platform.
 *
 * Linux distinguishes glibc and musl builds for packaged shared libraries.
 * The legacy linux-<arch> layout remains as a compatibility fallback.
 */
export function getBundledPlatformDirs(): string[] {
  const platform = process.platform;
  const arch = process.arch; // 'x64', 'arm64', etc.

  if (platform === 'linux') {
    const libc = detectLinuxLibc();
    const dirs = libc ? [`linux-${arch}-${libc}`] : [];
    dirs.push(`linux-${arch}`);
    return dirs;
  }

  return [`${platform}-${arch}`];
}

/**
 * Get bundled package library candidates for the current platform.
 */
export function getBundledLibraryCandidates(baseDir: string = __dirname): string[] {
  const libName = getLibName();
  return getBundledPlatformDirs().map((platformDir) =>
    path.join(baseDir, '../../lib', platformDir, libName)
  );
}

/**
 * Get Electron-specific library candidates for the current platform.
 *
 * Covers the layouts an Electron app can end up with:
 * - `asarUnpack` of this package's `lib/` directory
 * - an unpacked (`asar: false`) application directory
 * - `extraResources` copying the library, or a platform directory, into
 *   `process.resourcesPath`
 * - `extraFiles` copying the library next to the executable
 *
 * Returns an empty array outside Electron.
 */
export function getElectronLibraryCandidates(): string[] {
  const resourcesPath = getResourcesPath();
  if (!resourcesPath) {
    return [];
  }

  const libName = getLibName();
  const candidates: string[] = [];

  for (const platformDir of getBundledPlatformDirs()) {
    for (const appDir of ['app.asar.unpacked', 'app']) {
      candidates.push(
        path.join(resourcesPath, appDir, 'node_modules', PACKAGE_NAME, 'lib', platformDir, libName)
      );
    }
    candidates.push(path.join(resourcesPath, 'lib', platformDir, libName));
    candidates.push(path.join(resourcesPath, platformDir, libName));
  }

  candidates.push(path.join(resourcesPath, 'lib', libName));
  candidates.push(path.join(resourcesPath, libName));
  candidates.push(path.join(path.dirname(process.execPath), libName));

  return candidates;
}

/**
 * Accept a candidate path only if the native loader can actually open it.
 *
 * An archived asar candidate is rewritten to its unpacked twin; if no twin was
 * unpacked the candidate is recorded in `asarBlocked` and rejected, because
 * `dlopen()` cannot read through the asar virtual filesystem.
 */
function acceptCandidate(candidate: string, asarBlocked: string[]): string | null {
  if (isInsideAsarArchive(candidate)) {
    const unpacked = fixPathForAsarUnpack(candidate);
    if (fs.existsSync(unpacked)) {
      return unpacked;
    }
    if (fs.existsSync(candidate) && !asarBlocked.includes(candidate)) {
      asarBlocked.push(candidate);
    }
    return null;
  }

  return fs.existsSync(candidate) ? candidate : null;
}

/**
 * Find the lattice shared library.
 *
 * @returns The resolved path (or null), plus any candidates that were found
 *          inside a packed asar archive and therefore could not be loaded.
 */
function findLibrary(): { libPath: string | null; asarBlocked: string[] } {
  const libName = getLibName();
  const asarBlocked: string[] = [];
  const accept = (candidate: string): string | null => acceptCandidate(candidate, asarBlocked);

  // 1. Environment variable override (explicit path)
  const envPath = process.env.LATTICE_LIB_PATH;
  if (envPath) {
    if (fs.existsSync(envPath)) {
      // Could be direct path to library or directory containing it
      const stats = fs.statSync(envPath);
      if (stats.isFile()) {
        const resolved = accept(envPath);
        if (resolved) {
          return { libPath: resolved, asarBlocked };
        }
      }
      if (stats.isDirectory()) {
        const resolved = accept(path.join(envPath, libName));
        if (resolved) {
          return { libPath: resolved, asarBlocked };
        }
      }
    }
  }

  // 2. Bundled in package (for npm installs)
  // Goes up from src/ffi/ to package root, then into lib/
  for (const bundledPath of getBundledLibraryCandidates()) {
    const resolved = accept(bundledPath);
    if (resolved) {
      return { libPath: resolved, asarBlocked };
    }
  }

  // 3. Electron resource directories (no-op outside Electron)
  for (const electronPath of getElectronLibraryCandidates()) {
    const resolved = accept(electronPath);
    if (resolved) {
      return { libPath: resolved, asarBlocked };
    }
  }

  // 4. Installed prefix override
  const prefix = process.env.LATTICE_PREFIX;
  if (prefix) {
    const resolved = accept(path.join(prefix, 'lib', libName));
    if (resolved) {
      return { libPath: resolved, asarBlocked };
    }
  }

  // 5. Development build (zig-out/lib)
  // Goes up from bindings/typescript/src/ffi/ to repo root
  const devPath = accept(path.join(__dirname, '../../../../zig-out/lib', libName));
  if (devPath) {
    return { libPath: devPath, asarBlocked };
  }

  // 6. pkg-config metadata
  try {
    const libDir = execFileSync('pkg-config', ['--variable=libdir', 'lattice'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
    if (libDir) {
      const resolved = accept(path.join(libDir, libName));
      if (resolved) {
        return { libPath: resolved, asarBlocked };
      }
    }
  } catch {
    // pkg-config unavailable or lattice.pc not present
  }

  // 7. System paths
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
    const resolved = accept(path.join(dir, libName));
    if (resolved) {
      return { libPath: resolved, asarBlocked };
    }
  }

  return { libPath: null, asarBlocked };
}

/**
 * Build the "library not found" error, adding Electron packaging guidance when
 * the runtime or the rejected candidates call for it.
 */
export function buildNotFoundError(asarBlocked: string[]): Error {
  const parts = [
    'Could not find liblattice shared library. ' +
      'Set LATTICE_LIB_PATH or LATTICE_PREFIX, ' +
      'configure PKG_CONFIG_PATH for an installed build, ' +
      'or install lattice-db with bundled binaries, ' +
      'or build from source with "zig build shared".',
  ];

  if (asarBlocked.length > 0) {
    parts.push(
      `The library was found inside a packed Electron asar archive (${asarBlocked.join(', ')}), ` +
        'which the native loader cannot read. Unpack it, for example with electron-builder ' +
        `"asarUnpack": ["${ELECTRON_ASAR_UNPACK_GLOB}"].`
    );
  } else if (isElectronRuntime()) {
    parts.push(
      'Running under Electron: bundlers rewrite __dirname, so the packaged library may not sit ' +
        `beside this module. Add "asarUnpack": ["${ELECTRON_ASAR_UNPACK_GLOB}"], ship the library ` +
        'through extraResources, or set LATTICE_LIB_PATH before opening a database.'
    );
  }

  return new Error(parts.join(' '));
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

  const { libPath, asarBlocked } = findLibrary();
  if (!libPath) {
    throw buildNotFoundError(asarBlocked);
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
    return findLibrary().libPath !== null;
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

/**
 * Resolve the shared library path without loading it.
 *
 * Useful for Electron build scripts that stage the library into
 * `extraResources`, and for diagnosing packaging problems at runtime.
 */
export function resolveLibraryPath(): string | null {
  return findLibrary().libPath;
}
