/**
 * Electron packaging tests for the native library loader.
 *
 * Electron's `fs` reads through `.asar` archives but `dlopen()`/`LoadLibraryW()`
 * do not, so a candidate inside an archive must be redirected to its
 * `.asar.unpacked` twin or rejected. A real directory named `app.asar` gives the
 * same `fs.existsSync()` answers as a packed archive, so these tests reproduce
 * the packaged layout without running Electron.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

type LibraryModule = typeof import('../src/ffi/library');

function nativeLibName(): string {
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
 * Load a fresh copy of the loader so its cached library handle is discarded.
 */
async function freshLibraryModule(): Promise<LibraryModule> {
  jest.resetModules();
  return import('../src/ffi/library');
}

/**
 * Run `body` with the given environment overrides, restoring them afterwards.
 * An `undefined` value deletes the variable for the duration of the call.
 */
async function withEnv(
  overrides: Record<string, string | undefined>,
  body: () => Promise<void>
): Promise<void> {
  const previous = new Map<string, string | undefined>();
  for (const [key, value] of Object.entries(overrides)) {
    previous.set(key, process.env[key]);
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }

  try {
    await body();
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
}

describe('asar path handling', () => {
  let library: LibraryModule;

  beforeAll(async () => {
    library = await freshLibraryModule();
  });

  test('detects packed archive paths with either separator', () => {
    expect(library.isInsideAsarArchive('/app/resources/app.asar/lib/liblattice.so')).toBe(true);
    expect(library.isInsideAsarArchive('C:\\app\\resources\\app.asar\\lib\\lattice.dll')).toBe(true);
  });

  test('unpacked paths are not treated as archived', () => {
    expect(library.isInsideAsarArchive('/app/resources/app.asar.unpacked/lib/liblattice.so')).toBe(
      false
    );
    expect(
      library.isInsideAsarArchive('C:\\app\\resources\\app.asar.unpacked\\lib\\lattice.dll')
    ).toBe(false);
    expect(library.isInsideAsarArchive('/usr/local/lib/liblattice.so')).toBe(false);
  });

  test('rewrites archived paths to their unpacked twin', () => {
    expect(library.fixPathForAsarUnpack('/app/resources/app.asar/lib/liblattice.so')).toBe(
      '/app/resources/app.asar.unpacked/lib/liblattice.so'
    );
    expect(library.fixPathForAsarUnpack('C:\\app\\resources\\app.asar\\lattice.dll')).toBe(
      'C:\\app\\resources\\app.asar.unpacked\\lattice.dll'
    );
  });

  test('leaves non-archived paths untouched', () => {
    expect(library.fixPathForAsarUnpack('/usr/local/lib/liblattice.so')).toBe(
      '/usr/local/lib/liblattice.so'
    );
  });
});

describe('Electron runtime detection', () => {
  test('plain Node is not an Electron runtime', async () => {
    const library = await freshLibraryModule();
    expect(library.isElectronRuntime()).toBe(false);
  });

  test('reports an Electron runtime when process.versions.electron is set', async () => {
    const versions = process.versions as NodeJS.ProcessVersions & { electron?: string };
    versions.electron = '31.0.0';
    try {
      const library = await freshLibraryModule();
      expect(library.isElectronRuntime()).toBe(true);
    } finally {
      delete versions.electron;
    }
  });
});

describe('Electron resource candidates', () => {
  test('no candidates outside Electron', async () => {
    const library = await freshLibraryModule();
    expect(library.getElectronLibraryCandidates()).toEqual([]);
  });

  test('covers asarUnpack, unpacked app, and extraResources layouts', async () => {
    const resourcesPath = path.join(os.tmpdir(), 'lattice-electron-resources');
    const withResources = process as NodeJS.Process & { resourcesPath?: string };
    withResources.resourcesPath = resourcesPath;

    try {
      const library = await freshLibraryModule();
      const candidates = library.getElectronLibraryCandidates();
      const libName = nativeLibName();
      const platformDir = library.getBundledPlatformDirs()[0]!;

      expect(candidates).toContain(
        path.join(
          resourcesPath,
          'app.asar.unpacked',
          'node_modules',
          '@hajewski/latticedb',
          'lib',
          platformDir,
          libName
        )
      );
      expect(candidates).toContain(
        path.join(
          resourcesPath,
          'app',
          'node_modules',
          '@hajewski/latticedb',
          'lib',
          platformDir,
          libName
        )
      );
      expect(candidates).toContain(path.join(resourcesPath, 'lib', platformDir, libName));
      expect(candidates).toContain(path.join(resourcesPath, libName));
      expect(candidates).toContain(path.join(path.dirname(process.execPath), libName));
    } finally {
      delete withResources.resourcesPath;
    }
  });
});

describe('Windows bundles', () => {
  let library: LibraryModule;

  beforeAll(async () => {
    // Loaded before any platform is forced: koffi resolves its own native
    // module from process.platform/arch at require time, so re-importing under
    // a foreign arch would fail to find koffi rather than exercise the lookup.
    library = await freshLibraryModule();
  });

  /**
   * Run `body` with `process.platform` and `process.arch` forced, so the
   * Windows lookups are exercised from any host. The loader reads both at call
   * time, so no module reload is needed.
   */
  function asPlatform(
    platform: NodeJS.Platform,
    arch: string,
    body: (library: LibraryModule) => void
  ): void {
    const platformDescriptor = Object.getOwnPropertyDescriptor(process, 'platform')!;
    const archDescriptor = Object.getOwnPropertyDescriptor(process, 'arch')!;
    Object.defineProperty(process, 'platform', { value: platform, configurable: true });
    Object.defineProperty(process, 'arch', { value: arch, configurable: true });

    try {
      body(library);
    } finally {
      Object.defineProperty(process, 'platform', platformDescriptor);
      Object.defineProperty(process, 'arch', archDescriptor);
    }
  }

  test('x64 resolves the win32-x64 bundle', () => {
    asPlatform('win32', 'x64', (library) => {
      expect(library.getBundledPlatformDirs()).toEqual(['win32-x64']);
      expect(library.getBundledLibraryCandidates('/pkg/dist/ffi')).toEqual([
        path.join('/pkg', 'lib', 'win32-x64', 'lattice.dll'),
      ]);
    });
  });

  test('arm64 resolves the win32-arm64 bundle', async () => {
    await asPlatform('win32', 'arm64', (library) => {
      expect(library.getBundledPlatformDirs()).toEqual(['win32-arm64']);
      expect(library.getBundledLibraryCandidates('/pkg/dist/ffi')).toEqual([
        path.join('/pkg', 'lib', 'win32-arm64', 'lattice.dll'),
      ]);
    });
  });

  test('Electron resource candidates carry the Windows library name', async () => {
    const resourcesPath = path.join(os.tmpdir(), 'lattice-electron-win');
    const withResources = process as NodeJS.Process & { resourcesPath?: string };
    withResources.resourcesPath = resourcesPath;

    try {
      await asPlatform('win32', 'arm64', (library) => {
        const candidates = library.getElectronLibraryCandidates();
        expect(candidates).toContain(
          path.join(
            resourcesPath,
            'app.asar.unpacked',
            'node_modules',
            '@hajewski/latticedb',
            'lib',
            'win32-arm64',
            'lattice.dll'
          )
        );
        expect(candidates).toContain(path.join(resourcesPath, 'lib', 'win32-arm64', 'lattice.dll'));
        expect(candidates.every((candidate) => candidate.endsWith('lattice.dll'))).toBe(true);
      });
    } finally {
      delete withResources.resourcesPath;
    }
  });
});

describe('packaged library resolution', () => {
  let appRoot: string;
  let libName: string;

  beforeEach(() => {
    appRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'lattice-asar-'));
    libName = nativeLibName();
  });

  afterEach(() => {
    fs.rmSync(appRoot, { recursive: true, force: true });
  });

  test('prefers the unpacked twin when the archived path is requested', async () => {
    const archivedDir = path.join(appRoot, 'app.asar', 'lib');
    const unpackedDir = path.join(appRoot, 'app.asar.unpacked', 'lib');
    fs.mkdirSync(archivedDir, { recursive: true });
    fs.mkdirSync(unpackedDir, { recursive: true });
    fs.writeFileSync(path.join(archivedDir, libName), '');
    fs.writeFileSync(path.join(unpackedDir, libName), '');

    await withEnv(
      { LATTICE_LIB_PATH: path.join(archivedDir, libName), LATTICE_PREFIX: undefined },
      async () => {
        const library = await freshLibraryModule();
        expect(library.resolveLibraryPath()).toBe(path.join(unpackedDir, libName));
      }
    );
  });

  test('resolves the unpacked twin when an archived directory is requested', async () => {
    const archivedDir = path.join(appRoot, 'app.asar', 'lib');
    const unpackedDir = path.join(appRoot, 'app.asar.unpacked', 'lib');
    fs.mkdirSync(archivedDir, { recursive: true });
    fs.mkdirSync(unpackedDir, { recursive: true });
    fs.writeFileSync(path.join(unpackedDir, libName), '');

    await withEnv({ LATTICE_LIB_PATH: archivedDir, LATTICE_PREFIX: undefined }, async () => {
      const library = await freshLibraryModule();
      expect(library.resolveLibraryPath()).toBe(path.join(unpackedDir, libName));
    });
  });

  test('never hands an archived path to the native loader', async () => {
    const archivedDir = path.join(appRoot, 'app.asar', 'lib');
    fs.mkdirSync(archivedDir, { recursive: true });
    const archivedLibrary = path.join(archivedDir, libName);
    fs.writeFileSync(archivedLibrary, '');

    await withEnv(
      { LATTICE_LIB_PATH: archivedLibrary, LATTICE_PREFIX: undefined },
      async () => {
        const library = await freshLibraryModule();
        // Other search steps may still find a real library on this machine;
        // what matters is that the archived candidate is never chosen.
        expect(library.resolveLibraryPath()).not.toBe(archivedLibrary);
      }
    );
  });
});

describe('packaging guidance in errors', () => {
  test('explains asarUnpack when the library is stuck inside an archive', async () => {
    const library = await freshLibraryModule();
    const archived = '/app/resources/app.asar/node_modules/@hajewski/latticedb/lib/lattice.dll';
    const message = library.buildNotFoundError([archived]).message;

    expect(message).toContain(archived);
    expect(message).toContain('asarUnpack');
    expect(message).toContain(library.ELECTRON_ASAR_UNPACK_GLOB);
  });

  test('mentions Electron packaging when running under Electron', async () => {
    const versions = process.versions as NodeJS.ProcessVersions & { electron?: string };
    versions.electron = '31.0.0';
    try {
      const library = await freshLibraryModule();
      const message = library.buildNotFoundError([]).message;
      expect(message).toContain('Electron');
      expect(message).toContain('LATTICE_LIB_PATH');
    } finally {
      delete versions.electron;
    }
  });

  test('stays Electron-free outside Electron', async () => {
    const library = await freshLibraryModule();
    expect(library.buildNotFoundError([]).message).not.toContain('Electron');
  });

  test('unpack glob targets the bundled native directory', async () => {
    const library = await freshLibraryModule();
    expect(library.ELECTRON_ASAR_UNPACK_GLOB).toBe(
      '**/node_modules/@hajewski/latticedb/lib/**'
    );
  });
});
