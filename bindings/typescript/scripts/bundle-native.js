#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const packageRoot = path.resolve(__dirname, '..');
const libRoot = path.join(packageRoot, 'lib');

const targets = [
  { zigTarget: 'x86_64-linux-gnu', platformDir: 'linux-x64-gnu', libName: 'liblattice.so' },
  { zigTarget: 'x86_64-linux-musl', platformDir: 'linux-x64-musl', libName: 'liblattice.so' },
  { zigTarget: 'aarch64-linux-gnu', platformDir: 'linux-arm64-gnu', libName: 'liblattice.so' },
  { zigTarget: 'aarch64-linux-musl', platformDir: 'linux-arm64-musl', libName: 'liblattice.so' },
  { zigTarget: 'x86_64-macos', platformDir: 'darwin-x64', libName: 'liblattice.dylib' },
  { zigTarget: 'aarch64-macos', platformDir: 'darwin-arm64', libName: 'liblattice.dylib' },
  { zigTarget: 'x86_64-windows-gnu', platformDir: 'win32-x64', libName: 'lattice.dll' },
];

const publishTargets = targets.filter((target) => target.platformDir !== 'win32-x64');

function findRepoRoot(startDir) {
  const explicitRoot = process.env.LATTICE_REPO_ROOT;
  if (explicitRoot) {
    return path.resolve(explicitRoot);
  }

  let current = startDir;
  while (true) {
    if (fs.existsSync(path.join(current, 'build.zig'))) {
      return current;
    }

    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function parseArgs(argv) {
  const args = {
    mode: 'current',
    libDir: process.env.LATTICE_BUNDLE_LIB_DIR,
    libPath: process.env.LATTICE_BUNDLE_LIB_PATH,
  };

  for (const arg of argv) {
    if (arg === '--all') {
      args.mode = 'all';
    } else if (arg === '--current') {
      args.mode = 'current';
    } else if (arg.startsWith('--lib-dir=')) {
      args.libDir = arg.slice('--lib-dir='.length);
    } else if (arg.startsWith('--lib-path=')) {
      args.libPath = arg.slice('--lib-path='.length);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (args.mode === 'all' && (args.libDir || args.libPath)) {
    throw new Error('--all cannot be combined with LATTICE_BUNDLE_LIB_DIR or LATTICE_BUNDLE_LIB_PATH');
  }

  return args;
}

function detectLinuxLibc() {
  if (process.platform !== 'linux') {
    return null;
  }

  if (process.report && typeof process.report.getReport === 'function') {
    const report = process.report.getReport();
    if (report && report.header && report.header.glibcVersionRuntime) {
      return 'gnu';
    }
  }

  const ldd = spawnSync('ldd', ['--version'], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const lddOutput = `${ldd.stdout || ''}\n${ldd.stderr || ''}`.toLowerCase();
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

function getCurrentPlatformDir() {
  if (process.platform === 'linux') {
    const libc = detectLinuxLibc();
    if (libc) {
      return `linux-${process.arch}-${libc}`;
    }
    return `linux-${process.arch}`;
  }

  return `${process.platform}-${process.arch}`;
}

function getCurrentTarget() {
  const platformDir = getCurrentPlatformDir();
  const target = targets.find((candidate) => candidate.platformDir === platformDir);
  if (!target) {
    throw new Error(
      `No bundled native target is configured for ${platformDir}. ` +
      'Use LATTICE_LIB_PATH/LATTICE_PREFIX at runtime or add a package target.'
    );
  }
  return target;
}

function buildTarget(target) {
  const repoRoot = findRepoRoot(packageRoot);
  if (!repoRoot) {
    throw new Error(
      'Could not find the repository root with build.zig. ' +
      'Set LATTICE_REPO_ROOT, or provide LATTICE_BUNDLE_LIB_DIR / LATTICE_BUNDLE_LIB_PATH.'
    );
  }

  const env = {
    ...process.env,
    ZIG_GLOBAL_CACHE_DIR:
      process.env.ZIG_GLOBAL_CACHE_DIR || path.join(os.tmpdir(), 'latticedb-zig-global-cache'),
    ZIG_LOCAL_CACHE_DIR:
      process.env.ZIG_LOCAL_CACHE_DIR || path.join(os.tmpdir(), 'latticedb-zig-local-cache'),
  };

  const result = spawnSync(
    'zig',
    ['build', 'shared', `-Dtarget=${target.zigTarget}`, '-Doptimize=ReleaseFast'],
    {
      cwd: repoRoot,
      env,
      stdio: 'inherit',
    }
  );

  if (result.status !== 0) {
    process.exit(result.status || 1);
  }

  const builtLibrary = path.join(repoRoot, 'zig-out', 'lib', target.libName);
  if (!fs.existsSync(builtLibrary)) {
    throw new Error(`Built library not found: ${builtLibrary}`);
  }
  return builtLibrary;
}

function resolveSourceLibrary(target, args) {
  if (args.libPath) {
    const explicitPath = fs.statSync(args.libPath).isDirectory()
      ? path.join(args.libPath, target.libName)
      : args.libPath;
    if (!fs.existsSync(explicitPath)) {
      throw new Error(`Bundled library not found: ${explicitPath}`);
    }
    return explicitPath;
  }

  if (args.libDir) {
    const explicitPath = path.join(args.libDir, target.libName);
    if (!fs.existsSync(explicitPath)) {
      throw new Error(`Bundled library not found: ${explicitPath}`);
    }
    return explicitPath;
  }

  return buildTarget(target);
}

function cleanLibRoot() {
  fs.rmSync(libRoot, { recursive: true, force: true });
}

function copyTargetLibrary(target, sourceLibrary) {
  const targetDir = path.join(libRoot, target.platformDir);
  fs.mkdirSync(targetDir, { recursive: true });

  const destination = path.join(targetDir, target.libName);
  fs.copyFileSync(sourceLibrary, destination);
  console.log(`Bundled ${target.platformDir}: ${destination}`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const selectedTargets = args.mode === 'all' ? publishTargets : [getCurrentTarget()];

  cleanLibRoot();

  for (const target of selectedTargets) {
    const sourceLibrary = resolveSourceLibrary(target, args);
    copyTargetLibrary(target, sourceLibrary);
  }

  if (selectedTargets.length > 1) {
    console.log(`Bundled ${selectedTargets.length} native libraries into ${libRoot}`);
  } else {
    console.log(`Bundled native library for ${selectedTargets[0].platformDir}`);
  }
}

main();
