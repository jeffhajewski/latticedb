const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the main library module
    const lib_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add self-import so internal files can use @import("lattice")
    lib_module.addImport("lattice", lib_module);

    // Core static library
    const lib = b.addLibrary(.{
        .name = "lattice",
        .root_module = lib_module,
        .linkage = .static,
    });

    // Shared library module (for Python/FFI bindings)
    // For macOS, set minimum deployment target for compatibility with node-gyp
    var shared_target = target;
    if (target.result.os.tag == .macos) {
        var query = target.query;
        query.os_version_min = .{ .semver = .{ .major = 11, .minor = 0, .patch = 0 } };
        shared_target = b.resolveTargetQuery(query);
    }

    const shared_lib_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = shared_target,
        .optimize = optimize,
    });
    shared_lib_module.addImport("lattice", shared_lib_module);

    // Shared library
    const shared_lib = b.addLibrary(.{
        .name = "lattice",
        .root_module = shared_lib_module,
        .linkage = .dynamic,
    });

    // CLI module - imports the library module
    const cli_module = b.createModule(.{
        .root_source_file = b.path("src/cli/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // CLI executable
    const cli = b.addExecutable(.{
        .name = "lattice",
        .root_module = cli_module,
    });
    cli.linkLibrary(lib);

    // Unit test module - imports the library module
    const unit_test_module = b.createModule(.{
        .root_source_file = b.path("tests/unit/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // Unit tests
    const unit_tests = b.addTest(.{
        .root_module = unit_test_module,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    // Library test module
    const lib_test_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add self-import for tests
    lib_test_module.addImport("lattice", lib_test_module);

    // Library tests (tests within src/)
    const lib_tests = b.addTest(.{
        .root_module = lib_test_module,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Install artifacts
    b.installArtifact(lib);
    b.installArtifact(shared_lib);
    b.installArtifact(cli);

    // Build steps
    const lib_step = b.step("lib", "Build static library only");
    lib_step.dependOn(&lib.step);

    const shared_step = b.step("shared", "Build shared library (for Python bindings)");
    const install_shared = b.addInstallArtifact(shared_lib, .{});
    shared_step.dependOn(&install_shared.step);

    const cli_step = b.step("cli", "Build CLI executable only");
    const install_cli = b.addInstallArtifact(cli, .{});
    cli_step.dependOn(&install_cli.step);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_lib_tests.step);

    // Integration test module - imports the library module
    const integration_test_module = b.createModule(.{
        .root_source_file = b.path("tests/integration/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // Integration tests
    const integration_tests = b.addTest(.{
        .root_module = integration_test_module,
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const integration_test_step = b.step("integration-test", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);

    // Benchmark module - imports the library module
    const bench_module = b.createModule(.{
        .root_source_file = b.path("tests/benchmark/main.zig"),
        .target = target,
        .optimize = .ReleaseFast, // Always optimize benchmarks
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // Benchmark executable
    const bench_exe = b.addExecutable(.{
        .name = "lattice-bench",
        .root_module = bench_module,
    });
    bench_exe.linkLibrary(lib);

    const run_bench = b.addRunArtifact(bench_exe);
    run_bench.step.dependOn(&bench_exe.step);

    const bench_step = b.step("benchmark", "Run performance benchmarks");
    bench_step.dependOn(&run_bench.step);

    // Stress test module
    const stress_module = b.createModule(.{
        .root_source_file = b.path("tests/benchmark/stress_test.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // Stress test executable
    const stress_exe = b.addExecutable(.{
        .name = "lattice-stress",
        .root_module = stress_module,
    });
    stress_exe.linkLibrary(lib);

    const run_stress = b.addRunArtifact(stress_exe);
    run_stress.step.dependOn(&stress_exe.step);

    const stress_step = b.step("stress", "Run stress tests to find performance limits");
    stress_step.dependOn(&run_stress.step);

    // SQLite comparison benchmark module
    const sqlite_bench_module = b.createModule(.{
        .root_source_file = b.path("tests/benchmark/sqlite_comparison.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // SQLite comparison benchmark executable
    const sqlite_bench_exe = b.addExecutable(.{
        .name = "sqlite-benchmark",
        .root_module = sqlite_bench_module,
    });
    sqlite_bench_exe.linkLibrary(lib);
    sqlite_bench_exe.linkSystemLibrary("sqlite3");

    const run_sqlite_bench = b.addRunArtifact(sqlite_bench_exe);
    run_sqlite_bench.step.dependOn(&sqlite_bench_exe.step);

    const sqlite_bench_step = b.step("sqlite-benchmark", "Run SQLite vs LatticeDB comparison benchmark");
    sqlite_bench_step.dependOn(&run_sqlite_bench.step);

    // Fuzz test module - imports the library module
    const fuzz_module = b.createModule(.{
        .root_source_file = b.path("tests/fuzz/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "lattice", .module = lib_module },
        },
    });

    // Fuzz tests (run with: zig build fuzz -- --fuzz)
    const fuzz_tests = b.addTest(.{
        .root_module = fuzz_module,
    });
    const run_fuzz_tests = b.addRunArtifact(fuzz_tests);
    if (b.args) |args| {
        run_fuzz_tests.addArgs(args);
    }

    const fuzz_step = b.step("fuzz", "Run fuzz tests (add -- --fuzz for continuous fuzzing)");
    fuzz_step.dependOn(&run_fuzz_tests.step);

    // Run CLI
    const run_cli = b.addRunArtifact(cli);
    run_cli.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cli.addArgs(args);
    }

    const run_step = b.step("run", "Run the CLI");
    run_step.dependOn(&run_cli.step);
}
