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

    // Core static library
    const lib = b.addLibrary(.{
        .name = "lattice",
        .root_module = lib_module,
        .linkage = .static,
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

    // Library tests (tests within src/)
    const lib_tests = b.addTest(.{
        .root_module = lib_test_module,
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Install artifacts
    b.installArtifact(lib);
    b.installArtifact(cli);

    // Build steps
    const lib_step = b.step("lib", "Build static library only");
    lib_step.dependOn(&lib.step);

    const cli_step = b.step("cli", "Build CLI executable only");
    cli_step.dependOn(&cli.step);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_lib_tests.step);

    // Run CLI
    const run_cli = b.addRunArtifact(cli);
    run_cli.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cli.addArgs(args);
    }

    const run_step = b.step("run", "Run the CLI");
    run_step.dependOn(&run_cli.step);
}
