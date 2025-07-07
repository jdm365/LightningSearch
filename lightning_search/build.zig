const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zap = b.dependency("zap", .{
        .target = target,
        .optimize = optimize,
        .openssl = false,
    });

    const shared_lib = b.addSharedLibrary(.{
        .name = "search_app",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        // .optimize = .ReleaseFast,
        // .optimize = .ReleaseSafe,
        // .optimize = .Debug,
    });
    shared_lib.linkLibC();
    shared_lib.linkSystemLibrary("unwind");
    shared_lib.addIncludePath(b.path("lib"));
    shared_lib.addObjectFile(b.path("lib/libparquet_bindings.a"));
    shared_lib.installHeader(b.path("lib/parquet_bindings.h"), "parquet_bindings.h");


    const shared_install = b.addInstallArtifact(shared_lib, .{});
    b.installArtifact(shared_lib);

    const exe = b.addExecutable(.{
        .name = "lightning_search.bin",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.linkLibC();
    exe.linkSystemLibrary("unwind");
    exe.addIncludePath(b.path("lib"));
    exe.addObjectFile(b.path("lib/libparquet_bindings.a"));
    exe.installHeader(b.path("lib/parquet_bindings.h"), "parquet_bindings.h");
    // exe.linkFramework("CoreFoundation");

    exe.root_module.addImport("zap", zap.module("zap"));
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const shared_step = b.step("shared", "Build the shared library");
    const exe_step = b.step("exe", "Build the executable");
    shared_step.dependOn(&shared_lib.step);
    shared_step.dependOn(&shared_install.step);
    exe_step.dependOn(&exe.step);



     const tests = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Apply same dependencies as your executable
    tests.linkLibC();
    tests.linkSystemLibrary("unwind");
    tests.addIncludePath(b.path("lib"));
    tests.addObjectFile(b.path("lib/libparquet_bindings.a"));
    tests.root_module.addImport("zap", zap.module("zap"));

    // Create run step that actually executes the tests
    const run_tests = b.addRunArtifact(tests);

    // Create top-level test step
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);
}
