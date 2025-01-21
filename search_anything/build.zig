const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zap = b.dependency("zap", .{
        .target = target,
        .optimize = optimize,
        .openssl = false, // set to true to enable TLS support
    });

    const lib = b.addStaticLibrary(.{
        .name = "search_anything",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.root_module.addImport("zap", zap.module("zap"));
    b.installArtifact(lib);

    const shared_lib = b.addSharedLibrary(.{
        .name = "search_app",
        .root_source_file = b.path("src/server.zig"),
        .target = target,
        // .optimize = .ReleaseFast,
        .optimize = .ReleaseSafe,
        // .optimize = .Debug,
    });
    const shared_install = b.addInstallArtifact(shared_lib, .{});
    b.installArtifact(shared_lib);

    const exe = b.addExecutable(.{
        .name = "search_anything",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("zap", zap.module("zap"));

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const lib_step = b.step("lib", "Build the static library");
    const shared_step = b.step("shared", "Build the shared library");
    const exe_step = b.step("exe", "Build the executable");
    lib_step.dependOn(&lib.step);
    shared_step.dependOn(&shared_lib.step);
    shared_step.dependOn(&shared_install.step);
    exe_step.dependOn(&exe.step);

    // const tests = b.addTest(.{
            // .target = target,
            // .optimize = optimize,
            // .root_source_file = b.path("src/root.zig"),
    // });
    // tests.root_module.addImport("zap", zap.module("zap"));
 
    // const test_cmd = b.addRunArtifact(tests);
    // test_cmd.step.dependOn(b.getInstallStep());
    // const test_step = b.step("test", "Run the tests");
    // test_step.dependOn(&test_cmd.step);


    // const radix_lib = b.addSharedLibrary(.{
        // .name = "radix_trie",
        // .root_source_file = b.path("src/radix_bindings.zig"),
        // .target = target,
        // .optimize = optimize,
    // });
    // radix_lib.linkLibC();
    // b.installArtifact(radix_lib);
    
    // const header_install = b.addInstallFileWithDir(
        // b.path("src/radix.h"),
        // .{ .custom = "include" },
        // "radix.h"
    // );
    
    // // Make header installation part of the default install step
    // b.getInstallStep().dependOn(&header_install.step);

    // const py_install = b.addSystemCommand(&.{
        // "bash",
        // "py_install.sh",
    // });
    // b.getInstallStep().dependOn(&py_install.step);
}
