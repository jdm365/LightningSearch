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
        .name = "csv_search",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.root_module.addImport("zap", zap.module("zap"));
    b.installArtifact(lib);

    const exe = b.addExecutable(.{
        .name = "csv_search",
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

    // const install_step = b.step("install_local", "Install the app");
    // const install_step = b.step("install", "Install the binary in /usr/local/bin");
    // install_step.dependOn(b.getInstallStep());

    const tests = b.addTest(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/root.zig"),
    });
    tests.root_module.addImport("zap", zap.module("zap"));

    const test_cmd = b.addRunArtifact(tests);
    test_cmd.step.dependOn(b.getInstallStep());
    const test_step = b.step("test", "Run the tests");
    test_step.dependOn(&test_cmd.step);


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
