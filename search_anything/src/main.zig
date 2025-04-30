const std     = @import("std");
const builtin = @import("builtin");

const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;

const server = @import("server/server.zig");


fn bench(filename: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var index_manager = try IndexManager.init(allocator);

    defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    }

    try index_manager.readHeader(filename, FileType.CSV);
    try index_manager.scanFile();

    try index_manager.addSearchCol("title");
    try index_manager.addSearchCol("artist");
    try index_manager.addSearchCol("album");

    try index_manager.indexFile();

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "UNDER MY SKIN");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "LIGHTNING");

    const num_queries: usize = 5_000;

    const start_time = std.time.milliTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );
    }
    const end_time = std.time.milliTimestamp();
    const execution_time_ms: usize = @intCast(end_time - start_time);
    const qps = @divFloor(num_queries * 1000, execution_time_ms);

    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});
}

fn serveHTML(filename: []const u8) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var index_manager = try IndexManager.init(allocator);

    defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    }

    try index_manager.readHeader(filename, FileType.CSV);
    try index_manager.scanFile();

    try index_manager.addSearchCol("title");
    try index_manager.addSearchCol("artist");
    try index_manager.addSearchCol("album");

    try index_manager.indexFile();

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    var server_handler = try server.QueryHandlerZap.init(
        &index_manager,
        boost_factors,
        );
    defer server_handler.deinit();

    try server_handler.serve();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len != 2) {
        std.debug.print("Usage: {s} <filename>\n", .{args[0]});

        for (args) |arg| {
            std.debug.print("Arg: {s}\n", .{arg});
        }
        return error.InvalidArguments;
    }

    const filename = args[1];
    try serveHTML(filename);

    // const filename = "../data/mb_small.csv";
    // try bench(filename);
}
