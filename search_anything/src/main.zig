const std     = @import("std");
const builtin = @import("builtin");

// const zap = @import("zap");

const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;

const server = @import("server/server.zig");


fn bench(testing: bool) !void {
    // const filename: []const u8 = "../tests/mb_small.csv";
    const filename: []const u8 = "../tests/mb.csv";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    // const allocator = std.heap.c_allocator;

    var search_cols = std.ArrayList([]u8).init(allocator);
    const title:  []u8 = try allocator.dupe(u8, "Title");
    const artist: []u8 = try allocator.dupe(u8, "ARTIST");
    const album:  []u8 = try allocator.dupe(u8, "ALBUM");

    try search_cols.append(title);
    try search_cols.append(artist);
    try search_cols.append(album);

    // var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    var index_manager = try IndexManager.init(filename, &search_cols);
    try index_manager.readFile();

    try index_manager.printDebugInfo();

    defer {
        allocator.free(title);
        allocator.free(artist);
        allocator.free(album);

        search_cols.deinit();
        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "UNDER MY SKIN");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "LIGHTNING");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(1.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    const num_queries: usize = if (testing) 1 else 5_000;

    const start_time = std.time.milliTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );
    }
    const end_time = std.time.milliTimestamp();
    const execution_time_ms = (end_time - start_time);
    const qps = @as(f64, @floatFromInt(num_queries)) / @as(f64, @floatFromInt(execution_time_ms)) * 1000;

    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});
}

fn serveHTML() !void {
    const filename: []const u8 = "../data/mb_small.csv";
    // const filename: []const u8 = "../data/mb.csv";

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
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
// 
    // // const filename: []const u8 = "../data/mb.csv";
    // const filename: []const u8 = "../data/mb_small.csv";
// 
    // var index_manager = try IndexManager.init(gpa.allocator());
// 
    // defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    // }
// 
    // // try index_manager.readHeader(filename, FileType.PARQUET);
    // try index_manager.readHeader(filename, FileType.CSV);
    // try index_manager.scanFile();
// 
    // try index_manager.addSearchCol("title");
    // try index_manager.addSearchCol("artist");
    // try index_manager.addSearchCol("album");
// 
    // try index_manager.indexFile();
// 
    // var query_map = std.StringHashMap([]const u8).init(gpa.allocator());
    // defer query_map.deinit();
// 
    // try query_map.put("TITLE", "UNDER MY SKIN");
    // try query_map.put("ARTIST", "FRANK SINATRA");
    // try query_map.put("ALBUM", "LIGHTNING");
// 
    // var boost_factors = std.ArrayList(f32).init(gpa.allocator());
    // defer boost_factors.deinit();
// 
    // try boost_factors.append(1.0);
    // try boost_factors.append(1.0);
    // try boost_factors.append(1.0);
// 
    // const num_queries: usize = 1;
    // const K: usize = 25;
// 
    // const start_time = std.time.milliTimestamp();
    // for (0..num_queries) |_| {
        // try index_manager.query(
            // query_map,
            // K,
            // boost_factors,
            // );
    // }
    // const end_time = std.time.milliTimestamp();
    // const execution_time_ms = (end_time - start_time);
    // const qps = @as(f64, @floatFromInt(num_queries)) / @as(f64, @floatFromInt(execution_time_ms)) * 1000;
// 
    // std.debug.print("\n\n================================================\n", .{});
    // std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    // std.debug.print("================================================\n", .{});
// 
    // var query_it = query_map.iterator();
    // while (query_it.next()) |val| {
        // std.debug.print("Query - {s}: {s}\n", .{val.key_ptr.*, val.value_ptr.*});
    // }
// 
    // var column_names = std.ArrayList([]const u8).init(index_manager.stringArena());
    // try column_names.resize(index_manager.columns.num_keys);
// 
    // var it = try index_manager.columns.iterator();
    // while (try it.next()) |val| {
        // column_names.items[val.value] = val.key;
    // }
// 
    // for (0..K) |idx| {
        // const line = try server.csvLineToJson(
            // index_manager.scratchArena(),
            // index_manager.query_state.result_strings[idx],
            // index_manager.query_state.result_positions[idx],
            // column_names,
        // );
// 
        // std.debug.print("Result {d}\n", .{idx});
        // line.dump();
    // }

    try serveHTML();
}
