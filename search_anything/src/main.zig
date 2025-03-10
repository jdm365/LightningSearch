const std     = @import("std");
const builtin = @import("builtin");

const zap = @import("zap");

const IndexManager = @import("index_manager.zig").IndexManager;
const FileType = @import("file_utils.zig").FileType;

const server = @import("server.zig");


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

pub fn main() !void {
    // const filename: [*:0]const u8 = "../data/mb.csv";
    // const filename: [*:0]const u8 = "../data/mb_small.csv";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};

    // const filename: []const u8 = "../data/mb.csv";
    const filename: []const u8 = "../data/mb.parquet";

    var index_manager = try IndexManager.init(gpa.allocator());

    defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    }

    try index_manager.readHeader(filename, FileType.PARQUET);
    // try index_manager.readHeader(filename, FileType.CSV);
    try index_manager.scanFile();

    try index_manager.addSearchCol("title");
    try index_manager.addSearchCol("artist");
    try index_manager.addSearchCol("album");

    try index_manager.indexFile();

    var query_map = std.StringHashMap([]const u8).init(gpa.allocator());
    defer query_map.deinit();

    try query_map.put("TITLE", "UNDER MY SKIN");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "LIGHTNING");

    var boost_factors = std.ArrayList(f32).init(gpa.allocator());
    defer boost_factors.deinit();

    try boost_factors.append(1.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    const num_queries: usize = 1;

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

    // server.init_allocators();
    // const QH = @as(*server.QueryHandlerLocal, @alignCast(@ptrCast(server.getQueryHandlerLocal())));
    // QH.readHeader(filename);
    // server.scanFile(QH);
    // server.addSearchCol(QH, "title");
    // server.addSearchCol(QH, "artist");
    // server.addSearchCol(QH, "album");
 
    // server.indexFile(QH);
}
