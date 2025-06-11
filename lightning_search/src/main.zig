const std     = @import("std");
const builtin = @import("builtin");

const SHM = @import("indexing/index.zig").SHM;
const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;

const server = @import("server/server.zig");


fn bench(filename: []const u8) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();

    var index_manager = try IndexManager.init(allocator);

    defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    }

    var filetype: FileType = undefined;

    if (std.mem.endsWith(u8, filename, ".csv")) {
        filetype = FileType.CSV;
    } else if (std.mem.endsWith(u8, filename, ".parquet")) {
        filetype = FileType.PARQUET;
    } else if (std.mem.endsWith(u8, filename, ".json")) {
        filetype = FileType.JSON;
    } else {
        @panic("Unsupported filetype.");
    }

    try index_manager.readHeader(filename, filetype);
    try index_manager.scanFile();

    if (std.mem.startsWith(u8, filename, "../data/hn")) {
        // try index_manager.addSearchCol("story_url");
        try index_manager.addSearchCol("story_text");
        // try index_manager.addSearchCol("story_author");
        try index_manager.addSearchCol("comment_text");
        // try index_manager.addSearchCol("comment_author");
    } else if (std.mem.startsWith(u8, filename, "../data/enwik")) {
        try index_manager.addSearchCol("text");
    } else if (std.mem.startsWith(u8, filename, "../data/mb")) {
        try index_manager.addSearchCol("title");
        try index_manager.addSearchCol("artist");
        try index_manager.addSearchCol("album");
    } else {
        @panic("File not supported yet.");
    }

    try index_manager.indexFile();

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);
    // try boost_factors.append(1.0);
    // try boost_factors.append(1.0);

    var query_map = SHM.init(allocator);
    defer query_map.deinit();

    if (std.mem.startsWith(u8, filename, "../data/hn")) {
        try query_map.put("STORY_TEXT", "zig");
        try query_map.put("COMMENT_TEXT", "gotta go fast");
    } else if (std.mem.startsWith(u8, filename, "../data/enwik")) {
        try query_map.put("TEXT", "griffith observatory");
    } else if (std.mem.startsWith(u8, filename, "../data/mb")) {
        try query_map.put("TITLE", "UNDER MY SKIN");
        try query_map.put("ARTIST", "FRANK SINATRA");
        try query_map.put("ALBUM", "LIGHTNING");
    } else {
        @panic("File not supported yet.");
    }

    const num_queries: usize = 10_000;

    const start_time = std.time.microTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );

        // std.debug.print(
            // "Query result: {s}\n",
            // .{index_manager.query_state.result_strings[0].items[0..256]},
        // );
    }
    const end_time = std.time.microTimestamp();
    const execution_time_us: usize = @intCast(end_time - start_time);
    const qps = @divFloor(num_queries * 1_000_000, execution_time_us);
    const avg_latency = @divFloor(execution_time_us, num_queries);

    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n",   .{qps});
    std.debug.print("AVG LATENCY:        {d}us\n", .{avg_latency});
    std.debug.print("================================================\n", .{});
}

fn serveHTML(filename: []const u8) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();

    var index_manager = try IndexManager.init(allocator);

    defer {
        // index_manager.deinit(gpa.allocator()) catch {};
        // _ = gpa.deinit();
    }

    var filetype: FileType = undefined;

    if (std.mem.endsWith(u8, filename, ".csv")) {
        filetype = FileType.CSV;
    } else if (std.mem.endsWith(u8, filename, ".parquet")) {
        filetype = FileType.PARQUET;
    } else if (std.mem.endsWith(u8, filename, ".json")) {
        filetype = FileType.JSON;
    } else {
        @panic("Unsupported filetype.");
    }

    try index_manager.readHeader(filename, filetype);
    try index_manager.scanFile();

    if (std.mem.startsWith(u8, filename, "../data/hn")) {
        // try index_manager.addSearchCol("story_url");
        try index_manager.addSearchCol("story_text");
        // try index_manager.addSearchCol("story_author");
        try index_manager.addSearchCol("comment_text");
        // try index_manager.addSearchCol("comment_author");
    } else if (std.mem.startsWith(u8, filename, "../data/enwik")) {
        try index_manager.addSearchCol("text");
    } else if (std.mem.startsWith(u8, filename, "../data/mb")) {
        try index_manager.addSearchCol("title");
        try index_manager.addSearchCol("artist");
        try index_manager.addSearchCol("album");
    } else {
        @panic("File not supported yet.");
    }

    try index_manager.indexFile();

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    // try boost_factors.append(1.0);
    // try boost_factors.append(1.0);

    var server_handler = try server.QueryHandlerZap.init(
        &index_manager,
        boost_factors,
        );
    defer server_handler.deinit();

    try server_handler.serve();
}

pub fn main() !void {
    // var gpa = std.heap.DebugAllocator(.{}){};
    // const allocator = gpa.allocator();
    // defer _ = gpa.deinit();
// 
    // const args = try std.process.argsAlloc(allocator);
    // defer std.process.argsFree(allocator, args);
// 
    // if (args.len != 2) {
        // std.debug.print("Usage: {s} <filename>\n", .{args[0]});
// 
        // for (args) |arg| {
            // std.debug.print("Arg: {s}\n", .{arg});
        // }
        // return error.InvalidArguments;
    // }
// 
    // const filename = args[1];
    // try serveHTML(filename);

    // const filename = "../data/mb_small.csv";
    const filename = "../data/mb.csv";
    // const filename = "../data/enwiki.csv";
    // const filename = "../data/enwiki_small.csv";
    // const filename = "../data/mb.parquet";
    // const filename = "../data/hn.csv";
    // const filename = "../data/hn_half.csv";
    try bench(filename);
}
