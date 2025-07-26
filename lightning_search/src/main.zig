const std     = @import("std");
const builtin = @import("builtin");

const misc = @import("utils/misc_utils.zig");
const SHM = @import("indexing/index.zig").SHM;
const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;

const server = @import("server/server.zig");

const contains = misc.contains;

fn bench(filename: []const u8) !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();

    var index_manager = try IndexManager.init(allocator);

    defer {
        index_manager.deinit(gpa.allocator()) catch {};
        _ = gpa.deinit();
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

    if (contains(filename, "hn")) {
        // try index_manager.addSearchCol("story_url");
        try index_manager.addSearchCol("story_text");
        // try index_manager.addSearchCol("story_author");
        try index_manager.addSearchCol("comment_text");
        // try index_manager.addSearchCol("comment_author");

    } else if (contains(filename, "enwik")) {
        try index_manager.addSearchCol("text");

    } else if (contains(filename, "mb")) {
        try index_manager.addSearchCol("title");
        try index_manager.addSearchCol("artist");
        try index_manager.addSearchCol("album");

    } else {
        @panic("File not supported yet.");
    }

    try index_manager.indexFile();

    if (contains(filename, "hn")) {
        try index_manager.addQueryField(
            "STORY_TEXT",
            "zig",
            2.0,
        );
        try index_manager.addQueryField(
            "COMMENT_TEXT",
            "gotta go fast",
            1.0,
        );

    } else if (contains(filename, "enwik")) {
        try index_manager.addQueryField(
            "TEXT",
            "griffith observatory",
            1.0,
        );

    } else if (contains(filename, "mb")) {
        try index_manager.addQueryField(
            "TITLE",
            "UNDER MY SKIN",
            2.0,
        );
        try index_manager.addQueryField(
            "ARTIST",
            "FRANK SINATRA",
            1.0,
        );
        try index_manager.addQueryField(
            "ALBUM",
            "LIGHTNING",
            1.0,
        );

    } else {
        @panic("File not supported yet.");
    }

    const num_queries: usize = 10_000;

    const start_time = std.time.microTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(10);

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
        index_manager.deinit(gpa.allocator()) catch {};
        _ = gpa.deinit();
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

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    if (contains(filename, "hn")) {
        // try index_manager.addSearchCol("story_url");
        try index_manager.addSearchCol("story_text");
        // try index_manager.addSearchCol("story_author");
        try index_manager.addSearchCol("comment_text");
        // try index_manager.addSearchCol("comment_author");

        try boost_factors.append(2.0);
        try boost_factors.append(1.0);

    } else if (contains(filename, "enwik")) {
        try index_manager.addSearchCol("text");

        try boost_factors.append(2.0);

    } else if (contains(filename, "mb")) {
        try index_manager.addSearchCol("title");
        try index_manager.addSearchCol("artist");
        try index_manager.addSearchCol("album");

        try boost_factors.append(2.0);
        try boost_factors.append(1.0);
        try boost_factors.append(1.0);

    } else {
        @panic("File not supported yet.");
    }

    try index_manager.indexFile();

    var server_handler = try server.QueryHandlerZap.init(
        &index_manager,
        boost_factors,
        );
    defer server_handler.deinit();

    try server_handler.serve();
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len > 2) {
        std.debug.print("Usage: {s} <filename>\n", .{args[0]});

        for (args) |arg| {
            std.debug.print("Arg: {s}\n", .{arg});
        }
        return error.InvalidArguments;
    } else if (args.len == 2) {
        const filename = args[1];
        try serveHTML(filename);
    } else {
        // const filename = "../data/mb_tiny.csv";
        // const filename = "../data/mb_small.csv";
        const filename = "../data/mb.csv";
        // const filename = "../data/enwiki.csv";
        // const filename = "../data/enwiki_small.csv";
        // const filename = "../data/mb.parquet";
        // const filename = "../data/hn.csv";
        // const filename = "../data/hn_half.csv";
        try bench(filename);
    }
}
