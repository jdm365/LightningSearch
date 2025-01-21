//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const IndexManager = @import("main.zig").IndexManager;

pub const progress = @import("progress.zig");
pub const sorted_array = @import("sorted_array.zig");
pub const server = @import("server.zig");


test "build_index" {
    const filename: []const u8 = "../tests/mb_small.csv";

    const allocator = std.testing.allocator;

    var search_cols = std.ArrayList([]const u8).init(allocator);
    try search_cols.append("TITLE");
    try search_cols.append("ARTIST");

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    defer {
        search_cols.deinit();
        index_manager.deinit() catch {};
    }

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "FRANK SINATRA");
    try query_map.put("ARTIST", "FRANK SINATRA");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(2.0);
    try boost_factors.append(1.0);

    try index_manager.query(
        query_map,
        10,
        boost_factors,
        );

    for (index_manager.result_positions[0]) |pos| {
        const start_byte = pos.start_pos;
        const end_byte   = start_byte + pos.field_len;
        std.debug.print("{s},", .{index_manager.result_strings[0].items[start_byte..end_byte]});
    }
}
