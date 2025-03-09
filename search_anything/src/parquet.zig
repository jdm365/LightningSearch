const std = @import("std");
const string_utils = @import("string_utils.zig");
const lp = @cImport({
    @cInclude("/home/jakemehlman/SearchAnything/search_anything/lib/parquet_bindings.h");
});




pub fn readParquetRowGroupColumnUtf8NullTerminated(
    allocator: std.mem.Allocator,
    filename: []const u8,
    row_group_idx: usize,
    col_idx: usize,
    num_values: *usize,
) ![*]u8 {
    const c_filename = try allocator.dupeZ(u8, filename);
    defer allocator.free(c_filename);

    return lp.read_parquet_row_group_column_utf8_null_terminated_c(
        c_filename,
        row_group_idx,
        col_idx,
        num_values,
    );
}

pub fn readParquetRowGroupColumnUtf8Vbyte(
    allocator: std.mem.Allocator,
    filename: []const u8,
    row_group_idx: usize,
    col_idx: usize,
    num_values: *usize,
) ![*]u8 {
    const c_filename = try allocator.dupeZ(u8, filename);
    defer allocator.free(c_filename);

    return lp.read_parquet_row_group_column_utf8_vbyte_c(
        c_filename,
        row_group_idx,
        col_idx,
        num_values,
    );
}

pub inline fn decodeVbyte(buffer: [*]u8, idx: *usize) u64 {
    var value: u64 = 0;
    var shift: usize = 0;
    while (true) {
        const byte = buffer[idx.*];
        value |= @as(u64, @intCast(byte & 0b01111111)) << shift;
        if (byte < 128) break;
        shift += 7;
        idx.* += 1;
    }
    return value;
}

pub fn getParuqetCols(
    allocator: std.mem.Allocator,
    cols: *std.ArrayListUnmanaged([]const u8),
    filename: []const u8,
) !void {
    const c_filename = try allocator.dupeZ(u8, filename);
    defer allocator.free(c_filename);

    var col_buffer: [8192]u8 = undefined;
    lp.get_col_names_c(c_filename, @ptrCast(&col_buffer));

    var idx: usize = 0;
    while (true) {
        if (col_buffer[idx] == 0) break;

        const length = string_utils.simdFindCharIdxEscapedFull(
            col_buffer[idx..],
            0,
        );
        string_utils.stringToUpper(
            col_buffer[idx..].ptr,
            length,
        );
        try cols.append(
            allocator,
            col_buffer[idx..idx+length],
            );
        idx += length + 1;
    }
}

pub fn getNumRowGroupsParuqet(
    allocator: std.mem.Allocator,
    filename: []const u8,
) !usize {
    const c_filename = try allocator.dupeZ(u8, filename);
    defer allocator.free(c_filename);
    return lp.get_num_row_groups_c(c_filename);
}

pub fn getNumRowsParuqet(
    allocator: std.mem.Allocator,
    filename: []const u8,
) !usize {
    const c_filename = try allocator.dupeZ(u8, filename);
    defer allocator.free(c_filename);
    return lp.get_num_rows_c(c_filename);
}

test "read_parquet_col" {
    const filename = "../data/mb.parquet";

    var num_values: usize = 0;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};

    var cols = std.ArrayListUnmanaged([]const u8){};

    defer {
        cols.deinit(gpa.allocator());
        _ = gpa.deinit();
    }

    try getParuqetCols(gpa.allocator(), &cols, filename);
    for (0.., cols.items) |idx, col| {
        std.debug.print("Col: {d} - {s}\n", .{idx, col});
    }
    std.debug.print("Num row groups: {d}\n", .{try getNumRowGroupsParuqet(gpa.allocator(), filename)});

    var data = try readParquetRowGroupColumnUtf8NullTerminated(
        gpa.allocator(),
        filename,
        0,
        8,
        &num_values,
    );
    // std.debug.print("Num values: {d}\n", .{num_values});
    // std.debug.print("Data: {s}\n", .{data[0..64]});

    data = try readParquetRowGroupColumnUtf8NullTerminated(
        gpa.allocator(),
        filename,
        0,
        8,
        &num_values,
    );
    lp.free_vec(data, num_values);
}
