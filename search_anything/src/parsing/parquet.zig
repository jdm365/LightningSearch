const std = @import("std");

const string_utils = @import("../utils/string_utils.zig");

const lp = @cImport({
    @cInclude("parquet_bindings.h");
});


pub fn readRowGroup(filename_c: [*:0]const u8, row_group_idx: usize) []u8 {
    var num_bytes: usize = undefined;
    const buffer = lp.convert_row_group_to_vbyte_buffer_c(
        filename_c,
        row_group_idx,
        &num_bytes,
    );

    return std.mem.bytesAsSlice(u8, buffer[0..num_bytes]);
}

pub fn freeRowGroup(buffer: []u8) void {
    lp.free_csv_buffer_c(buffer.ptr);
}

pub fn getSerializedReader(filename_c: [*:0]const u8) *anyopaque {
    return @ptrCast(lp.create_parquet_reader(filename_c));
}

pub fn readParquetRowGroupColumnUtf8NullTerminated(
    filename_c: [*:0]const u8,
    row_group_idx: usize,
    col_idx: usize,
    num_values: *usize,
) [*]u8 {
    return lp.read_parquet_row_group_column_utf8_null_terminated_c(
        filename_c,
        row_group_idx,
        col_idx,
        num_values,
    );
}

pub fn readParquetRowGroupColumnUtf8Vbyte(
    filename_c: [*:0]const u8,
    row_group_idx: usize,
    col_idx: usize,
    num_values: *usize,
) [*]u8 {

    return lp.read_parquet_row_group_column_utf8_vbyte_c(
        filename_c,
        row_group_idx,
        col_idx,
        num_values,
    );
}

pub inline fn decodeVbyte(buffer: [*]u8, idx: *usize) u64 {
    var value: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        const byte = buffer[idx.*];
        value |= @as(u64, @intCast(byte & 0b01111111)) << shift;
        idx.* += 1;
        if (byte < 128) break;
        shift += 7;
    }
    return value;
}

pub fn getParquetCols(
    allocator: std.mem.Allocator,
    cols: *std.ArrayListUnmanaged([]const u8),
    filename_c: [*:0]const u8,
) !void {
    var col_buffer: [8192]u8 = undefined;
    lp.get_col_names_c(filename_c, @ptrCast(&col_buffer));

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
            try allocator.dupe(u8, col_buffer[idx..idx+length]),
            );
        idx += length + 1;
    }
}

pub fn getNumRowGroupsParquet(
    filename_c: [*:0]const u8,
) usize {
    return lp.get_num_row_groups_c(filename_c);
}

pub fn getNumRowsParquet(
    filename_c: [*:0]const u8,
) usize {
    return lp.get_num_rows_c(filename_c);
}

pub fn getNumRowGroupsInRowGroup(
    // filename_c: [*:0]const u8,
    serialized_reader: *anyopaque,
    row_group_idx: usize,
) usize {
    return lp.get_num_rows_in_row_group_c(
        // filename_c,
        @ptrCast(serialized_reader),
        row_group_idx,
    );
}

pub fn fetchRowFromRowGroup(
    serialized_reader: *anyopaque,
    row_group_idx: usize,
    row_idx: usize,
    values_ptr: [*]u8,
    result_positions_ptr: [*]u64,
) void {
    return lp.fetch_row_from_row_group_c(
        @ptrCast(serialized_reader),
        row_group_idx,
        row_idx,
        values_ptr,
        @ptrCast(result_positions_ptr),
    );
}


test "read_parquet_col" {
    const filename = "../data/mb.parquet";

    var num_values: usize = 0;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);

    var cols = std.ArrayListUnmanaged([]const u8){};

    defer {
        cols.deinit(arena.allocator());
        arena.deinit();
    }

    try getParquetCols(arena.allocator(), &cols, filename);
    for (0.., cols.items) |idx, col| {
        std.debug.print("Col: {d} - {s}\n", .{idx, col});
    }
    std.debug.print("Num row groups: {d}\n", .{getNumRowGroupsParquet(filename)});

    var data = readParquetRowGroupColumnUtf8NullTerminated(
        filename,
        0,
        8,
        &num_values,
    );
    // std.debug.print("Num values: {d}\n", .{num_values});
    // std.debug.print("Data: {s}\n", .{data[0..64]});

    data = readParquetRowGroupColumnUtf8NullTerminated(
        filename,
        0,
        8,
        &num_values,
    );
    lp.free_vec(data, num_values);


    // TODO: Test row fetch.
}
