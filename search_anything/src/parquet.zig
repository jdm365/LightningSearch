const std = @import("std");
const lp = @cImport({
    @cInclude("../lib/parquet_bindings.h");
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



test "read_parquet_col" {
    const filename = "../data/mb.parquet";
    var num_values: usize = 0;

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    // const data = readParquetRowGroupColumnUtf8NullTerminated(
    _ = readParquetRowGroupColumnUtf8NullTerminated(
        gpa.allocator(),
        filename,
        0,
        0,
        &num_values,
    );
    std.debug.print("Num values: {d}\n", .{num_values});
}
