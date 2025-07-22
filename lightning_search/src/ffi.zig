const std    = @import("std");

const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;


pub export fn create_index() ?*anyopaque {
    const idx_ptr = std.heap.c_allocator.create(IndexManager) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to allocate memory for base `IndexManager`");
    };

    idx_ptr.* = IndexManager.init(std.heap.c_allocator) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to initialize `IndexManager`");
    };

    return idx_ptr;
}


pub export fn destroy_index(idx_ptr: *IndexManager) void {
    idx_ptr.deinit(std.heap.c_allocator) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to deinit `IndexManager`");
    };
    std.heap.c_allocator.destroy(idx_ptr);
}


pub export fn index_file(
    idx_ptr: *IndexManager, 
    _filename: [*:0]const u8,
    query_cols: [*][*:0]const u8,
    num_query_cols: u32,
    ) void {
    const filename = std.mem.span(_filename);

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

    idx_ptr.readHeader(filename, filetype) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to read file header.");
    };
    idx_ptr.scanFile() catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to scan file.");
    };

    for (0..num_query_cols) |col_idx| {
        const col = std.mem.span(query_cols[col_idx]);

        idx_ptr.addSearchCol(col) catch |err| {
            std.debug.print("Error: {any}\n", .{err});
            @panic("Failed to add search column.");
        };
    }

    idx_ptr.indexFile() catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to index file.");
    };
}
