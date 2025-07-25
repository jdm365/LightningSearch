const std = @import("std");

const SHM = @import("indexing/index.zig").SHM;
const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;
const TermPos = @import("server/server.zig").TermPos;
const csvLineToJsonScore = @import("server/server.zig").csvLineToJsonScore;


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

pub export fn c_query(
    idx_ptr: *IndexManager, 
    search_col_idxs: [*]u32,
    queries: [*][*:0]const u8,
    boost_factors: [*]f32,
    num_query_cols: u32,
    k: u32,

    num_matched_records: *u32,
    result_json_str_buf: *[*]u8,
    result_json_str_buf_size: *u64,
    ) void {
    idx_ptr.query_state.json_objects.clearRetainingCapacity();
    idx_ptr.query_state.json_output_buffer.clearRetainingCapacity();

    for (0..num_query_cols) |idx| {
        idx_ptr.addQueryFieldIdx(
            search_col_idxs[idx],
            std.mem.span(queries[idx]),
            boost_factors[idx],
            ) catch |err| {
            std.debug.print("Error: {any}\n", .{err});
            @panic("Failed to add query field index.");
        };
    }

    idx_ptr.query(k) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to execute query.");
    };
    num_matched_records.* = @truncate(idx_ptr.query_state.results_arrays[0].count);

    if (num_matched_records.* == 0) {
        return;
    }

    for (0..idx_ptr.query_state.results_arrays[0].count) |idx| {
        idx_ptr.query_state.json_objects.append(
            idx_ptr.stringArena(),
            csvLineToJsonScore(
                idx_ptr.stringArena(),
                idx_ptr.query_state.result_strings[idx],
                idx_ptr.query_state.result_positions[idx],
                idx_ptr.file_data.column_names,
                idx_ptr.query_state.results_arrays[0].scores[idx],
                idx,
            ) catch |err| {
                std.debug.print("Error: {any}\n", .{err});
                @panic("Failed to convert CSV line to JSON score.");
            },
        ) catch |err| {
            std.debug.print("Error: {any}\n", .{err});
            @panic("Failed to append JSON object.");
        };
    }

    var response = std.json.Value{
        .object = std.StringArrayHashMap(std.json.Value).init(
            idx_ptr.stringArena()
            ),
    };

    response.object.put(
        "results",
        std.json.Value{ 
            .array = idx_ptr.query_state.json_objects.toManaged(
                idx_ptr.stringArena()
                ) 
        },
    ) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to put results in JSON object.");
    };

    std.json.stringify(
        response,
        .{},
        idx_ptr.query_state.json_output_buffer.writer(idx_ptr.stringArena()),
    ) catch |err| {
        std.debug.print("Error: {any}\n", .{err});
        @panic("Failed to stringify JSON response.");
    };

    result_json_str_buf.* = idx_ptr.query_state.json_output_buffer.items.ptr;
    result_json_str_buf_size.* = idx_ptr.query_state.json_output_buffer.items.len;
}

pub export fn get_num_cols(idx_ptr: *IndexManager) u32 {
    return @truncate(idx_ptr.file_data.column_idx_map.num_keys);
}

pub export fn load(
    idx_ptr: *IndexManager, 
    _dir: [*:0]const u8
    ) void {
    idx_ptr.load(std.mem.span(_dir)) catch |err| {
        std.debug.print("Failed to load index from {s}: {any}\n", .{std.mem.span(_dir), err});
        @panic("Failed to load index.");
    };
}
