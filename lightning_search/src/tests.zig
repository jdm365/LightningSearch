const std = @import("std");

const SHM = @import("indexing/index.zig").SHM;
const idx = @import("indexing/index.zig");
const IndexManager = @import("indexing/index_manager.zig").IndexManager;
const FileType = @import("storage/file_utils.zig").FileType;

test "PostingFullBlock compression/decompression" {
    const allocator = std.heap.page_allocator;

    var sorted_vals: [idx.BLOCK_SIZE]u32 align(512) = .{
        0, 2, 5, 8, 12, 16, 20, 24,
        30, 35, 40, 45, 50, 55, 60, 65,

        70, 75, 80, 85, 90, 95, 100, 105,
        110, 115, 120, 125, 130, 135, 140, 145,

        150, 155, 160, 165, 170, 175, 180, 185,
        190, 195, 200, 205, 210, 215, 220, 225,

        230, 235, 240, 245, 250, 255, 260, 265,
        270, 275, 280, 285, 290, 295, 300, 305,
    };
    var sorted_vals_cpy: [idx.BLOCK_SIZE]u32 align(512) = undefined;
    @memcpy(
        sorted_vals_cpy[0..idx.BLOCK_SIZE],
        sorted_vals[0..idx.BLOCK_SIZE],
    );


    var scratch_arr: [idx.BLOCK_SIZE]u32 align(512) = undefined;
    @memcpy(
        scratch_arr[0..idx.BLOCK_SIZE],
        sorted_vals[0..idx.BLOCK_SIZE],
    );

    var tf_arr: [idx.BLOCK_SIZE]u16 align(512) = .{
        1, 1, 1, 1, 2, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,

        1, 1, 1, 1, 1, 4, 1, 1,
        1, 1, 1, 1, 1, 1, 2, 1,

        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,

        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 3,
    };
    var tf_arr_cpy: [idx.BLOCK_SIZE]u16 align(512) = undefined;
    @memcpy(
        tf_arr_cpy[0..idx.BLOCK_SIZE],
        tf_arr[0..idx.BLOCK_SIZE],
    );

    const fb = idx.PostingsBlockFull{
        .doc_ids = try idx.DeltaBitpackedBlock.build(
                allocator,
                &scratch_arr,
                &sorted_vals,
            ),
            .tfs = try idx.BitpackedBlock.build(
                allocator,
                &tf_arr,
            ),
            .max_doc_id = scratch_arr[idx.BLOCK_SIZE - 1],
        };
    defer allocator.free(fb.doc_ids.buffer);
    defer allocator.free(fb.tfs.buffer);

    @memset(sorted_vals[0..idx.BLOCK_SIZE], 0);
    @memset(tf_arr[0..idx.BLOCK_SIZE], 0);

    try fb.decompressToBuffers(
        &sorted_vals,
        &tf_arr,
    );

    for (0..idx.BLOCK_SIZE) |i| {
        try std.testing.expectEqual(sorted_vals[i], sorted_vals_cpy[i]);
        try std.testing.expectEqual(tf_arr[i], tf_arr_cpy[i]);
    }
}

test "PostingPartialBlock compression/decompression" {
    const allocator = std.heap.page_allocator;

    var pb = idx.PostingsBlockPartial.init();

    var sorted_vals: [idx.BLOCK_SIZE]u32 align(512) = .{
        2, 5, 8, 12, 16, 20, 24, 26,
        30, 35, 40, 45, 50, 55, 60, 65,

        70, 75, 80, 85, 90, 95, 100, 105,
        110, 115, 120, 125, 130, 135, 140, 145,

        150, 155, 160, 165, 170, 175, 180, 185,
        190, 195, 200, 205, 210, 215, 220, 225,

        230, 235, 240, 245, 250, 255, 260, 265,
        270, 275, 280, 285, 290, 295, 10000, 1058020,
    };
    var sorted_vals_cpy: [idx.BLOCK_SIZE]u32 align(512) = undefined;
    @memcpy(
        sorted_vals_cpy[0..idx.BLOCK_SIZE],
        sorted_vals[0..idx.BLOCK_SIZE],
    );


    var scratch_arr: [idx.BLOCK_SIZE]u32 align(512) = undefined;
    @memcpy(
        scratch_arr[0..idx.BLOCK_SIZE],
        sorted_vals[0..idx.BLOCK_SIZE],
    );

    var tf_arr: [idx.BLOCK_SIZE]u16 align(512) = .{
        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,

        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,

        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,

        1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1,
    };
    var tf_arr_cpy: [idx.BLOCK_SIZE]u16 align(512) = undefined;
    @memcpy(
        tf_arr_cpy[0..idx.BLOCK_SIZE],
        tf_arr[0..idx.BLOCK_SIZE],
    );

    for (0..idx.BLOCK_SIZE) |i| {
        _ = try pb.add(allocator, sorted_vals[i]);
    }
    defer pb.doc_ids.buffer.deinit(allocator);
    defer pb.tfs.buffer.deinit(allocator);

    @memset(sorted_vals[0..idx.BLOCK_SIZE], 0);
    @memset(tf_arr[0..idx.BLOCK_SIZE], 0);

    pb.decompressToBuffers(
        &sorted_vals,
        &tf_arr,
    );

    for (0..idx.BLOCK_SIZE) |i| {
        try std.testing.expectEqual(sorted_vals[i], sorted_vals_cpy[i]);
        try std.testing.expectEqual(tf_arr[i], tf_arr_cpy[i]);
    }
}

test "BM25 Rank Ordering" {
    var gpa = std.heap.DebugAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const filename = "../data/mb_tiny.csv";
    const queries_filename = "../data/bm25_results.bin";

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

    try index_manager.addSearchCol("title");

    try index_manager.indexFile();

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(1.0);

    var query_map = SHM.init(allocator);
    defer query_map.deinit();

    const queries_buffer = try std.fs.Dir.readFileAlloc(
        std.fs.cwd(),
        allocator,
        queries_filename,
        1 << 20,
    );
    defer allocator.free(queries_buffer);

    var queries = std.ArrayListUnmanaged([]const u8){};
    defer queries.deinit(allocator);

    var correct_doc_ids = std.ArrayListUnmanaged(u32){};
    defer correct_doc_ids.deinit(allocator);

    var i: usize = 0;
    var start_idx: usize = 0;
    loop: while (i < queries_buffer.len) {
        switch (queries_buffer[i]) {
            '\n' => {
                try queries.append(
                    allocator, 
                    queries_buffer[start_idx..i],
                    );
                i += 1;
                break :loop;
            },
            '|' => {
                try queries.append(
                    allocator, 
                    queries_buffer[start_idx..i],
                    );
                i += 1;
                start_idx = i;
            },
            else => {
                i += 1;
                continue;
            },
        }
    }

    while (i < queries_buffer.len) {
        const k = std.mem.readInt(
            u32,
            @ptrCast(@as([*]u8, @ptrCast(queries_buffer))[i..(i+4)]),
            std.builtin.Endian.little,
        );
        i += 4;

        for (0..k) |_| {
            try correct_doc_ids.append(
                allocator,
                std.mem.readInt(
                    u32,
                    @ptrCast(@as([*]u8, @ptrCast(queries_buffer))[i..(i+4)]),
                    std.builtin.Endian.little,
                ),
            );
            i += 4;
        }
    }

    for (0.., queries.items) |_i, q| {
        std.debug.print("Queries: {s}\n", .{q});
        std.debug.print("Doc IDs: {any}\n", .{correct_doc_ids.items[_i * 10..][0..10]});
    }

    for (0.., queries.items) |_i, q| {
        try query_map.put("TITLE", q);

        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );

        for (0..10, ) |_j| {
            const res = index_manager.query_state.results_arrays[0].items[_j];
            std.testing.expectEqual(res.doc_id, correct_doc_ids.items[_i * 10 + _j]) catch |err| {
                std.debug.print("Queries: {s}\n", .{q});
                std.debug.print("Match: {s}\n", .{index_manager.query_state.result_strings[_j].items});
                return err;
            };
        }
    }
}
