const std = @import("std");

const idx = @import("indexing/index.zig");

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
            .max_score = 0.0,
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
