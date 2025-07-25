const std     = @import("std");
const builtin = @import("builtin");

const csv  = @import("../parsing/csv.zig");
const json = @import("../parsing/json.zig");
const pq   = @import("../parsing/parquet.zig");

const SortedIntMultiArray = @import("../utils/sorted_array.zig").SortedIntMultiArray;
const CaseInsensitiveWyhash = @import("../utils/custom_wyhash.zig").CaseInsensitiveWyhash;
const CaseInsensitiveWyhashContext = @import("../utils/custom_wyhash.zig").CaseInsensitiveWyhashContext;
const string_utils = @import("../utils/string_utils.zig");
const file_utils   = @import("../storage/file_utils.zig");

const findSorted       = @import("../utils/misc_utils.zig").findSorted;
const TermPos          = @import("../server/server.zig").TermPos;
const StaticIntegerSet = @import("../utils/static_integer_set.zig").StaticIntegerSet;
const PruningRadixTrie = @import("../utils/pruning_radix_trie.zig").PruningRadixTrie;
const RadixTrie        = @import("../utils/radix_trie.zig").RadixTrie;
const DocStore         = @import("../storage/doc_store.zig").DocStore;

pub const MAX_NUM_RESULTS = 1000;
pub const MAX_TERM_LENGTH = 256;
// pub const MAX_NUM_TERMS   = 16_384;
pub const MAX_NUM_TERMS   = 65_536;
pub const MAX_LINE_LENGTH = 1_048_576;


const POST_ALIGNMENT = 64;

const K1: f32 = 1.4;
const B:  f32 = 0.75;
const C1: f32 = K1 * (1 - B);

pub inline fn scoreBM25(
    idf: f32,
    term_freq: u32,
    doc_size: u16,
    avg_doc_size: f32,
) f32 {
    const ftf: f32 = @floatFromInt(term_freq);
    const fds: f32 = @floatFromInt(doc_size);

    const num   = ftf * (K1 + 1) * idf;
    const denom = (ftf + (K1 * (1 - B + (B * fds / avg_doc_size))));

    return num / denom;
}


pub inline fn scoreBM25Fast(
    term_freq: u16, 
    doc_size: u16,
    A: f32,
    C2: f32,
    ) f32 {
    const tf: f32 = @floatFromInt(term_freq);
    const d:  f32 = @floatFromInt(doc_size);

    const L = tf + C1 + C2 * d;
    const r = 1.0 / L;
    // r = r * (2.0 - L * r);
    return A * tf * r;
}

inline fn lowerBound(comptime T: type, slice: []const T, target: T) usize {
    var low: usize = 0;
    var high: usize = slice.len;

    while (low < high) {
        const mid = low + ((high - low) >> 1);
        if (slice[mid] < target) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

inline fn linearLowerBound(slice: []const u32, target: u32) usize {
    const new_value: @Vector(8, u32) align(4) = @splat(target);
    var existing_values = @as(
        *const align(4) @Vector(8, u32), 
        @alignCast(@ptrCast(slice.ptr)),
        );

    const simd_limit: usize = @divFloor(slice.len, 8) + 
                              8 * @as(usize, @intFromBool(slice.len % 8 != 0));
    var idx: usize = 0;
    while (idx < simd_limit) {
        const mask = new_value < existing_values.*;

        const set_idx = @ctz(@as(u8, @bitCast(mask)));
        if (set_idx != 8) {
            return @min(idx + set_idx, slice.len);
        }

        existing_values = @ptrFromInt(@intFromPtr(existing_values) + 32);
        idx += 8;
    }
    return slice.len;
}

inline fn linearLowerBoundSIMD(
    slice: *const align(64) [BLOCK_SIZE]u32, 
    start_idx: usize,
    target: u32,
    ) ?usize {
    // We have a guarantee that the current doc
    // is less than the target, so ids in prev loaded SIMD
    // buffer shouldn't pass.
    const adjusted_start_idx = std.mem.alignBackward(
        usize,
        start_idx,
        16,
    );
    const start_block_idx = adjusted_start_idx >> 4;

    const new_value: @Vector(16, u32) = @splat(target);
    var existing_values = @as(
        *const @Vector(16, u32), 
        @alignCast(@ptrCast(slice[adjusted_start_idx..])),
        );

    var block_idx = start_block_idx;
    while (block_idx < NUM_BLOCKS) {
        const mask = new_value <= existing_values.*;

        const set_idx = @ctz(@as(u16, @bitCast(mask)));
        if (set_idx != 16) {
            return (block_idx << 4) + set_idx;
        }

        existing_values = @ptrFromInt(
            @intFromPtr(existing_values) + @sizeOf(@Vector(16, u32))
            );
        block_idx += 1;
    }
    return null;
}

pub const IteratorHeap = struct {
    its: []PostingsIteratorV2,
    current_idx: usize,

    // WIP

    pub fn init(
        allocator: std.mem.Allocator,
        iterators_arr: []PostingsIteratorV2,
        ) !IteratorHeap {
        return IteratorHeap{
            .its = try allocator.alignedAlloc(
                PostingsIteratorV2, 
                .@"16",
                iterators_arr.len,
                ),
            .current_idx = 0,
        };
    }

    pub fn deinit(
        self: IteratorHeap,
        allocator: std.mem.Allocator,
        ) void {
        allocator.free(self.its);
    }

    pub inline fn pop(self: IteratorHeap) PostingsIteratorV2 {
        const init_idx = self.current_idx;
        self.current_idx += 1;
        return self.its[init_idx];
    }

    pub inline fn push(self: IteratorHeap, new_it: PostingsIteratorV2) void {
        std.debug.assert(self.current_idx != 0);

        self.its[self.current_idx] = new_it;
        self.current_idx -= 1;

        self.heapify();
    }

    pub inline fn heapify(self: IteratorHeap) void {
        var idx = self.current_idx;
        while (idx < self.its.len) {
            const cur_it = self.its[idx];
            const parent_idx = (idx - 1) >> 1;

            if (parent_idx >= self.current_idx) {
                break;
            }

            const parent_it = self.its[parent_idx];
            if (cur_it.current_block_max_score > 
                parent_it.current_block_max_score) {

                self.its[idx] = parent_it;
                self.its[parent_idx] = cur_it;
                idx = parent_idx;
            } else {
                break;
            }
        }
    }
};

pub const PostingsIteratorV2 = struct {
    posting: PostingV3,
    posting_len: usize,
    uncompressed_doc_ids_buffer: [BLOCK_SIZE]u32 align(64),
    uncompressed_tfs_buffer: [BLOCK_SIZE]u16 align(64),
    current_doc_idx: usize,
    tp_idx: usize,
    boost_weighted_idf: f32,
    boost: f32,

    current_block_max_score: f32,
    A: f32,
    C2: f32,

    term_id: u32,
    col_idx: u32,
    avg_doc_size: f32,
    consumed: bool,
    on_partial_block: bool,

    pub const Result = packed struct(u64) {
        doc_id:    u32,
        term_freq: u16,
        term_pos:  u16,
    };

    pub fn init(
        posting: PostingV3,
        term_id: u32,
        col_idx: u32,
        idf: f32,
        boost: f32,
        avg_doc_size: f32,
        ) !PostingsIteratorV2 {
        const posting_len = (BLOCK_SIZE * posting.full_blocks.items.len) + 
                             posting.partial_block.num_docs;
        var it = PostingsIteratorV2{ 
            .posting = posting,
            .posting_len = posting_len,
            .uncompressed_doc_ids_buffer =  [_]u32{0} ** BLOCK_SIZE,
            .uncompressed_tfs_buffer = [_]u16{0} ** BLOCK_SIZE,
            .current_doc_idx = 0,
            .tp_idx = undefined,
            .boost_weighted_idf = idf * boost,
            .boost = boost,

            .current_block_max_score = 0.0,
            .A = (K1 + 1) * idf * boost,
            .C2 = (K1 * B) / avg_doc_size,

            .term_id = term_id,
            .col_idx = col_idx,
            .avg_doc_size = avg_doc_size,
            .consumed = false,
            .on_partial_block = posting_len < BLOCK_SIZE,
        };

        if (it.on_partial_block) {
            // Decompress partial
            it.posting.partial_block.decompressToBuffers(
                &it.uncompressed_doc_ids_buffer,
                &it.uncompressed_tfs_buffer,
                );
            it.current_block_max_score = scoreBM25Fast(
                it.posting.partial_block.max_tf,
                it.posting.partial_block.max_doc_size,
                it.A,
                it.C2,
            );
        } else {
            // Decompress full
            try it.posting.full_blocks.items[0].decompressToBuffers(
                &it.uncompressed_doc_ids_buffer,
                &it.uncompressed_tfs_buffer,
                );
            it.current_block_max_score = scoreBM25Fast(
                it.posting.full_blocks.items[0].max_tf,
                it.posting.full_blocks.items[0].max_doc_size,
                it.A,
                it.C2,
            );
        }

        return it;
    }

    pub inline fn currentDocId(self: *const PostingsIteratorV2) ?u32 {
        if (self.consumed) {
            return null;
        }
        return self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE];
    }

    // pub inline fn currentTermPos(self: *const PostingsIteratorV2) ?u16 {
        // if (self.current_idx >= self.term_positions.len) {
            // return null;
        // }
// 
        // return self.term_positions[self.current_idx];
    // }

    pub inline fn next(self: *PostingsIteratorV2) !?Result {
        if (self.consumed) {
            return null;
        }

        const current_res = Result{
            .doc_id    = self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_freq = self.uncompressed_tfs_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_pos  = undefined,
        };

        self.current_doc_idx += 1;

        if (self.current_doc_idx >= self.posting_len) {
            self.consumed = true;
            self.current_doc_idx = std.math.maxInt(u32);
            return null;
        }


        if (self.current_doc_idx % BLOCK_SIZE == 0) {
            const block_idx = self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE));
            if (block_idx == self.posting.full_blocks.items.len) {
                self.on_partial_block = true;
                self.posting.partial_block.decompressToBuffers(
                    &self.uncompressed_doc_ids_buffer,
                    &self.uncompressed_tfs_buffer,
                    );
            } else {
                try self.posting.full_blocks.items[block_idx].decompressToBuffers(
                    &self.uncompressed_doc_ids_buffer,
                    &self.uncompressed_tfs_buffer,
                );
                // std.debug.print(
                    // "BLOCK IDX: {d} | DOC IDS: {d}\n",
                    // .{block_idx, self.uncompressed_doc_ids_buffer},
                // );
            }

            if (self.on_partial_block) {
                self.current_block_max_score = scoreBM25Fast(
                        self.posting.partial_block.max_tf,
                        self.posting.partial_block.max_doc_size,
                        self.A,
                        self.C2,
                    );
            } else {
                self.current_block_max_score = scoreBM25Fast(
                        self.posting.full_blocks.items[block_idx].max_tf,
                        self.posting.full_blocks.items[block_idx].max_doc_size,
                        self.A,
                        self.C2,
                    );
            }
        }
        // std.debug.print("Current doc id: {d}\n", .{self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE]});

        return current_res;
    }

    pub inline fn nextSkipping(self: *PostingsIteratorV2, min_score: f32) !?Result {
        if (self.consumed) {
            return null;
        }

        var skipped = false;
        while (self.currentBlockMaxScore() <= min_score) {
            if (self.on_partial_block) {
                self.consumed = true;
                self.current_doc_idx = std.math.maxInt(u32);
                return null;
            }

            skipped = true;

            self.current_doc_idx = std.mem.alignBackward(
                usize,
                self.current_doc_idx + BLOCK_SIZE,
                64,
            );
            const block_idx = self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE));
            if (block_idx == self.posting.full_blocks.items.len) {
                self.on_partial_block = true;
            }
            continue;
        }

        const prev_res = Result{
            .doc_id    = self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_freq = self.uncompressed_tfs_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_pos  = undefined,
        };


        if (skipped) {
            if (self.on_partial_block) {
                self.posting.partial_block.decompressToBuffers(
                    &self.uncompressed_doc_ids_buffer,
                    &self.uncompressed_tfs_buffer,
                    );
            } else {
                const block_idx = @divFloor(self.current_doc_idx, BLOCK_SIZE);
                try self.posting.full_blocks.items[block_idx].decompressToBuffers(
                    &self.uncompressed_doc_ids_buffer,
                    &self.uncompressed_tfs_buffer,
                );
            }
        } else {
            self.current_doc_idx += 1;

            const block_idx = @divFloor(self.current_doc_idx, BLOCK_SIZE);
            if (self.current_doc_idx % BLOCK_SIZE == 0) {
                self.on_partial_block = (block_idx == self.posting.full_blocks.items.len);
                if (self.on_partial_block) {
                    self.posting.partial_block.decompressToBuffers(
                        &self.uncompressed_doc_ids_buffer,
                        &self.uncompressed_tfs_buffer,
                        );
                } else {
                    try self.posting.full_blocks.items[block_idx].decompressToBuffers(
                        &self.uncompressed_doc_ids_buffer,
                        &self.uncompressed_tfs_buffer,
                    );
                }
            }
        }


        if (self.current_doc_idx >= self.posting_len) {
            self.consumed = true;
            self.current_doc_idx = std.math.maxInt(u32);
            return null;
        }

        return prev_res;
    }

    pub inline fn advanceTo(self: *PostingsIteratorV2, target_id: u32) !?Result {
        if (self.consumed) {
            return null;
        }

        var idx = self.current_doc_idx % BLOCK_SIZE;
        if (self.uncompressed_doc_ids_buffer[idx] >= target_id) {
            return .{
                .doc_id    = self.uncompressed_doc_ids_buffer[idx],
                .term_freq = self.uncompressed_tfs_buffer[idx],
                .term_pos  = undefined,
            };
        }

        if (!self.on_partial_block) {
            const num_full_blocks = self.posting.full_blocks.items.len;

            std.debug.assert(num_full_blocks > 0);

            var block_idx = self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE));
            var skipped_block = false;
            while (block_idx < num_full_blocks) {

                if (target_id > self.posting.full_blocks.items[block_idx].max_doc_id) {
                    idx = 0;
                    block_idx += 1;
                    self.current_doc_idx = std.mem.alignForward(
                        usize,
                        self.current_doc_idx + 1,
                        BLOCK_SIZE,
                    );
                    skipped_block = true;
                    continue;
                }

                // Doc id in full block if here.
                if (skipped_block) {
                    try self.posting.full_blocks.items[block_idx].decompressToBuffers(
                        &self.uncompressed_doc_ids_buffer,
                        &self.uncompressed_tfs_buffer,
                        );
                }

                if (linearLowerBoundSIMD(
                    &self.uncompressed_doc_ids_buffer,
                    idx,
                    target_id,
                )) |matched_idx| {

                    self.current_doc_idx = std.mem.alignBackward(
                        usize,
                        self.current_doc_idx,
                        BLOCK_SIZE,
                    ) + matched_idx;

                    self.current_block_max_score = scoreBM25Fast(
                            self.posting.full_blocks.items[block_idx].max_tf,
                            self.posting.full_blocks.items[block_idx].max_doc_size,
                            self.A,
                            self.C2,
                        );
                    return .{
                        .doc_id = self.uncompressed_doc_ids_buffer[matched_idx],
                        .term_freq = self.uncompressed_tfs_buffer[matched_idx],
                        .term_pos = undefined,
                    };
                }

                unreachable;
            }

            // if (skipped_block) {
                // self.current_doc_idx = std.mem.alignBackward(
                    // usize,
                    // self.current_doc_idx + BLOCK_SIZE,
                    // 64,
                // );
            // }
            self.posting.partial_block.decompressToBuffers(
                &self.uncompressed_doc_ids_buffer,
                &self.uncompressed_tfs_buffer,
                );
            self.on_partial_block = true;
        }

        const match_idx = linearLowerBound(
            self.uncompressed_doc_ids_buffer[idx..self.posting.partial_block.num_docs],
            target_id,
        );
        self.current_doc_idx += match_idx;

        if (self.current_doc_idx >= self.posting_len) {
            self.consumed = true;
            self.current_doc_idx = std.math.maxInt(u32);
            return null;
        }

        self.current_block_max_score = scoreBM25Fast(
                self.posting.partial_block.max_tf,
                self.posting.partial_block.max_doc_size,
                self.A,
                self.C2,
            );

        idx = self.current_doc_idx % BLOCK_SIZE;
        return .{
            .doc_id    = self.uncompressed_doc_ids_buffer[idx],
            .term_freq = self.uncompressed_tfs_buffer[idx],
            .term_pos  = undefined,
        };
    }

    pub inline fn currentBlockMaxScore(self: *const PostingsIteratorV2) f32 {
        if (self.on_partial_block) {
            return scoreBM25(
                self.boost_weighted_idf,
                @intCast(self.posting.partial_block.max_tf),
                self.posting.partial_block.max_doc_size,
                self.avg_doc_size,
            );
        }
        const fb = self.posting.full_blocks.items[
            self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE))
        ];
        return scoreBM25(
            self.boost_weighted_idf,
            @intCast(fb.max_tf),
            fb.max_doc_size,
            self.avg_doc_size,
        );
    }
};

pub const QueryResult = packed struct(u64) {
    doc_id: u32,
    partition_idx: u32,
};

pub const SHM = std.HashMap([]const u8, []const u8, CaseInsensitiveWyhashContext, 80);

const IndexContext = struct {
    string_bytes: *const std.ArrayListUnmanaged(u8),

    pub fn eql(_: IndexContext, a: u32, b: u32) bool {
        return a == b;
    }

    pub fn hash(self: IndexContext, x: u32) u64 {
        const x_slice = std.mem.span(
            @as([*:0]u8, @ptrCast(&self.string_bytes.items[x]))
            );
        return CaseInsensitiveWyhash.hash(42, x_slice);
    }
};

const SliceAdapter = struct {
    string_bytes: *const std.ArrayListUnmanaged(u8),

    pub fn eql(self: SliceAdapter, a_slice: []u8, b: u32) bool {
        const b_slice = std.mem.span(
            @as([*:0]u8, @ptrCast(&self.string_bytes.items[b])),
            );

        if (a_slice.len != b_slice.len) return false;
        for (0..a_slice.len) |i| {
            if (std.ascii.toLower(a_slice[i]) != std.ascii.toLower(b_slice[i])) {
                return false;
            }
        }
        return true;
    }

    pub fn hash(_: SliceAdapter, adapted_key: []u8) u64 {
        return CaseInsensitiveWyhash.hash(42, adapted_key);
    }
};

const Vocab = struct {
    string_bytes: std.ArrayListUnmanaged(u8),
    map: std.hash_map.HashMapUnmanaged(u32, u32, IndexContext, 80),

    pub fn init() Vocab {
        return Vocab{
            .string_bytes = .{},
            .map = .{},
        };
    }

    pub fn deinit(self: *Vocab, allocator: std.mem.Allocator) void {
        self.string_bytes.deinit(allocator);
        self.map.deinit(allocator);
    }

    pub inline fn getCtx(self: *Vocab) IndexContext {
        return IndexContext{ .string_bytes = &self.string_bytes };
    }

    pub inline fn getAdapter(self: *Vocab) SliceAdapter {
        return SliceAdapter{ .string_bytes = &self.string_bytes };
    }

    pub inline fn get(self: *Vocab, key: []u8, adapter: SliceAdapter) ?u32 {
        return self.map.getAdapted(key, adapter);
    }
};

pub const BLOCK_SIZE: usize = 64;
pub const NUM_BLOCKS: usize = @divExact(BLOCK_SIZE, 16);

inline fn putBits(buf: []u8, bit_pos: usize, value: u32, lo_bits: u8) void {
    var v: u32 = value;
    var written: u8 = 0;

    while (written < lo_bits) {
        const byte_idx = (bit_pos + written) >> 3;
        const bit_idx  = (bit_pos + written) & 7;
        const chunk: u5 = @truncate(@min(8 - bit_idx, lo_bits - written));

        const mask: u32 = (@as(u32, 1) << chunk) - 1;
        const bits: u8  = @truncate((v & mask) << @truncate(bit_idx));

        buf[byte_idx] |= bits;
        v >>= chunk;
        written += chunk;
    }
}

inline fn getBits32(
    buf: []const u8,
    bit_pos: usize,
    width: usize,
) u32 {
    const byte_off = bit_pos >> 3;
    const lo_bits  = bit_pos & 7;

    // This will be aligned, but the compiler can't figure it out.
    const word_ptr: *align(1) u64 = @constCast(
        @alignCast(@ptrCast(buf.ptr + byte_off))
    );
    const word: u64 = word_ptr.*;

    const value = (word >> @truncate(lo_bits)) & ((@as(u64, 1) << @truncate(width)) - 1);
    return @as(u32, @truncate(value));
}

pub const DeltaBitpackedBlock = struct {
    min_val: u32,
    bit_size: u8,
    buffer: []u8 align(64),


    pub fn build(
        allocator: std.mem.Allocator,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
        sorted_vals: *align(64) [BLOCK_SIZE]u32,
        ) !DeltaBitpackedBlock {
        var dbp = DeltaBitpackedBlock{
            .min_val = sorted_vals[0],
            .buffer = undefined,
            .bit_size = undefined,
        };
        var block_maxes: @Vector(NUM_BLOCKS, u32) = @splat(0);

        const sv_vec: [*]@Vector(16, u32) = @ptrCast(sorted_vals);
        var scratch_vec: [*]@Vector(16, u32) = @ptrCast(scratch_arr);

        inline for (0..NUM_BLOCKS) |block_idx| {
            const start_idx = comptime block_idx * 16;
            const shift_in_idx = comptime if (block_idx == NUM_BLOCKS - 1) 
                BLOCK_SIZE - 1
             else 
                start_idx + 16;
            

            scratch_vec[block_idx] = std.simd.shiftElementsLeft(
                sv_vec[block_idx],
                1,
                sorted_vals[shift_in_idx],
            ) - sv_vec[block_idx];
            block_maxes[block_idx] = @reduce(.Max, scratch_vec[block_idx]);
        }

        const max_gap: u32 = @reduce(.Max, block_maxes);
        dbp.bit_size = 32 - @clz(max_gap);
        const buffer_size = ((BLOCK_SIZE * dbp.bit_size) + 7) >> 3;
        dbp.buffer = try allocator.alignedAlloc(
            u8,
            POST_ALIGNMENT,
            buffer_size,
            );
        @memset(dbp.buffer, 0);

        var bit_pos: usize = 0;
        inline for (0..BLOCK_SIZE) |idx| {
            const gap: u32  = scratch_arr[idx];

            putBits(dbp.buffer, bit_pos, gap, dbp.bit_size);
            bit_pos += dbp.bit_size;
        }

        scratch_arr[BLOCK_SIZE - 1] = sorted_vals[BLOCK_SIZE - 1];
        return dbp;
    }
};

pub const BitpackedBlock = struct {
    max_val: u32,
    bit_size: u8,
    buffer: []u8 align(64),

    pub fn build(
        allocator: std.mem.Allocator,
        vals: *align(64) [BLOCK_SIZE]u16,
        ) !BitpackedBlock {
        var dbp = BitpackedBlock{
            .max_val = undefined,
            .buffer = undefined,
            .bit_size = undefined,
        };
        var block_maxes: @Vector(NUM_BLOCKS, u16) = @splat(0);

        const sv_vec: [*]@Vector(16, u16) = @ptrCast(vals);
        inline for (0..NUM_BLOCKS) |block_idx| {
            block_maxes[block_idx] = @reduce(.Max, sv_vec[block_idx]);
        }

        dbp.max_val = @reduce(.Max, block_maxes);
        dbp.bit_size = 16 - @clz(@as(u16, @truncate(dbp.max_val)));
        const buffer_size = ((BLOCK_SIZE * dbp.bit_size) + 7) >> 3;

        dbp.buffer = try allocator.alignedAlloc(
            u8,
            POST_ALIGNMENT,
            buffer_size,
            );
        @memset(dbp.buffer, 0);

        var bit_pos: usize = 0;
        inline for (0..BLOCK_SIZE) |idx| {
            const val: u16  = vals[idx];

            putBits(dbp.buffer, bit_pos, val, dbp.bit_size);
            bit_pos += dbp.bit_size;
        }

        return dbp;
    }

};

pub const PostingsBlockFull = struct {
    doc_ids: DeltaBitpackedBlock,
    tfs:     BitpackedBlock,
    max_tf: u16,
    max_doc_size: u16,
    max_doc_id: u32,

    // TODO: Store `impacts` (scores) as u16.
    //       Keep small (~4) exception array of highest impact docs, [impact, doc_id]

    pub inline fn decompressToBuffers(
        self: *const PostingsBlockFull,
        doc_ids: *[BLOCK_SIZE]u32,
        tfs: *[BLOCK_SIZE]u16,
    ) !void {
        const d_bits: usize = self.doc_ids.bit_size;
        var bit_pos: usize = 0;

        var prev: u32 = self.doc_ids.min_val;
        doc_ids[0] = prev;

        if (d_bits == 0) {
            @branchHint(.cold);
            for (1..BLOCK_SIZE) |i| doc_ids[i] = prev;
        } else {
            for (1..BLOCK_SIZE) |i| {
                // @setEvalBranchQuota(1600);

                const gap: u32 = getBits32(self.doc_ids.buffer, bit_pos, d_bits);
                bit_pos += d_bits;

                const cur: u32 = prev + gap;
                doc_ids[i] = cur;
                prev = cur;
            }
        }

        const tf_bits: usize = self.tfs.bit_size;
        bit_pos = 0;

        if (tf_bits == 0) {
            @branchHint(.cold);
            @memset(tfs, 1);
        } else {
            inline for (0..BLOCK_SIZE) |i| {
                const v = getBits32(self.tfs.buffer, bit_pos, tf_bits);
                bit_pos += tf_bits;
                tfs[i] = @truncate(v);
            }
        }
    }
};


const DeltaVByteBlock = struct {
    // TODO: Consider making these page aligned.
    //       Reconsider alignment and allocations in these structs
    //       to try to minimize faults and prevent fragmentation.
    //
    //       Also consider using raw pointer with size as a smaller
    //       integer type.
    buffer: std.ArrayListAlignedUnmanaged(u8, POST_ALIGNMENT),

    pub fn init() DeltaVByteBlock {
        return DeltaVByteBlock{
            .buffer = std.ArrayListAlignedUnmanaged(u8, POST_ALIGNMENT){},
        };
    }

    pub fn build(
        allocator: std.mem.Allocator,
        sorted_vals: []u32,
        ) !DeltaVByteBlock {

        var bytes_needed: usize = 0;
        for (sorted_vals) |val| {
            const delta = if (val == 0) 0 else val - sorted_vals[0];
            bytes_needed += pq.getVbyteSize(@intCast(delta));
        }
        const dvb = DeltaVByteBlock{
            .buffer = try std.ArrayListAlignedUnmanaged(
                u8, 
                POST_ALIGNMENT,
                ).initCapacity(
                allocator,
                bytes_needed,
                ),
        };
        var buf_idx: usize = 0;
        for (sorted_vals) |val| {
            const delta = if (val == 0) 0 else val - sorted_vals[0];
            pq.encodeVbyte(
                dvb.buffer.items.ptr,
                &buf_idx,
                @intCast(delta),
            );
        }

        return dvb;
    }

    pub inline fn add(
        self: *DeltaVByteBlock,
        allocator: std.mem.Allocator,
        value: u32,
        prev_val: u32,
    ) !void {
        const bytes_needed = pq.getVbyteSize(value - prev_val);
        var init_ptr = self.buffer.items.len;
        try self.buffer.resize(
            allocator,
            init_ptr + bytes_needed
        );
        pq.encodeVbyte(
            self.buffer.items.ptr,
            &init_ptr,
            @intCast(value - prev_val),
        );
    }

    pub fn buildIntoDeltaBitpacked(
        self: *DeltaVByteBlock,
        allocator: std.mem.Allocator,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
        ) !DeltaBitpackedBlock {
        // Optimize later. For now, just convert to u32 buf, then call above func.

        // TODO: Thread local static arrays.
        var tmp_arr: [BLOCK_SIZE]u32 align(64) = undefined;

        var prev_val: u32 = 0;
        var byte_ptr: usize = 0;
        for (0..BLOCK_SIZE) |idx| {
            tmp_arr[idx] = @as(u32, @truncate(pq.decodeVbyte(
                self.buffer.items.ptr,
                &byte_ptr,
            ))) + prev_val;
            prev_val = tmp_arr[idx];
        }

        return try DeltaBitpackedBlock.build(
            allocator,
            scratch_arr,
            &tmp_arr,
        );
    }

    pub inline fn clear(self: *DeltaVByteBlock) void {
        self.buffer.clearRetainingCapacity();
    }
};

const VByteBlock = struct {
    buffer: std.ArrayListAlignedUnmanaged(
                u8, 
                POST_ALIGNMENT,
                ),

    pub fn init() VByteBlock {
        return VByteBlock{
            .buffer = std.ArrayListAlignedUnmanaged(
                u8, 
                POST_ALIGNMENT,
                ){},
        };
    }

    pub fn build(
        allocator: std.mem.Allocator,
        vals: [BLOCK_SIZE]u32,
        ) !VByteBlock {
        var bytes_needed: usize = 0;
        for (vals) |val| {
            bytes_needed += pq.getVbyteSize(@intCast(val));
        }
        const vb = VByteBlock{
            .buffer = try std.ArrayListAlignedUnmanaged(
                u8, 
                POST_ALIGNMENT,
                ).initCapacity(
                allocator,
                bytes_needed,
                ),
        };

        var buf_idx: usize = 0;
        for (vals) |val| {
            pq.encodeVbyte(
                vb.buffer.items.ptr,
                &buf_idx,
                @intCast(val),
            );
        }

        return vb;
    }

    pub inline fn add(
        self: *VByteBlock,
        allocator: std.mem.Allocator,
        value: u32,
    ) !void {
        const bytes_needed = pq.getVbyteSize(value);
        var init_ptr = self.buffer.items.len;
        try self.buffer.resize(
            allocator,
            init_ptr + bytes_needed
        );
        pq.encodeVbyte(
            self.buffer.items.ptr,
            &init_ptr,
            @intCast(value),
        );
    }

    pub fn buildIntoBitpacked(
        self: *VByteBlock,
        allocator: std.mem.Allocator,
        ) !BitpackedBlock {
        // Optimize later. For now, just convert to u32 buf, then call above func.

        // TODO: Thread local static arrays.
        var tmp_arr: [BLOCK_SIZE]u16 align(64) = undefined;

        var byte_ptr: usize = 0;
        for (0..BLOCK_SIZE) |idx| {
            tmp_arr[idx] = @truncate(pq.decodeVbyte(
                self.buffer.items.ptr,
                &byte_ptr,
            ));
        }

        return try BitpackedBlock.build(
            allocator,
            &tmp_arr,
        );
    }

    pub inline fn clear(self: *VByteBlock) void {
        self.buffer.clearRetainingCapacity();
    }
};

pub const PostingsBlockPartial = struct {
    doc_ids: DeltaVByteBlock,
    tfs:     VByteBlock,

    prev_doc_id: u32,
    max_tf: u16,
    max_doc_size: u16,
    prev_tf: u16,
    num_docs: u8,

    pub fn init() PostingsBlockPartial {
        return PostingsBlockPartial{
            .doc_ids = DeltaVByteBlock.init(),
            .tfs = VByteBlock.init(),

            .prev_doc_id = 0,
            .prev_tf = 1,
            .max_tf = 1,
            .max_doc_size = 1,
            .num_docs = 0,
        };
    }

    pub fn flush(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
    ) !PostingsBlockFull {
        const full_map = try self.partialToFull(allocator, scratch_arr);

        self.num_docs = 0;
        self.doc_ids.clear();
        self.tfs.clear();
        self.max_tf = 1;
        self.max_doc_size = 1;
        self.prev_doc_id = 0;
        self.prev_tf = 1;

        return full_map;
    }

    fn partialToFull(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
    ) !PostingsBlockFull {
        // TODO: CONSIDER MODIFYING STANDARD BM25 HERE.
        //       1. For short docs, don't use tf.
        //       2. For medium docs, cap tf score incluence.

        var pbf = PostingsBlockFull{
            .doc_ids = try self.doc_ids.buildIntoDeltaBitpacked(
                allocator,
                scratch_arr,
            ),
            .tfs = try self.tfs.buildIntoBitpacked(
                allocator,
            ),
            .max_tf = self.max_tf,
            .max_doc_size = self.max_doc_size,
            .max_doc_id = undefined,
        };
        pbf.max_doc_id = scratch_arr[BLOCK_SIZE - 1];

        return pbf;
    }

    pub inline fn add(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        doc_id: u32,
        doc_size: u16,
    ) !u32 {
        // Return 1 if first_occurence of doc_id, 0 otherwise.
        // Needed to increment df in indexing loop.

        // if ((doc_id != self.prev_doc_id) or (doc_id + self.num_docs == 0)) {
        if (doc_id != self.prev_doc_id or self.num_docs == 0) {

            // TODO: 
            // 1. Swith tf_cntr to tf_max.
            // 2. Implement vbyte addition.
            // 3. On first doc append new element to tfs/doc_ids.
            // 4. Increment tf on subsequent docs.

            try self.tfs.add(allocator, 1);

            try self.doc_ids.add(allocator, doc_id, self.prev_doc_id);
            self.num_docs += 1;
            self.prev_doc_id = doc_id;
            self.prev_tf = 1;
            return 1;

        } else {
            // TODO: Technincally when equal, doc size needs to be considered.
            const new_tf          = self.prev_tf + 1;
            const old_size        = pq.getVbyteSize(@intCast(self.prev_tf));
            const new_size        = pq.getVbyteSize(@intCast(new_tf));
            const start           = self.tfs.buffer.items.len - old_size;

            // remove the old encoding
            self.tfs.buffer.items.len -= old_size;

            // make room for the new one
            try self.tfs.buffer.resize(allocator, self.tfs.buffer.items.len + new_size);

            var write_ptr: usize = start;
            pq.encodeVbyte(self.tfs.buffer.items.ptr, &write_ptr, @intCast(new_tf));

            self.prev_tf = new_tf;
            self.prev_doc_id = doc_id;

            if (new_tf > self.max_tf) {
                self.max_tf       = new_tf;
                self.max_doc_size = doc_size;
            }
            return 0;
        }

    }

    pub inline fn decompressToBuffers(
        self: *const PostingsBlockPartial,
        doc_ids: *[BLOCK_SIZE]u32,
        tfs: *[BLOCK_SIZE]u16,
    ) void {

        var prev_doc_id: u32 = 0;
        var doc_idx: u64 = 0;
        var tf_idx:  u64 = 0;
        for (0..self.num_docs) |idx| {
            doc_ids[idx] = prev_doc_id + @as(u32, @truncate(pq.decodeVbyte(
                self.doc_ids.buffer.items.ptr,
                &doc_idx,
            )));
            tfs[idx] = @truncate(pq.decodeVbyte(
                self.tfs.buffer.items.ptr,
                &tf_idx,
            ));

            prev_doc_id = doc_ids[idx];
        }
    }
};

pub const PostingV3 = struct {
    full_blocks: std.ArrayListAlignedUnmanaged(
                     PostingsBlockFull,
                     POST_ALIGNMENT,
                 ),
    partial_block: PostingsBlockPartial,

    pub inline fn init() PostingV3 {
        return PostingV3{
            .full_blocks = std.ArrayListAlignedUnmanaged(
                PostingsBlockFull,
                POST_ALIGNMENT,
            ){},
            .partial_block = PostingsBlockPartial.init(),
        };
    }

    pub inline fn add(
        self: *PostingV3,
        allocator: std.mem.Allocator,
        doc_id: u32,
        doc_size: u16,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
    ) !u32 {
        const first_occurence = try self.partial_block.add(
            allocator,
            doc_id,
            doc_size,
        );

        if (self.partial_block.num_docs == BLOCK_SIZE) {
            const new_val = try self.full_blocks.addOne(allocator);
            new_val.* = try self.partial_block.flush(allocator, scratch_arr);
        }

        return first_occurence;
    }

    pub fn serialize(
        self: *PostingV3, 
        buffer: *std.ArrayListUnmanaged(u8),
        allocator: std.mem.Allocator,
        current_pos: *usize,
        ) !void {

        pq.encodeVbyte(
            buffer.items.ptr,
            current_pos,
            self.full_blocks.items.len,
        );
        for (self.full_blocks.items) |*block| {
            const max_range = current_pos.* + 
                (block.doc_ids.buffer.len + block.tfs.buffer.len) + 
                @sizeOf(u32) * 3 + 
                @sizeOf(u8) * 2;

            if (max_range > buffer.items.len) {
                @branchHint(.unlikely);
                try buffer.resize(allocator, 2 * max_range);
            }

            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                @intCast(block.max_doc_id),
            );
            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                @intCast(block.max_tf),
            );
            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                @intCast(block.max_doc_size),
            );
            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                @intCast(block.doc_ids.min_val),
            );

            buffer.items[current_pos.*] = block.doc_ids.bit_size;
            current_pos.* += 1;

            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                block.doc_ids.buffer.len,
            );
            @memcpy(
                buffer.items[current_pos.*..][0..block.doc_ids.buffer.len],
                block.doc_ids.buffer,
            );
            current_pos.* += block.doc_ids.buffer.len;

            buffer.items[current_pos.*] = block.tfs.bit_size;
            current_pos.* += 1;

            pq.encodeVbyte(
                buffer.items.ptr,
                current_pos,
                block.tfs.buffer.len,
            );
            @memcpy(
                buffer.items[current_pos.*..][0..block.tfs.buffer.len],
                block.tfs.buffer,
            );
            current_pos.* += block.tfs.buffer.len;
        }

        const max_range = current_pos.* + 1 + 4 +
            (self.partial_block.doc_ids.buffer.items.len + 
             self.partial_block.tfs.buffer.items.len);
        if (max_range > buffer.items.len) {
            @branchHint(.unlikely);
            try buffer.resize(allocator, 2 * max_range);
        }

        buffer.items[current_pos.*] = self.partial_block.num_docs;
        current_pos.* += 1;

        pq.encodeVbyte(
            buffer.items.ptr,
            current_pos,
            @intCast(self.partial_block.max_tf),
        );
        pq.encodeVbyte(
            buffer.items.ptr,
            current_pos,
            @intCast(self.partial_block.max_doc_size),
        );

        pq.encodeVbyte(
            buffer.items.ptr,
            current_pos,
            self.partial_block.doc_ids.buffer.items.len,
        );
        @memcpy(
            buffer.items[current_pos.*..][
                0..self.partial_block.doc_ids.buffer.items.len
            ],
            self.partial_block.doc_ids.buffer.items,
        );
        current_pos.* += self.partial_block.doc_ids.buffer.items.len;

        pq.encodeVbyte(
            buffer.items.ptr,
            current_pos,
            self.partial_block.tfs.buffer.items.len,
        );
        @memcpy(
            buffer.items[current_pos.*..][
                0..self.partial_block.tfs.buffer.items.len
            ],
            self.partial_block.tfs.buffer.items,
        );
        current_pos.* += self.partial_block.tfs.buffer.items.len;
    }

    pub fn deserialize(
        self: *PostingV3, 
        allocator: std.mem.Allocator,
        buffer: []u8,
        current_pos: *usize,
        ) !void {
        const num_full_blocks = pq.decodeVbyte(
            buffer.ptr,
            current_pos,
        );
        try self.full_blocks.resize(allocator, num_full_blocks);

        for (0..num_full_blocks) |idx| {
            var block = &self.full_blocks.items[idx];

            block.max_doc_id      = @truncate(pq.decodeVbyte(buffer, current_pos));
            block.max_tf          = @truncate(pq.decodeVbyte(buffer, current_pos));
            block.max_doc_size    = @truncate(pq.decodeVbyte(buffer, current_pos));
            block.doc_ids.min_val = @truncate(pq.decodeVbyte(buffer, current_pos));

            block.doc_ids.bit_size = buffer[current_pos.*];
            current_pos.* += 1;

            const doc_id_buf_len = pq.decodeVbyte(buffer, current_pos);
            block.doc_ids.buffer = try allocator.alignedAlloc(
                u8,
                POST_ALIGNMENT,
                doc_id_buf_len,
            );
            @memcpy(
                block.doc_ids.buffer[0..doc_id_buf_len],
                buffer[current_pos.*..][0..doc_id_buf_len],
            );
            current_pos.* += doc_id_buf_len;

            const tf_buf_len = pq.decodeVbyte(buffer, current_pos);
            block.tfs.buffer = try allocator.alignedAlloc(
                u8,
                POST_ALIGNMENT,
                tf_buf_len,
            );
            @memcpy(
                block.tfs.buffer[0..tf_buf_len],
                buffer[current_pos.*..][0..tf_buf_len],
            );
            current_pos.* += tf_buf_len;
        }

        self.partial_block.num_docs = buffer[current_pos.*];
        current_pos.* += 1;
        
        self.partial_block.max_tf       = @truncate(
            pq.decodeVbyte(buffer.items, current_pos)
            );
        self.partial_block.max_doc_size = @truncate(
            pq.decodeVbyte(buffer.items, current_pos)
            );


        const doc_id_buf_len = pq.decodeVbyte(buffer.items, current_pos);
        try self.partial_block.doc_ids.buffer.resize(allocator, doc_id_buf_len);

        @memcpy(
            self.partial_block.doc_ids.buffer.items,
            buffer[current_pos.*..][0..doc_id_buf_len],
        );
        current_pos.* += doc_id_buf_len;

        const tf_buf_len = pq.decodeVbyte(buffer.items, current_pos);
        try self.partial_block.tfs.buffer.resize(allocator, tf_buf_len);

        @memcpy(
            self.partial_block.tfs.buffer.items,
            buffer[current_pos.*..][0..tf_buf_len],
        );
        current_pos.* += tf_buf_len;
    }
};

pub const PostingsList = struct {
    postings: std.ArrayListAlignedUnmanaged(
                  PostingV3,
                  POST_ALIGNMENT,
              ),
    arena: std.heap.ArenaAllocator,

    pub fn init(child_allocator: std.mem.Allocator) PostingsList {
        return PostingsList{
            .postings = std.ArrayListAlignedUnmanaged(
                  PostingV3,
                  POST_ALIGNMENT,
              ){},
            .arena = std.heap.ArenaAllocator.init(child_allocator),
        };
    }

    pub fn deinit(self: *PostingsList) void {
        self.arena.deinit();
    }

    pub inline fn add(
        self: *PostingsList, 
        term_id: u32,
        doc_id: u32,
        doc_size: u16,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
        ) !u32 {
        const first_occurence = try self.postings.items[term_id].add(
            self.arena.allocator(), 
            doc_id, 
            doc_size,
            scratch_arr,
            );
        return first_occurence;
    }

    pub inline fn append(
        self: *PostingsList, 
        doc_id: u32,
        doc_size: u16,
        scratch_arr: *align(64) [BLOCK_SIZE]u32,
        ) !void {
        const val = try self.postings.addOne(self.arena.allocator());
        val.* = PostingV3.init();
        _ = try val.add(self.arena.allocator(), doc_id, doc_size, scratch_arr);
    }
};

pub const InvertedIndexV2 = struct {
    posting_list: PostingsList,
    vocab: Vocab,
    doc_freqs: std.ArrayListUnmanaged(u32),
    doc_sizes: std.ArrayListUnmanaged(u16),

    avg_doc_size: f32,

    file_handle: std.fs.File,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: ?usize,
        filename: []const u8,
        ) !InvertedIndexV2 {
        var II = InvertedIndexV2{
            .posting_list = PostingsList.init(allocator),
            .vocab = undefined,
            .doc_freqs = std.ArrayListUnmanaged(u32){},
            .doc_sizes = std.ArrayListUnmanaged(u16){},
            .avg_doc_size = 0.0,
            .file_handle = undefined,
        };
        II.file_handle = try std.fs.cwd().createFile(
             filename, 
             .{ .read = true },
             );
        II.vocab = Vocab.init();

        if (num_docs) |nd| {
            try II.doc_sizes.ensureTotalCapacity(allocator, nd);

            // Guess capacity.
            II.doc_freqs = try std.ArrayListUnmanaged(u32).initCapacity(
                allocator, 
                @as(usize, @intFromFloat(@as(f32, @floatFromInt(nd)) * 0.1))
                );

            // Guess capacity
            try II.vocab.string_bytes.ensureTotalCapacity(allocator, @intCast(nd));
            try II.vocab.map.ensureTotalCapacityContext(allocator, @intCast(nd / 25), II.vocab.getCtx());
        }

        return II;
    }

    pub fn deinit(self: *InvertedIndexV2, allocator: std.mem.Allocator) void {
        self.file_handle.close();

        self.posting_list.deinit();

        self.vocab.deinit(allocator);

        self.doc_freqs.deinit(allocator);
        self.doc_sizes.deinit(allocator);
    }

    pub fn commit(self: *InvertedIndexV2, allocator: std.mem.Allocator) !void {
        var buf = std.ArrayListUnmanaged(u8){};
        try buf.resize(allocator, 1 << 22);
        defer buf.deinit(allocator);

        var current_pos: u64 = 0;
        std.mem.writePackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            @truncate(self.posting_list.postings.items.len),
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        for (self.posting_list.postings.items) |*pos| {
            try pos.serialize(&buf, allocator, &current_pos);
        }

        std.mem.writePackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            @truncate(self.vocab.string_bytes.items.len),
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        if (current_pos + self.vocab.string_bytes.items.len > buf.items.len) {
            try buf.resize(
                allocator, 
                buf.items.len + 2 * self.vocab.string_bytes.items.len,
                );
        }
        @memcpy(
            buf.items[current_pos..][0..self.vocab.string_bytes.items.len],
            self.vocab.string_bytes.items,
        );
        current_pos += self.vocab.string_bytes.items.len;

        std.mem.writePackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            @truncate(self.vocab.map.count()),
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        var max_range = current_pos + (self.vocab.map.count() * 2 * @sizeOf(u32));
        if (max_range > buf.items.len) {
            try buf.resize(allocator, 2 * max_range);
        }
        var map_iterator = self.vocab.map.iterator();
        while (map_iterator.next()) |item| {
            std.mem.writePackedInt(
                u32,
                buf.items[current_pos..][0..4],
                0,
                item.key_ptr.*,
                comptime builtin.cpu.arch.endian(),
            );
            std.mem.writePackedInt(
                u32,
                buf.items[current_pos..][4..8],
                0,
                item.value_ptr.*,
                comptime builtin.cpu.arch.endian(),
            );
            current_pos += 8;
        }

        std.mem.writePackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            @truncate(self.doc_sizes.items.len),
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        max_range = current_pos + self.doc_sizes.items.len * @sizeOf(u16) + @sizeOf(u32);
        if (max_range > buf.items.len) {
            try buf.resize(
                allocator, 
                max_range,
                );
        }
        @memcpy(
            buf.items[current_pos..][0..(self.doc_sizes.items.len * @sizeOf(u16))],
            std.mem.sliceAsBytes(self.doc_sizes.items),
        );
        current_pos += self.doc_sizes.items.len * @sizeOf(u16);

        var ds_sum: u64 = 0;
        for (self.doc_sizes.items) |ds| {
            ds_sum += ds;
        }

        self.avg_doc_size = @as(f32, @floatFromInt(ds_sum)) / 
                            @as(f32, @floatFromInt(self.doc_sizes.items.len));

        std.mem.writePackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            @as(u32, @bitCast(self.avg_doc_size)),
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        try self.file_handle.writeAll(
            buf.items[0..current_pos],
        );

    }

    pub fn load(
        self: *InvertedIndexV2, 
        allocator: std.mem.Allocator,
        filename: []const u8,
        ) !void {
        self.file_handle = try std.fs.cwd().openFile(
             filename, 
             .{ .read = true },
             );
            
        // TODO: Consider buffering on large indexes.
        const file_size = try self.file_handle.getEndPos();
        try self.file_handle.seekTo(0);

        var buf = try allocator.alloc(u8, file_size);
        defer allocator.free(buf);

        _ = try self.file_handle.readAll(buf);

        var current_pos: usize = 0;

        // 1. Postings
        const num_postings = std.mem.readPackedInt(
            u32,
            buf[current_pos..][0..4],
            0,
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        try self.posting_list.postings.resize(num_postings);
        for (0..num_postings) |idx| {
            try self.posting_list.postings.items[idx].deserialize(
                allocator,
                buf,
                current_pos,
            );
        }

        // 2. Vocab
        const num_string_bytes_vocab = std.mem.readPackedInt(
            u32,
            buf[current_pos..][0..4],
            0,
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        try self.vocab.string_bytes.resize(num_string_bytes_vocab);
        @memcpy(
            self.vocab.string_bytes.items,
            buf[current_pos..][0..num_string_bytes_vocab],
        );
        current_pos += num_string_bytes_vocab;

        const num_terms_vocab = std.mem.readPackedInt(
            u32,
            buf[current_pos..][0..4],
            0,
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        for (0..num_terms_vocab) |_| {
            const key = std.mem.readPackedInt(
                u32,
                buf.items[current_pos..][0..4],
                0,
                comptime builtin.cpu.arch.endian(),
            );
            const value = std.mem.readPackedInt(
                u32,
                buf.items[current_pos..][4..8],
                0,
                comptime builtin.cpu.arch.endian(),
            );
            current_pos += 8;

            try self.vocab.map.putNoClobber(allocator, key, value);
        }

        // Doc sizes
        const num_docs = std.mem.readPackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            comptime builtin.cpu.arch.endian(),
        );
        current_pos += 4;

        try self.doc_sizes.resize(allocator, num_docs);
        @memcpy(
            std.mem.sliceAsBytes(self.doc_sizes.items),
            buf[current_pos..][0..(num_docs * @sizeOf(u16))],
        );
        current_pos += num_docs * @sizeOf(u16);

        // Avg doc size
        self.avg_doc_size = @bitCast(std.mem.readPackedInt(
            u32,
            buf.items[current_pos..][0..4],
            0,
            comptime builtin.cpu.arch.endian(),
        ));
        current_pos += 4;
    }
};

pub const BM25Partition = struct {
    II: []InvertedIndexV2,
    allocator: std.mem.Allocator,

    doc_store: DocStore,

    scratch_arr: [BLOCK_SIZE]u32 align(64),

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        num_records: usize,
        tmp_dir: []const u8,
        partition_idx: usize,
    ) !BM25Partition {
        var partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .allocator = allocator,
            .doc_store = undefined,

            .scratch_arr = undefined,
        };
        @memset(partition.scratch_arr[0..BLOCK_SIZE], 0);

        for (0..num_search_cols) |idx| {
            const output_filename = try std.fmt.allocPrint(
                allocator, 
                "{s}/posting_{d}_{d}.bin", 
                .{tmp_dir, idx, partition_idx}
                );
            defer allocator.free(output_filename);

            partition.II[idx] = try InvertedIndexV2.init(
                allocator, 
                num_records,
                output_filename,
                );
        }

        return partition;
    }

    pub fn deinit(self: *BM25Partition) !void {
        for (0..self.II.len) |i| {
            self.II[i].deinit(self.allocator);
        }
        self.allocator.free(self.II);

        try self.doc_store.deinit();
    }

    pub fn initFromDisk(
        allocator: std.mem.Allocator,
        dir: []const u8, 
        partition_idx: usize,
        num_search_cols: usize,
        ) !BM25Partition {

        var partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .allocator = allocator,

            // TODO: Implement doc_store loading.
            .doc_store = undefined,

            .scratch_arr = undefined,
        };
        @memset(partition.scratch_arr[0..BLOCK_SIZE], 0);

        for (0..num_search_cols) |idx| {
            const filename = try std.fmt.allocPrint(
                allocator,
                "{s}/posting_{d}_{d}.bin",
                .{dir, idx, partition_idx},
                );
            defer allocator.free(filename);

            partition.II[idx] = try InvertedIndexV2.init(
                allocator, 
                null,
                filename,
                );
            try partition.II[idx].load(partition_idx.allocator, filename);
        }
    }

    pub fn resizeNumSearchCols(
        self: *BM25Partition, 
        num_search_cols: usize,
        tmp_dir: []const u8,
        partition_idx: usize,
        ) !void {
        const current_length = self.II.len;
        if (num_search_cols <= current_length) return;


        self.II = try self.allocator.realloc(self.II, num_search_cols);
        for (current_length..num_search_cols) |idx| {
            const output_filename = try std.fmt.allocPrint(
                self.allocator, 
                "{s}/posting_{d}_{d}.bin", 
                .{tmp_dir, idx, partition_idx},
                );
            defer self.allocator.free(output_filename);

            self.II[idx] = try InvertedIndexV2.init(
                self.allocator, 
                self.II[idx].doc_sizes.items.len,
                output_filename,
                );
        }
    }

    inline fn addTerm(
        self: *BM25Partition,
        term: []u8,
        term_len: usize,
        doc_id: u32,
        _: u16,
        col_idx: usize,
        // terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        _: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        std.debug.assert(
            self.II[col_idx].vocab.map.count() < (1 << 32),
            );
        // std.debug.assert(
            // terms_seen.count < MAX_NUM_TERMS
            // );

        const gop = try self.II[col_idx].vocab.map.getOrPutContextAdapted(
            self.allocator,
            term[0..term_len],
            self.II[col_idx].vocab.getAdapter(),
            self.II[col_idx].vocab.getCtx(),
            );

        self.II[col_idx].doc_sizes.items[doc_id] += 1;

        if (!gop.found_existing) {
            try self.II[col_idx].vocab.string_bytes.appendSlice(
                self.allocator, 
                term[0..term_len],
                );
            try self.II[col_idx].vocab.string_bytes.append(self.allocator, 0);

            gop.key_ptr.* = @truncate(
                self.II[col_idx].vocab.string_bytes.items.len - term_len - 1,
                );
            gop.value_ptr.* = self.II[col_idx].vocab.map.count() - 1;

            try self.II[col_idx].doc_freqs.append(self.allocator, 1);
            try self.II[col_idx].posting_list.append(
                doc_id,
                self.II[col_idx].doc_sizes.items[doc_id],
                &self.scratch_arr,
            );

        } else {

            const val = gop.value_ptr.*;

            self.II[col_idx].doc_freqs.items[val] += try self.II[col_idx].posting_list.add(
                val,
                doc_id,
                self.II[col_idx].doc_sizes.items[doc_id],
                &self.scratch_arr,
                );
        }
    }

    inline fn addToken(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u16,
        col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        if (cntr.* == 0) {
            return;
        }

        try self.addTerm(
            term, 
            cntr.*, 
            doc_id, 
            term_pos.*, 
            col_idx, 
            terms_seen,
            );

        term_pos.* += @intFromBool(term_pos.* != std.math.maxInt(u16));
        cntr.* = 0;
    }

    inline fn flushLargeToken(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u16,
        col_idx: usize,
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {
        try self.addTerm(
            term, 
            cntr.*, 
            doc_id, 
            term_pos.*, 
            col_idx, 
            token_stream, 
            terms_seen,
            new_doc,
            );

        term_pos.* += @intFromBool(term_pos.* != std.math.maxInt(u16));
        cntr.* = 0;
    }


    pub fn processDocRfc4180(
        self: *BM25Partition,
        buffer: []u8,
        byte_idx: *usize,
        doc_id: u32,
        col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        try self.II[col_idx].doc_sizes.append(self.allocator, 0);

        var buffer_idx = byte_idx.*;

        if (
            (buffer[buffer_idx] == ',') 
                or 
            (buffer[buffer_idx] == '\n')
            ) {
            byte_idx.* += 1;
            return;
        }

        var term_pos: u16 = 0;
        const is_quoted = (buffer[buffer_idx] == '"');
        buffer_idx += @intFromBool(is_quoted);

        var cntr: usize = 0;

        // terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[col_idx].doc_sizes.items[doc_id] >= MAX_NUM_TERMS) {
                    csv._iterFieldCSV(buffer, byte_idx);
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);

                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        terms_seen,
                        );
                    buffer_idx += 1;
                    continue;
                }

                switch (buffer[buffer_idx]) {
                    '"' => {
                        buffer_idx += 1;

                        switch (buffer[buffer_idx]) {
                            ',', '\n' => {
                                buffer_idx += 1;
                                break :outer_loop;
                            },
                            '"' => {
                                if (cntr == 0) {
                                    const start_idx = buffer_idx - 1 - @min(buffer_idx - 1, cntr);

                                    try self.addToken(
                                        buffer[start_idx..], 
                                        &cntr, 
                                        doc_id, 
                                        &term_pos, 
                                        col_idx, 
                                        terms_seen,
                                        );
                                }
                                buffer_idx += 1;
                                continue;
                            },
                            else => return error.UnexpectedQuote,
                        }
                    },
                    0...33, 35...47, 58...64, 91...96, 123...126 => {
                        if (cntr == 0) {
                            buffer_idx += 1;
                            continue;
                        }

                        const start_idx = buffer_idx - @min(buffer_idx, cntr);

                        try self.addToken(
                            buffer[start_idx..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            col_idx, 
                            terms_seen,
                            );
                    },
                    else => {
                        cntr += 1;
                        buffer_idx += 1;
                    },
                }
            }

        } else {

            outer_loop: while (true) {
                std.debug.assert(
                    self.II[col_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS
                    );

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);
                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        terms_seen,
                        );
                    buffer_idx += 1;
                    continue;
                }


                switch (buffer[buffer_idx]) {
                    ',', '\n' => {
                        buffer_idx += 1;
                        break :outer_loop;
                    },
                    0...9, 11...43, 45...47, 58...64, 91...96, 123...126 => {
                        if (cntr == 0) {
                            buffer_idx += 1;
                            cntr = 0;
                            continue;
                        }

                        const start_idx = buffer_idx - @min(buffer_idx, cntr);

                        try self.addToken(
                            buffer[start_idx..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            col_idx, 
                            terms_seen,
                            );
                    },
                    else => {
                        cntr       += 1;
                        buffer_idx += 1;
                    },
                }
            }
        }

        if (cntr > 0) {
            std.debug.assert(self.II[col_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS);

            const start_idx = buffer_idx - @intFromBool(is_quoted) 
                              - @min(buffer_idx - @intFromBool(is_quoted), cntr + 1);

            try self.addTerm(
                buffer[start_idx..], 
                cntr, 
                doc_id, 
                term_pos, 
                col_idx, 
                terms_seen,
                );
        }

        byte_idx.* = buffer_idx;
    }


    pub fn processDocVbyte(
        self: *BM25Partition,
        buffer: []u8,
        doc_id: u32,
        search_col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        try self.II[search_col_idx].doc_sizes.append(self.allocator, 0);

        string_utils.stringToUpper(
            @ptrCast(buffer[0..]), 
            buffer.len,
            );

        if (buffer.len == 0) {
            return;
        }

        var term_pos: u16 = 0;

        var cntr: usize = 0;

        terms_seen.clear();

        var buffer_idx: usize = 0;
        while (buffer_idx < buffer.len) {
            std.debug.assert(
                self.II[search_col_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS
                );

            if (cntr > MAX_TERM_LENGTH - 4) {
                @branchHint(.cold);
                try self.addToken(
                    buffer[buffer_idx - cntr..], 
                    &cntr, 
                    doc_id, 
                    &term_pos, 
                    search_col_idx, 
                    terms_seen,
                    );
                buffer_idx += 1;
                continue;
            }


            switch (buffer[buffer_idx]) {
                0...47, 58...64, 91...96, 123...126 => {
                    if (cntr == 0) {
                        buffer_idx += 1;
                        cntr = 0;
                        continue;
                    }

                    const start_idx = buffer_idx - @min(buffer_idx, cntr);
                    try self.addToken(
                        buffer[start_idx..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        search_col_idx, 
                        terms_seen,
                        );
                },
                else => {
                    cntr += 1;
                    buffer_idx += 1;
                },
            }
        }

        if (cntr > 0) {
            std.debug.assert(
                self.II[search_col_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS
                );

            const start_idx = buffer_idx - @min(buffer_idx, cntr + 1);
            try self.addTerm(
                buffer[start_idx..], 
                cntr, 
                doc_id, 
                term_pos, 
                search_col_idx, 
                terms_seen,
                );
        }
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        buffer: []u8,
        trie: *const RadixTrie(u32),
        search_col_idxs: *const std.ArrayListUnmanaged(u32),
        byte_idx: *usize,
        doc_id: u32,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        var buffer_idx: usize = 0;
        // std.debug.print("BUFFER: {s}\n", .{buffer[buffer_idx..][0..64]});

        // Matches key against keys stored in trie. If not found 
        // maxInt(u32) is returned. If EOL an error is returned.
        // buffer_idx is incremented to the next key, next line,
        // or if matched, start of value.
        const matched_col_idx = json.matchKVPair(
            buffer,
            &buffer_idx,
            trie,
            false,
        ) catch {
            // EOL. Not indexed key.
            byte_idx.* += buffer_idx;
            return;
        };
        if (matched_col_idx == std.math.maxInt(u32)) {
            byte_idx.* += buffer_idx;
            return;
        }

        const II_idx = findSorted(
            u32,
            search_col_idxs.items,
            matched_col_idx,
        ) catch {
            try json.iterValueJSON(buffer, &buffer_idx);
            if (buffer[buffer_idx] == '}') {
                buffer_idx += 1;
                while (buffer[buffer_idx] != '{') buffer_idx += 1;
                byte_idx.* += buffer_idx;
                return error.EOL;
            }
            buffer_idx += 1;
            buffer_idx += string_utils.simdFindCharIdxEscaped(
                buffer[buffer_idx..], 
                '"',
                false,
            );
            byte_idx.* += buffer_idx;
            return;
        };

        try self.II[II_idx].doc_sizes.append(self.allocator, 0);

        // Empty check.
        if (
            (buffer[buffer_idx] == ',') 
                or 
            (buffer[buffer_idx] == '}')
            ) {

            buffer_idx += 1;
            switch (buffer[buffer_idx - 1]) {
                '}' => {
                    buffer_idx += string_utils.simdFindCharIdxEscapedFull(
                        buffer[buffer_idx..],
                        '{',
                        );
                    byte_idx.* += buffer_idx;
                    return error.EOL;
                },
                ',' => {
                    buffer_idx += string_utils.simdFindCharIdxEscapedFull(
                        buffer[buffer_idx..],
                        '"',
                        );
                    byte_idx.* += buffer_idx;
                },
                else => unreachable,
            }

            return;
        }

        var term_pos: u16 = 0;
        const is_quoted = (buffer[buffer_idx] == '"');
        buffer_idx += @intFromBool(is_quoted);

        var cntr: usize   = 0;

        terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[II_idx].doc_sizes.items[doc_id] >= MAX_NUM_TERMS) {
                    buffer_idx = 0;
                    try json._iterFieldJSON(buffer, &buffer_idx);
                    byte_idx.* += buffer_idx;
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);
                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        terms_seen,
                        );
                    buffer_idx += 1;
                    continue;
                }

                switch (buffer[buffer_idx]) {
                    '"' => {
                        buffer_idx += 1;
                        json.nextPoint(buffer, &buffer_idx);
                        break :outer_loop;
                    },
                    0...33, 35...47, 58...64, 91, 93...96, 123...126 => {
                        if (cntr == 0) {
                            buffer_idx += 1;
                            continue;
                        }

                        const start_idx = buffer_idx - @min(buffer_idx, cntr);

                        try self.addToken(
                            buffer[start_idx..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            II_idx, 
                            terms_seen,
                            );
                    },
                    '\\' => buffer_idx += 2,
                    else => {
                        cntr += 1;
                        buffer_idx += 1;
                    },
                }
            }
        } else {

            outer_loop: while (true) {
                std.debug.assert(self.II[II_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS);

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);
                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        terms_seen,
                        );
                    buffer_idx += 1;
                    continue;
                }


                switch (buffer[buffer_idx]) {
                    ',', '}' => break :outer_loop,

                    78 => {
                        // NULL
                        buffer_idx += 4;
                        cntr = 4;

                        try self.addToken(
                            buffer[buffer_idx - 4..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            II_idx, 
                            terms_seen,
                            );
                        json.nextPoint(buffer, &buffer_idx);
                        break: outer_loop;
                    },
                    84 => {
                        // TRUE
                        buffer_idx += 4;
                        cntr = 4;

                        try self.addToken(
                            buffer[buffer_idx - 4..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            II_idx, 
                            terms_seen,
                            );
                        json.nextPoint(buffer, &buffer_idx);
                        break: outer_loop;
                    },
                    70 => {
                        // FALSE
                        buffer_idx += 5;
                        cntr = 5;

                        try self.addToken(
                            buffer[buffer_idx - 5..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            II_idx, 
                            terms_seen,
                            );
                        json.nextPoint(buffer, &buffer_idx);
                        break: outer_loop;
                    },
                    45, 48...57 => {
                        if (cntr == 0) {
                            buffer_idx += 1;
                            cntr = 0;
                            continue;
                        }
                        while (
                            (buffer[buffer_idx] == 45)
                                or
                            (
                             (buffer[buffer_idx] >= 48)
                                and
                             (buffer[buffer_idx] <= 57)
                            )
                        ) {
                            buffer_idx += 1;
                            cntr += 1;
                        }

                        const start_idx = buffer_idx - @min(buffer_idx, cntr);

                        try self.addToken(
                            buffer[start_idx..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            II_idx, 
                            terms_seen,
                            );
                        json.nextPoint(buffer, &buffer_idx);
                        break :outer_loop;
                    },
                    else => {
                        cntr += 1;
                        buffer_idx += 1;
                        json.nextPoint(buffer, &buffer_idx);
                        break :outer_loop;
                    },
                }
            }
        }

        if (cntr > 0) {
            std.debug.assert(self.II[II_idx].doc_sizes.items[doc_id] < MAX_NUM_TERMS);

            const start_idx = buffer_idx - @intFromBool(is_quoted) - 
                              @min(buffer_idx - @intFromBool(is_quoted), cntr + 1);
            try self.addTerm(
                buffer[start_idx..],
                cntr,
                doc_id,
                term_pos,
                II_idx,
                terms_seen,
                );
        }

        switch (buffer[buffer_idx]) {
            ',' => {
                while (buffer[buffer_idx] != '"') buffer_idx += 1;
                byte_idx.* += buffer_idx;
            },
            '}' => {
                while (buffer[buffer_idx] != '{') buffer_idx += 1;
                byte_idx.* += buffer_idx;
                return error.EOL;
            },
            else => unreachable,
        }
    }

    pub inline fn fetchRecordsDocStore(
        self: *BM25Partition,
        result_positions: []TermPos,
        query_result: QueryResult,
        record_string: *std.ArrayListUnmanaged(u8),

        bit_sizes: []u32,
    ) void {
        self.doc_store.getRow(
            @intCast(query_result.doc_id),
            record_string,
            self.allocator,
            result_positions,

            bit_sizes,
        ) catch {
            @panic("Error fetching document.");
        };
    }
};
