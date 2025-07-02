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

pub const ENDIANESS = builtin.cpu.arch.endian();




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
    slice: [BLOCK_SIZE]u32, 
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

    const new_value: @Vector(16, u32) align(8) = @splat(target);
    var existing_values = @as(
        *const align(8) @Vector(16, u32), 
        @alignCast(@ptrCast(&slice)),
        );

    var block_idx = start_block_idx;
    while (block_idx < NUM_BLOCKS) {
        const mask = new_value < existing_values.*;

        const set_idx = @ctz(@as(u16, @bitCast(mask)));
        if (set_idx != 16) {
            return (block_idx << 4) + set_idx;
        }

        existing_values = @ptrFromInt(@intFromPtr(existing_values) + 32);
        block_idx += 1;
    }
    return null;
}


pub const PostingsIteratorV2 = struct {
    posting: PostingV3,
    posting_len: usize,
    uncompressed_doc_ids_buffer: [BLOCK_SIZE]u32,
    uncompressed_tfs_buffer: [BLOCK_SIZE]u16,
    current_doc_idx: usize,
    tp_idx: usize,
    boost_weighted_idf: f32,
    boost: f32,
    term_id: u32,
    col_idx: u32,
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
        ) !PostingsIteratorV2 {
        const posting_len = (BLOCK_SIZE * posting.full_blocks.items.len) + 
                             posting.partial_block.num_docs;
        var it = PostingsIteratorV2{ 
            .posting = posting,
            .posting_len = posting_len,
            .uncompressed_doc_ids_buffer = [_]u32{0} ** BLOCK_SIZE,
            .uncompressed_tfs_buffer = [_]u16{0} ** BLOCK_SIZE,
            .current_doc_idx = 0,
            .tp_idx = undefined,
            .boost_weighted_idf = idf * boost,
            .boost = boost,
            .term_id = term_id,
            .col_idx = col_idx,
            .consumed = false,
            .on_partial_block = posting_len < BLOCK_SIZE,
        };

        // TODO: Consider storing max_doc_id in metadata to potentially
        //       skip decompressing blocks.

        if (it.on_partial_block) {
            // Decompress partial
            it.posting.partial_block.decompressToBuffers(
                &it.uncompressed_doc_ids_buffer,
                &it.uncompressed_tfs_buffer,
                );
        } else {
            // Decompress full
            try it.posting.full_blocks.items[0].decompressToBuffers(
                &it.uncompressed_doc_ids_buffer,
                &it.uncompressed_tfs_buffer,
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
        }
        // std.debug.print("Current doc id: {d}\n", .{self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE]});

        const prev_idx = self.current_doc_idx - 1;
        return .{
            .doc_id    = self.uncompressed_doc_ids_buffer[prev_idx % BLOCK_SIZE],
            .term_freq = self.uncompressed_tfs_buffer[prev_idx % BLOCK_SIZE],
            .term_pos  = undefined,
        };
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
                self.on_partial_block = block_idx == self.posting.full_blocks.items.len;
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

        return .{
            .doc_id = self.uncompressed_doc_ids_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_freq = self.uncompressed_tfs_buffer[self.current_doc_idx % BLOCK_SIZE],
            .term_pos = undefined,
        };
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

            if (num_full_blocks == 0) {
                self.on_partial_block = true;
                self.posting.partial_block.decompressToBuffers(
                    &self.uncompressed_doc_ids_buffer,
                    &self.uncompressed_tfs_buffer,
                    );

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

                idx = self.current_doc_idx % BLOCK_SIZE;
                return .{
                    .doc_id    = self.uncompressed_doc_ids_buffer[idx],
                    .term_freq = self.uncompressed_tfs_buffer[idx],
                    .term_pos  = undefined,
                };
            }

            var block_idx = self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE));
            var skipped_block = false;
            while (block_idx < num_full_blocks) {
                if (target_id > self.posting.full_blocks.items[block_idx].max_doc_id) {
                    idx = 0;
                    block_idx += 1;
                    self.current_doc_idx += BLOCK_SIZE;
                    skipped_block = true;
                    continue;
                }

                // Doc id in full block if here.
                if (skipped_block) {
                    self.current_doc_idx = std.mem.alignBackward(
                        usize,
                        self.current_doc_idx + BLOCK_SIZE,
                        64,
                    );
                    try self.posting.full_blocks.items[block_idx].decompressToBuffers(
                        &self.uncompressed_doc_ids_buffer,
                        &self.uncompressed_tfs_buffer,
                        );
                }

                if (linearLowerBoundSIMD(
                    self.uncompressed_doc_ids_buffer,
                    idx,
                    target_id,
                )) |matched_idx| {

                    self.current_doc_idx += matched_idx;
                    return .{
                        .doc_id = self.uncompressed_doc_ids_buffer[matched_idx],
                        .term_freq = self.uncompressed_tfs_buffer[matched_idx],
                        .term_pos = undefined,
                    };
                }
                unreachable;
            }

            if (skipped_block) {
                self.current_doc_idx = std.mem.alignBackward(
                    usize,
                    self.current_doc_idx + BLOCK_SIZE,
                    64,
                );
            }
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

        idx = self.current_doc_idx % BLOCK_SIZE;
        return .{
            .doc_id    = self.uncompressed_doc_ids_buffer[idx],
            .term_freq = self.uncompressed_tfs_buffer[idx],
            .term_pos  = undefined,
        };
    }

    pub inline fn currentBlockMaxScore(self: *const PostingsIteratorV2) f32 {
        if (self.on_partial_block) {
            return self.posting.partial_block.max_score * self.boost;
        }
        return self.posting.full_blocks.items[
            self.current_doc_idx >> (comptime std.math.log2(BLOCK_SIZE))
        ].max_score * self.boost;
    }
};

pub const QueryResult = packed struct(u64){
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

pub const Postings = struct {
    doc_ids: []u32,
    term_positions: []u16,
};

pub const flag_u32 = packed struct(u32) {
    value: u31,
    is_inline: u1,
};

pub const flag_tp_u32 = packed struct(u32) {
    second: u15,
    _: u1,

    first: u15,
    is_inline: u1,
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
    buffer: []u8 align(512),


    pub fn build(
        allocator: std.mem.Allocator,
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
        sorted_vals: *align(512) [BLOCK_SIZE]u32,
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
            std.mem.Alignment.fromByteUnits(512),
            buffer_size,
            );
        @memset(dbp.buffer, 0);

        var bit_pos: usize = 0;
        inline for (0..BLOCK_SIZE) |idx| {
            const gap: u32  = scratch_arr[idx];

            putBits(dbp.buffer, bit_pos, gap, dbp.bit_size);
            bit_pos += dbp.bit_size;
        }

        return dbp;
    }
};

pub const BitpackedBlock = struct {
    max_val: u32,
    bit_size: u8,
    buffer: []u8 align(512),

    pub fn build(
        allocator: std.mem.Allocator,
        vals: *align(512) [BLOCK_SIZE]u16,
        ) !BitpackedBlock {
        var dbp = BitpackedBlock{
            .max_val = vals[0],
            .buffer = undefined,
            .bit_size = undefined,
        };
        var block_maxes: @Vector(NUM_BLOCKS, u16) = @splat(0);

        const sv_vec: [*]@Vector(16, u16) = @ptrCast(vals);
        inline for (0..NUM_BLOCKS) |block_idx| {
            block_maxes[block_idx] = @reduce(.Max, sv_vec[block_idx]);
        }

        const max_val: u16 = @reduce(.Max, block_maxes);
        dbp.bit_size = 16 - @clz(max_val);
        const buffer_size = ((BLOCK_SIZE * dbp.bit_size) + 7) >> 3;

        dbp.buffer = try allocator.alignedAlloc(
            u8,
            comptime std.mem.Alignment.fromByteUnits(512),
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
    max_score: f32,
    max_doc_id: u32,

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
    buffer: std.ArrayListAlignedUnmanaged(u8, std.mem.Alignment.fromByteUnits(512)),

    pub fn init() DeltaVByteBlock {
        return DeltaVByteBlock{
            .buffer = std.ArrayListAlignedUnmanaged(u8, std.mem.Alignment.fromByteUnits(512)){},
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
                comptime std.mem.Alignment.fromByteUnits(512),
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
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
        ) !DeltaBitpackedBlock {
        // Optimize later. For now, just convert to u32 buf, then call above func.

        // TODO: Thread local static arrays.
        var tmp_arr: [BLOCK_SIZE]u32 align(512) = undefined;

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
                std.mem.Alignment.fromByteUnits(512),
                ),

    pub fn init() VByteBlock {
        return VByteBlock{
            .buffer = std.ArrayListAlignedUnmanaged(
                u8, 
                std.mem.Alignment.fromByteUnits(512),
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
                comptime std.mem.Alignment.fromByteUnits(512),
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
        var tmp_arr: [BLOCK_SIZE]u16 align(512) = undefined;

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
    max_score: f32,
    num_docs: u8,

    pub fn init() PostingsBlockPartial {
        return PostingsBlockPartial{
            .num_docs = 0,
            .doc_ids = DeltaVByteBlock.init(),
            .tfs = VByteBlock.init(),

            .prev_doc_id = 0,
            .max_score = undefined,
        };
    }

    pub fn flush(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
    ) !PostingsBlockFull {
        const full_map = try self.partialToFull(allocator, scratch_arr, self.max_score);

        self.num_docs = 0;
        self.doc_ids.clear();
        self.tfs.clear();
        self.max_score = 0.0;
        self.prev_doc_id = 0;

        return full_map;
    }

    fn partialToFull(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
        max_score: f32,
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
            .max_score = max_score,
            .max_doc_id = undefined,
        };
        pbf.max_doc_id = scratch_arr[BLOCK_SIZE - 1];

        return pbf;
    }

    pub inline fn add(
        self: *PostingsBlockPartial,
        allocator: std.mem.Allocator,
        doc_id: u32,
    ) !void {
        try self.doc_ids.add(
            allocator,
            doc_id,
            self.prev_doc_id,
        );
        self.prev_doc_id = doc_id;

        // TODO: Fix. Add actual TF.
        try self.tfs.add(
            allocator,
            1,
        );
        self.max_score = @max(self.max_score, 1.0);

        self.num_docs += 1;
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
                     std.mem.Alignment.fromByteUnits(512),
                 ),
    partial_block: PostingsBlockPartial,

    pub inline fn init() PostingV3 {
        return PostingV3{
            .full_blocks = std.ArrayListAlignedUnmanaged(
                PostingsBlockFull,
                std.mem.Alignment.fromByteUnits(512),
            ){},
            .partial_block = PostingsBlockPartial.init(),
        };
    }

    pub inline fn add(
        self: *PostingV3,
        allocator: std.mem.Allocator,
        doc_id: u32,
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
    ) !void {
        try self.partial_block.add(
            allocator,
            doc_id,
        );

        if (self.partial_block.num_docs == BLOCK_SIZE) {
            const new_val = try self.full_blocks.addOne(allocator);
            new_val.* = try self.partial_block.flush(allocator, scratch_arr);
        }
    }

    pub fn serialize(
        self: *PostingV3, 
        buffer: *std.ArrayListUnmanaged(u8),
        allocator: std.mem.Allocator,
        current_pos: *usize,
        num_docs: usize
        ) !void {
        const df = (BLOCK_SIZE * self.full_blocks.items.len) + self.partial_block.num_docs;
        const idf: f32 = 1.0 + @as(f32, @floatFromInt(std.math.log2(num_docs / df)));

        for (self.full_blocks.items) |*block| {
            const max_range = current_pos.* + 
                (block.doc_ids.buffer.len + block.tfs.buffer.len) + @sizeOf(u32) * 3
                + @sizeOf(u8) * 2;

            if (max_range > buffer.items.len) {
                @branchHint(.unlikely);
                try buffer.resize(allocator, 2 * max_range);
            }

            std.mem.writePackedInt(
                u32,
                buffer.items[current_pos.*..][0..4],
                0,
                block.max_doc_id,
                ENDIANESS,
            );
            current_pos.* += 4;

            // max_score currently is max_tf.
            block.max_score *= idf;
            @memcpy(
                buffer.items[current_pos.*..][0..4],
                @as([4]u8, @bitCast(block.max_score))[0..4],
            );
            current_pos.* += 4;

            std.mem.writePackedInt(
                u32,
                buffer.items[current_pos.*..][0..4],
                0,
                block.doc_ids.min_val,
                ENDIANESS,
            );
            current_pos.* += 4;

            buffer.items[current_pos.*] = block.doc_ids.bit_size;
            current_pos.* += 1;

            @memcpy(
                buffer.items[current_pos.*..][0..block.doc_ids.buffer.len],
                block.doc_ids.buffer,
            );
            current_pos.* += block.doc_ids.buffer.len;

            buffer.items[current_pos.*] = block.tfs.bit_size;
            current_pos.* += 1;

            @memcpy(
                buffer.items[current_pos.*..][0..block.tfs.buffer.len],
                block.tfs.buffer,
            );
            current_pos.* += block.tfs.buffer.len;
        }

        self.partial_block.max_score *= idf;
        const max_range = current_pos.* + 1 + 4 +
            (self.partial_block.doc_ids.buffer.items.len + self.partial_block.tfs.buffer.items.len);
        if (max_range > buffer.items.len) {
            @branchHint(.unlikely);
            try buffer.resize(allocator, 2 * max_range);
        }

        buffer.items[current_pos.*] = self.partial_block.num_docs;
        current_pos.* += 1;

        @memcpy(
            buffer.items[current_pos.*..][0..4],
            @as([4]u8, @bitCast(self.partial_block.max_score))[0..4],
        );
        current_pos.* += 4;

        @memcpy(
            buffer.items[current_pos.*..][0..self.partial_block.doc_ids.buffer.items.len],
            self.partial_block.doc_ids.buffer.items,
        );
        current_pos.* += self.partial_block.doc_ids.buffer.items.len;

        @memcpy(
            buffer.items[current_pos.*..][0..self.partial_block.tfs.buffer.items.len],
            self.partial_block.tfs.buffer.items,
        );
        current_pos.* += self.partial_block.tfs.buffer.items.len;
    }
};


pub const TPList = struct {
    // TODO: Add compression.
    term_positions: []u16,
};

pub const PostingsV2 = struct {
    doc_id_ptrs:  []align(std.heap.page_size_min)flag_u32,
    doc_id_buf: []align(std.heap.page_size_min)u32,

    term_pos_ptrs:  []align(std.heap.page_size_min)flag_tp_u32,

    // Max value means TF > 1. Look up in hash map.
    term_positions: []align(std.heap.page_size_min)u16,

    pub const BlockPair = packed struct(u64) {
        block_idx: u32,
        buffer_idx: u32,
    };


    // ____________________________________________________________________
    // term_positions
    // term <IDX>: | doc_id<0> [tp_0, tp_1, tp_2], doc_id<1> [tp_0, tp_1] |
    //                                             ^greater than doc_id<0>
    // ____________________________________________________________________

    pub inline fn getBlockIdx(_: *const PostingsV2, idx: usize) !BlockPair {
        // Given an index which represents the TP if this were a giant raw u16 buffer,
        // return the corresponding block_idx and the buffer_idx.
        
        // block_idx = num_254_buffers.
        const block_idx = try std.math.divCeil(usize, idx, 254);
        const buffer_idx = idx % 254;
        return .{
            .block_idx = block_idx,
            .buffer_idx = buffer_idx,
        };
    }

    // pub inline fn advanceToBlock(self: *const BlockPair, doc_id: usize, start_pos: BlockPair) !BlockPair {
        // // Return first position where the doc_id is greater than or equal to the given doc_id.
       //  
        // // TODO: Handle boundary condition.
        // var current_buffer_idx = start_pos.buffer_idx;
        // var current_block_idx = start_pos.block_idx;
// 
        // var current_doc_id = self.term_positions[start_pos.block_idx].current_doc_id;
        // var next_doc_id = self.term_positions[start_pos.block_idx + 1].current_doc_id;
// 
        // while (next_doc_id < doc_id) {
            // current_block_idx += 1;
            // current_buffer_idx = 0;
// 
            // current_doc_id = next_doc_id;
            // next_doc_id = self.term_positions[start_pos.block_idx + 1].current_doc_id;
        // }
// 
        // // const current_block = self.term_positions[current_block_idx].term_positions;
        // const current_block = @as([*]@Vector(16, u16), @ptrCast(
            // self.term_positions[current_block_idx].term_positions
        // ));
        // const mask: @Vector(16, u16) = comptime @splat(0b10000000_00000000);
// 
        // while (current_doc_id < doc_id) {
            // while (current_buffer_idx < 254) {
                // const skip_docs = @popCount(current_block[@divExact(current_buffer_idx, 16)].* & mask);
                // if (skip_docs + current_doc_id >= doc_id) {
                    // while (current_doc_id < doc_id) {
                        // current_doc_id += @popCount(
                            // 0b10000000_00000000 & self.term_positions[current_block_idx].term_positions[current_buffer_idx]
                            // );
                        // current_buffer_idx += 1;
                        // if (current_doc_id >= doc_id) {
                            // return .{
                                // .block_idx = current_block_idx,
                                // .buffer_idx = current_buffer_idx,
                            // };
                        // }
                    // }
                    // unreachable;
                // }
// 
                // current_doc_id += skip_docs;
                // current_buffer_idx += 16;
            // }
        // }
        // unreachable;
    // }

    pub inline fn nextTermPos(
        self: *const PostingsV2, 
        start_idx: usize,
        ) !usize {
        // Return the next doc_id.

        // TODO: Handle boundary condition.
        var idx = start_idx + 1;
        var val: u16 = @bitCast(self.term_positions[idx]);
        const mask: u16 = 0b10000000_00000000;
        while (mask & val > 0) {
            idx += 1;
            val = @bitCast(self.term_positions[idx]);
        }
        return idx;
    }

    pub inline fn advanceToBlock(
        self: *const PostingsV2, 
        init_doc_id_idx: usize,
        target_doc_id_idx: usize,
        start_idx: usize,
        max_idx: usize,
        ) !usize {
        // Return first position where the doc_id is greater than or equal to the given doc_id.
        
        // TODO: Handle boundary condition.
        //       Can just check next block's max id.
        //       Only do that if this term's range extends beyond the current block.
        //       Else force linear scan.
        // std.debug.print(
            // "Current idx: {d} | Max idx: {d}\n",
            // .{start_idx, max_idx},
        // );
        std.debug.assert(max_idx >= start_idx);
        const block_range_rem = 256 - (start_idx % 256);
        const range_rem = max_idx - start_idx;
        if (range_rem > block_range_rem) {
            std.debug.print("BLOCK\n", .{});
            return try self.advanceToBlockSkipping(
                init_doc_id_idx, 
                target_doc_id_idx, 
                start_idx,
                max_idx,
                );
        } else {
            std.debug.print("INLINE\n", .{});
            return try self.advanceToBlockInline(
                init_doc_id_idx, 
                target_doc_id_idx, 
                start_idx, 
                max_idx,
                );
        }
    }

    inline fn advanceToBlockSkipping(
        self: *const PostingsV2, 
        // init_doc_id_idx: usize,
        _: usize,
        target_doc_id_idx: usize, 
        start_idx: usize,
        max_idx: usize,
    ) !usize {
        var current_block_idx = @divFloor(start_idx, 256);

        const end_doc_id = self.doc_id_buf[target_doc_id_idx];

        var range = max_idx - start_idx;

        var block_skipped: usize = 0;
        while (self.block_doc_id_offsets[current_block_idx + 1] < end_doc_id) {
            current_block_idx += 1;
            block_skipped += 1;

            if (range < 256) break;
            range -= 256;
        }
        const block_start_idx = std.mem.alignBackward(
            usize,
            start_idx + (256 * block_skipped),
            256,
        );
        return self.advanceToBlockInline(
            block_start_idx,
            target_doc_id_idx,
            start_idx,
            max_idx,
        );
    }

    inline fn advanceToBlockInline(
        self: *const PostingsV2, 
        init_doc_id_idx: usize,
        target_doc_id_idx: usize, 
        start_idx: usize,
        max_idx: usize,
    ) !usize {
        // Fix here.
        var current_doc_id_idx = init_doc_id_idx;

        const start_simd_idx = std.mem.alignForward(
            usize,
            start_idx,
            16,
        );
        const mask: u16 = 0b10000000_00000000;
        var current_buffer_idx = start_idx;

        if (max_idx >= start_simd_idx) {
            while ((current_doc_id_idx < target_doc_id_idx) and (current_buffer_idx < max_idx)) {
                current_doc_id_idx += @popCount(
                    mask & @as(u16, @bitCast(self.term_positions[current_buffer_idx]))
                    );
                current_buffer_idx += 1;

                if (current_doc_id_idx >= target_doc_id_idx) {
                    return current_buffer_idx;
                }
            }
        }

        while ((current_doc_id_idx < target_doc_id_idx) and (current_buffer_idx < start_simd_idx)) {
            current_doc_id_idx += @popCount(
                mask & @as(u16, @bitCast(self.term_positions[current_buffer_idx]))
                );
            current_buffer_idx += 1;

            if (current_doc_id_idx >= target_doc_id_idx) {
                return current_buffer_idx;
            }
        }

        const current_block = @as([*]@Vector(16, u16), @ptrCast(
            self.term_positions
        ));
        const vec_mask: @Vector(16, u16) = comptime @splat(0b10000000_00000000);

        while (current_doc_id_idx < target_doc_id_idx) {
            while (current_buffer_idx < max_idx) {
                const skip_docs: usize = std.simd.countTrues(
                    vec_mask == (current_block[@divExact(current_buffer_idx, 16)] & vec_mask)
                    );
                std.debug.assert(skip_docs <= 16);

                if (skip_docs + current_doc_id_idx >= target_doc_id_idx) {
                    while (current_doc_id_idx < target_doc_id_idx) {
                        current_doc_id_idx += @popCount(
                            mask & @as(u16, @bitCast(self.term_positions[current_buffer_idx]))
                            );
                        current_buffer_idx += 1;
                    }
                    return current_buffer_idx;
                }

                current_doc_id_idx += skip_docs;
                current_buffer_idx += 16;
            }
            // unreachable;
            return current_buffer_idx;
        }
        unreachable;
    }
};

pub const PostingsList = struct {
    postings: std.ArrayListAlignedUnmanaged(
                  PostingV3,
                  std.mem.Alignment.fromByteUnits(512),
              ),
    arena: std.heap.ArenaAllocator,

    pub fn init(child_allocator: std.mem.Allocator) PostingsList {
        return PostingsList{
            .postings = std.ArrayListAlignedUnmanaged(
                  PostingV3,
                  std.mem.Alignment.fromByteUnits(512),
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
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
        ) !void {
        try self.postings.items[term_id].add(
            self.arena.allocator(), 
            doc_id, 
            scratch_arr,
            );
    }

    pub inline fn append(
        self: *PostingsList, 
        doc_id: u32,
        scratch_arr: *align(512) [BLOCK_SIZE]u32,
        ) !void {
        const val = try self.postings.addOne(self.arena.allocator());
        val.* = PostingV3.init();
        try val.add(self.arena.allocator(), doc_id, scratch_arr);
    }
};

pub const InvertedIndexV2 = struct {
    posting_list: PostingsList,
    vocab: Vocab,
    doc_freqs: std.ArrayListUnmanaged(u32), // Should be needed soon. Visible in posting.
    doc_sizes: []u16, // Consider PFOR encoding w/ blocks.

    // TODO: Remove num_terms and num_docs.
    num_terms: u32,
    num_docs: u32,
    avg_doc_size: f32,

    file_handle: std.fs.File,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        filename: []const u8,
        ) !InvertedIndexV2 {
        var II = InvertedIndexV2{
            .posting_list = PostingsList.init(allocator),
            .vocab = undefined,
            .doc_freqs = std.ArrayListUnmanaged(u32){},
            .doc_sizes = try allocator.alloc(u16, num_docs),
            .num_terms = 0,
            .num_docs = @intCast(num_docs),
            .avg_doc_size = 0.0,
            .file_handle = undefined,
        };
        II.file_handle = try std.fs.cwd().createFile(
             filename, 
             .{ .read = true },
             );

        @memset(II.doc_sizes, 0);

        // Guess capacity.
        II.doc_freqs = try std.ArrayListUnmanaged(u32).initCapacity(
            allocator, 
            @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
            );

        II.vocab = Vocab.init();

        // Guess capacity
        try II.vocab.string_bytes.ensureTotalCapacity(allocator, @intCast(num_docs));
        try II.vocab.map.ensureTotalCapacityContext(allocator, @intCast(num_docs / 25), II.vocab.getCtx());

        return II;
    }

    pub fn deinit(
        self: *InvertedIndexV2, 
        allocator: std.mem.Allocator,
        ) void {

        // allocator.free(self.postings.doc_id_ptrs);
        // allocator.free(self.postings.doc_id_buf);
        // allocator.free(self.postings.term_pos_ptrs);
        // allocator.free(self.postings.term_positions);
        // allocator.free(self.postings.block_doc_id_offsets);
        self.file_handle.close();

        self.posting_list.deinit(allocator);

        self.vocab.deinit(allocator);
        // self.prt_vocab.deinit();

        self.doc_freqs.deinit(allocator);
        // self.term_occurences.deinit(allocator);
        allocator.free(self.doc_sizes);
    }

    pub fn commit(
        self: *InvertedIndexV2,
        allocator: std.mem.Allocator,
    ) !void {
        var buf = std.ArrayListUnmanaged(u8){};
        try buf.resize(allocator, 1 << 22);
        defer buf.deinit(allocator);

        buf.items[0] = 0;

        var current_pos: u64 = 0;
        for (self.posting_list.postings.items) |*pos| {
            try pos.serialize(&buf, allocator, &current_pos, self.num_docs);
        }

        if (current_pos + self.vocab.string_bytes.items.len > buf.items.len) {
            try buf.resize(allocator, buf.items.len + 2 * self.vocab.string_bytes.items.len);
        }
        @memcpy(
            buf.items[current_pos..][0..self.vocab.string_bytes.items.len],
            self.vocab.string_bytes.items,
        );
        current_pos += self.vocab.string_bytes.items.len;

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
                ENDIANESS,
            );
            std.mem.writePackedInt(
                u32,
                buf.items[current_pos..][4..8],
                0,
                item.value_ptr.*,
                ENDIANESS,
            );
            current_pos += 8;
        }
        std.debug.print("\nVocab size: {d}MB for {d} terms\n",
            .{
                @divFloor(
                    self.vocab.string_bytes.items.len + 8 * self.vocab.map.count(),
                    1 << 20,
                ),
                self.vocab.map.count(),
            });

        max_range = current_pos + self.doc_sizes.len * @sizeOf(u16);
        if (max_range > buf.items.len) {
            try buf.resize(
                allocator, 
                max_range,
                );
        }
        @memcpy(
            buf.items[current_pos..][0..(self.doc_sizes.len * @sizeOf(u16))],
            std.mem.sliceAsBytes(self.doc_sizes),
        );
        current_pos += self.doc_sizes.len * @sizeOf(u16);

        try self.file_handle.writeAll(
            buf.items[0..current_pos],
        );
    }

    // pub fn resizePostings(self: *InvertedIndexV2, allocator: std.mem.Allocator) !void {
        // self.num_terms = @intCast(self.doc_freqs.items.len);
        // self.postings.doc_id_ptrs = try allocator.alignedAlloc(
            // flag_u32,
            // comptime std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // self.num_terms + 1,
        // );
        // self.postings.term_pos_ptrs = try allocator.alignedAlloc(
            // flag_u32,
            // comptime std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // self.num_terms + 1,
        // );
// 
        // std.debug.assert(self.num_terms == self.vocab.map.count());
// 
        // // Num terms is now known.
        // var docs_postings_size:  usize = 0;
        // var terms_postings_size: usize = 0;
        // for (0.., self.doc_freqs.items) |idx, doc_freq| {
            // if (doc_freq == 1) {
                // self.postings.doc_id_ptrs[idx] = flag_u32{
                    // .is_inline = 1,
                    // .value = undefined,
                // };
            // } else {
                // self.postings.doc_id_ptrs[idx] = flag_u32{
                    // .is_inline = 0,
                    // .value = @truncate(docs_postings_size),
                // };
                // docs_postings_size += doc_freq;
            // }
// 
            // std.debug.assert(docs_postings_size < (comptime 1 << 31));
// 
            // const num_occurences = self.term_occurences.items[idx];
            // if (num_occurences == 1) {
                // self.postings.term_pos_ptrs[idx] = flag_u32{
                    // .is_inline = 1,
                    // .value = undefined,
                // };
            // } else {
                // self.postings.term_pos_ptrs[idx] = flag_u32{
                    // .is_inline = 0,
                    // .value = @truncate(terms_postings_size),
                // };
                // terms_postings_size += num_occurences;
            // }
        // }
        // self.postings.doc_id_ptrs[self.num_terms] = flag_u32{
            // .is_inline = 0,
            // .value = @truncate(docs_postings_size),
        // };
        // self.postings.term_pos_ptrs[self.num_terms] = flag_u32{
            // .is_inline = 0,
            // .value = @truncate(terms_postings_size),
        // };
// 
        // docs_postings_size  += 1;
        // terms_postings_size += 1;
// 
        // self.postings.doc_id_buf     = try allocator.alignedAlloc(
            // u32, 
            // comptime std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // docs_postings_size,
            // );
        // self.postings.term_positions = try allocator.alignedAlloc(
            // // u16, 
            // TP,
            // comptime std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // terms_postings_size,
            // );
// 
        // const num_blocks = try std.math.divCeil(
            // usize,
            // terms_postings_size,
            // 256,
        // );
        // self.postings.block_doc_id_offsets = try allocator.alignedAlloc(
            // u32,
            // comptime std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // num_blocks,
        // );
// 
        // var avg_doc_size: f64 = 0.0;
        // for (self.doc_sizes) |doc_size| {
            // avg_doc_size += @floatFromInt(doc_size);
        // }
        // avg_doc_size /= @floatFromInt(self.num_docs);
        // self.avg_doc_size = @floatCast(avg_doc_size);
    // }
};

pub const BM25Partition = struct {
    II: []InvertedIndexV2,
    num_records: usize,
    allocator: std.mem.Allocator,

    doc_store: DocStore,

    scratch_arr: [BLOCK_SIZE]u32 align(512),

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        num_records: usize,
        tmp_dir: []const u8,
        partition_idx: usize,
    ) !BM25Partition {
        var partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .num_records = num_records,
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
                .{tmp_dir, idx, partition_idx}
                );
            defer self.allocator.free(output_filename);

            self.II[idx] = try InvertedIndexV2.init(
                self.allocator, 
                self.num_records,
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
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        std.debug.assert(
            self.II[col_idx].vocab.map.count() < (1 << 32),
            );
        std.debug.assert(
            terms_seen.count < MAX_NUM_TERMS
            );

        const gop = try self.II[col_idx].vocab.map.getOrPutContextAdapted(
            self.allocator,
            term[0..term_len],
            self.II[col_idx].vocab.getAdapter(),
            self.II[col_idx].vocab.getCtx(),
            );

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
                &self.scratch_arr,
            );

        } else {

            const val = gop.value_ptr.*;

            if (!terms_seen.checkOrInsertSIMD(val)) {
                self.II[col_idx].doc_freqs.items[val] += 1;

                try self.II[col_idx].posting_list.add(
                    val,
                    doc_id,
                    &self.scratch_arr,
                    );
                }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
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

        terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[col_idx].doc_sizes[doc_id] >= MAX_NUM_TERMS) {
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
                std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

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
            std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

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
        // token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        buffer: []u8,
        doc_id: u32,
        search_col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        string_utils.stringToUpper(
            @ptrCast(buffer[0..]), 
            buffer.len,
            );

        if (buffer.len == 0) {
            // try token_stream.addToken(
                // true,
                // std.math.maxInt(u16),
                // std.math.maxInt(u31),
                // search_col_idx,
            // );
            return;
        }

        var term_pos: u16 = 0;

        var cntr: usize = 0;
        // var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        var buffer_idx: usize = 0;
        while (buffer_idx < buffer.len) {
            std.debug.assert(
                self.II[search_col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS
                );

            if (cntr > MAX_TERM_LENGTH - 4) {
                @branchHint(.cold);
                // try self.flushLargeToken(
                    // buffer[buffer_idx - cntr..], 
                    // &cntr, 
                    // doc_id, 
                    // &term_pos, 
                    // search_col_idx, 
                    // token_stream, 
                    // terms_seen,
                    // &new_doc,
                    // );
                try self.addToken(
                    buffer[buffer_idx - cntr..], 
                    &cntr, 
                    doc_id, 
                    &term_pos, 
                    search_col_idx, 
                    terms_seen,
                    // &new_doc,
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
                        // &new_doc,
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
                self.II[search_col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS
                );

            const start_idx = buffer_idx - @min(buffer_idx, cntr + 1);
            try self.addTerm(
                buffer[start_idx..], 
                cntr, 
                doc_id, 
                term_pos, 
                search_col_idx, 
                terms_seen,
                // &new_doc,
                );
        }

        // if (new_doc) {
            // // No terms found. Add null token.
            // try token_stream.addToken(
                // true,
                // std.math.maxInt(u16),
                // std.math.maxInt(u31),
                // search_col_idx,
            // );
        // }
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        // token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
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

        // Empty check.
        if (
            (buffer[buffer_idx] == ',') 
                or 
            (buffer[buffer_idx] == '}')
            ) {
            // try token_stream.addToken(
                // true,
                // std.math.maxInt(u16),
                // std.math.maxInt(u31),
                // II_idx,
            // );

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
        // var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[II_idx].doc_sizes[doc_id] >= MAX_NUM_TERMS) {
                    buffer_idx = 0;
                    try json._iterFieldJSON(buffer, &buffer_idx);
                    byte_idx.* += buffer_idx;
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);
                    // try self.flushLargeToken(
                        // buffer[buffer_idx - cntr..], 
                        // &cntr, 
                        // doc_id, 
                        // &term_pos, 
                        // II_idx, 
                        // token_stream, 
                        // terms_seen,
                        // &new_doc,
                        // );
                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        terms_seen,
                        // &new_doc,
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
                            // &new_doc,
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
                std.debug.assert(self.II[II_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);
                    // try self.flushLargeToken(
                        // buffer[buffer_idx - cntr..], 
                        // &cntr, 
                        // doc_id, 
                        // &term_pos, 
                        // II_idx, 
                        // terms_seen,
                        // &new_doc,
                        // );
                    try self.addToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        terms_seen,
                        // &new_doc,
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
                            // &new_doc,
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
                            // &new_doc,
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
                            // &new_doc,
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
                            // &new_doc,
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
            std.debug.assert(self.II[II_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

            const start_idx = buffer_idx - @intFromBool(is_quoted) - 
                              @min(buffer_idx - @intFromBool(is_quoted), cntr + 1);
            try self.addTerm(
                buffer[start_idx..],
                cntr,
                doc_id,
                term_pos,
                II_idx,
                terms_seen,
                // &new_doc,
                );
        }

        // if (new_doc) {
            // // No terms found. Add null token.
            // try token_stream.addToken(
                // true,
                // std.math.maxInt(u16),
                // std.math.maxInt(u31),
                // II_idx,
            // );
        // }

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

    // pub fn constructFromTokenStream(
        // self: *BM25Partition,
        // token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        // ) !void {
// 
        // var term_cntr_doc_ids    = try self.allocator.alloc(u32, self.II[0].num_terms);
        // var term_cntr_occurences = try self.allocator.alloc(u32, self.II[0].num_terms);
        // defer self.allocator.free(term_cntr_doc_ids);
        // defer self.allocator.free(term_cntr_occurences);
// 
        // for (0.., self.II) |col_idx, *II| {
            // try II.resizePostings(self.allocator);
// 
            // if (II.num_terms > term_cntr_doc_ids.len) {
                // term_cntr_doc_ids = try self.allocator.realloc(
                    // term_cntr_doc_ids, 
                    // @as(usize, @intCast(II.num_terms)),
                    // );
                // term_cntr_occurences = try self.allocator.realloc(
                    // term_cntr_occurences,
                    // @as(usize, @intCast(II.num_terms)),
                    // );
            // }
            // @memset(term_cntr_doc_ids, 0);
            // @memset(term_cntr_occurences, 0);
// 
            // // Create index.
            // const output_file = &token_stream.output_files[col_idx];
            // try output_file.seekTo(0);
// 
            // const doc_id_tokens   = token_stream.doc_id_tokens[col_idx];
            // const term_pos_tokens = token_stream.term_pos_tokens[col_idx];
// 
            // var bytes_read: usize = 0;
// 
            // var num_tokens: usize = file_utils.TOKEN_STREAM_CAPACITY;
            // var current_doc_id: usize = 0;
// 
            // var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();
// 
            // while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
                // var _num_tokens: [4]u8 = undefined;
                // bytes_read = try output_file.read(std.mem.asBytes(&_num_tokens));
                // std.debug.assert(bytes_read == 4);
// 
                // num_tokens = std.mem.readInt(u32, &_num_tokens, ENDIANESS);
// 
                // bytes_read = try output_file.read(
                    // std.mem.sliceAsBytes(doc_id_tokens[0..num_tokens])
                    // );
                // bytes_read += try output_file.read(
                    // std.mem.sliceAsBytes(term_pos_tokens[0..num_tokens])
                    // );
// 
                // terms_seen_bitset.clear();
// 
                // for (0..num_tokens) |idx| {
                    // if (@as(*u32, @ptrCast(&doc_id_tokens[idx])).* == std.math.maxInt(u32)) {
                        // // Null token.
                        // current_doc_id += 1;
                        // terms_seen_bitset.clear();
                        // continue;
                    // }
// 
                    // const new_doc  = doc_id_tokens[idx].new_doc;
                    // const term_pos = term_pos_tokens[idx];
                    // const term_id  = doc_id_tokens[idx].term_id;
// 
                    // const tp = TP{
                        // .term_pos = @truncate(term_pos),
                        // .new_doc = new_doc,
                    // };
// 
                    // if (new_doc == 1) {
                        // current_doc_id += 1;
                        // terms_seen_bitset.clear();
                    // }
// 
                    // if (!terms_seen_bitset.checkOrInsertSIMD(term_id)) {
// 
                        // const is_inline_doc_id = II.postings.doc_id_ptrs[term_id].is_inline;
                        // if (is_inline_doc_id == 1) {
                            // II.postings.doc_id_ptrs[term_id].value = @truncate(current_doc_id);
                        // } else {
                            // const doc_id_offset = @as(usize, @intCast(@as(u32, @bitCast(II.postings.doc_id_ptrs[term_id])))) + term_cntr_doc_ids[term_id];
                            // std.debug.assert(doc_id_offset < II.postings.doc_id_buf.len);
// 
                            // II.postings.doc_id_buf[doc_id_offset] = @truncate(current_doc_id);
                            // term_cntr_doc_ids[term_id] += 1;
                        // }
                    // }
// 
                    // const is_inline_term_pos = II.postings.term_pos_ptrs[term_id].is_inline;
                    // if (is_inline_term_pos == 1) {
                        // II.postings.term_pos_ptrs[term_id].value = @intCast(term_pos);
                    // } else {
                        // const tp_idx = (
                            // @as(usize, @intCast(@as(u32, @bitCast(II.postings.term_pos_ptrs[term_id])))) + term_cntr_occurences[term_id]
                        // );
                        // if (tp_idx % 256 == 0) {
                            // II.postings.block_doc_id_offsets[@divExact(tp_idx, 256)] = @truncate(current_doc_id);
                        // }
// 
                        // II.postings.term_positions[tp_idx] = tp;
                        // term_cntr_occurences[term_id] += 1;
                    // }
// 
                    // std.debug.assert(current_doc_id <= II.num_docs);
                // }
            // }
            // std.debug.assert(
                // (current_doc_id == II.num_docs - 1)
                    // or
                // (current_doc_id == II.num_docs)
            // );
        // }
    // }
// 
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
