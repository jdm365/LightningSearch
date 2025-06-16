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

pub const PostingsIteratorV2 = struct {
    // doc_ids: []u32,
    // term_positions: []u16,
    postings: PostingsV2,
    current_doc_idx: usize,
    current_term_idx: usize,
    score: usize,
    term_id: u32,
    col_idx: u32,
    consumed: bool,
    single_doc: ?flag_u32,
    single_term: ?flag_u32,

    pub const Result = struct {
        doc_id: u32,
        term_pos: u16,
    };

    pub fn init(
        postings: PostingsV2,
        term_id: u32,
        col_idx: u32,
        score: usize,
        ) PostingsIteratorV2 {

        const start_idx_doc_id = postings.doc_id_ptrs[term_id];
        const start_idx_term   = postings.term_pos_ptrs[term_id];

        const end_idx_doc_id = postings.doc_id_ptrs[term_id + 1];
        const end_idx_term   = postings.term_pos_ptrs[term_id + 1];

        const single_doc_id = if (start_idx_doc_id.is_inline == 1)
            @as(u32, @bitCast(postings.doc_id_ptrs[term_id]))
        else null;
        const single_term   = if (start_idx_term.is_inline == 1)
            @as(u32, @bitCast(postings.term_pos_ptrs[term_id]))
        else null;

        return PostingsIteratorV2{ 
            .doc_ids = postings.doc_ids[start_idx_doc_id..end_idx_doc_id],
            .term_pos_ptrs = postings.term_pos_ptrs[start_idx_term..end_idx_term],
            .term_id = term_id,
            .current_doc_idx = 0,
            .current_term_idx = 0,
            .score = score,
            .col_idx = col_idx,
            .consumed = false,
            .single_doc = single_doc_id,
            .single_term = single_term,
        };
    }

    pub inline fn currentDocId(self: *const PostingsIteratorV2) u32 {
        if (self.single_doc) |d| {
            return @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111;
        }

        if (self.current_doc_idx >= self.postings.doc_ids.len) {
            return std.math.maxInt(u32);
        }
        return self.doc_ids[self.current_idx];
    }

    pub inline fn currentTermPos(self: *const PostingsIteratorV2) u16 {
        if (self.single_term) |t| {
            return @truncate(
                @as(u32, @bitCast(t)) & 0b00000000_00000000_11111111_11111111
            );
        }

        if (self.current_idx >= self.term_positions.len) {
            return std.math.maxInt(u16);
        }

        return self.term_positions[self.current_idx];
    }

    pub inline fn next(self: *PostingsIteratorV2) Result {
        if (self.consumed) {
            return .{
                .doc_id = std.math.maxInt(u32),
                .term_pos = std.math.maxInt(u16),
            };
        }

        if (self.single_doc) |d| {
            if (self.single_term) |t| {
                return .{
                    .doc_id = @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111,
                    .term_pos = @truncate(@as(u32, @bitCast(t)) & 0b00000000_00000000_11111111_11111111),
                };
            } else {
                const term_pos = self.term_positions[self.current_term_idx];
                self.current_term_idx += 1;
                return .{
                    .doc_id = @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111,
                    .term_pos = term_pos,
                };
            }
        }

        self.current_doc_idx += 1;

        if (self.current_idx >= self.doc_ids.len) {
            self.consumed = true;
            return .{
                .doc_id = std.math.maxInt(u32),
                .term_pos = std.math.maxInt(u16),
            };
        }

        self.current_term_idx = self.postings.nextTermPos(self.current_term_idx);

        return .{
            .doc_id = self.doc_ids[self.current_doc_idx],
            .term_pos = self.term_positions[self.current_term_idx],
        };
    }

    pub inline fn advanceTo(self: *PostingsIteratorV2, target_id: u32) Result {
        if (self.current_idx >= self.doc_ids.len) {
            return .{
                .doc_id = std.math.maxInt(u32), 
                .term_pos = std.math.maxInt(u16),
            };
        }

        if (self.single_doc) |d| {
            return .{
                .doc_id = @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111,
                .term_pos = @truncate(
                    @as(u32, @bitCast(self.single_term.?)) & 0b00000000_00000000_01111111_11111111
                ),
            };
        }

        if (self.doc_ids[self.current_idx] >= target_id) {
            return .{
                .doc_id   = self.doc_ids[self.current_idx],
                .term_pos = self.term_positions[self.current_idx],
            };
        }

        const found_idx_in_slice = linearLowerBound(
            self.doc_ids[self.current_idx..],
            target_id,
        );

        self.current_idx += found_idx_in_slice;

        if (self.current_idx >= self.doc_ids.len) {
            self.consumed = true;
            return .{
                .doc_id = std.math.maxInt(u32), 
                .term_pos = std.math.maxInt(u16),
            };
        }

        return .{
            .doc_id = self.doc_ids[self.current_idx], 
            .term_pos = self.term_positions[self.current_idx],
        };
    }

    pub inline fn len(self: *PostingsIterator) usize {
        if (self.single_doc) |_| return 1;
        return self.doc_ids.len;
    }
};


pub const PostingsIterator = struct {
    doc_ids: []u32,
    term_positions: []u16,
    current_idx: usize,
    score: usize,
    term_id: u32,
    col_idx: u32,
    consumed: bool,
    single_doc: ?flag_u32,
    single_term: ?flag_u32,

    pub const Result = struct {
        doc_id: u32,
        term_pos: u16,
    };

    pub fn init(
        doc_ids: []u32, 
        term_positions: []u16, 
        term_id: u32,
        col_idx: u32,
        score: usize,
        single_doc_id: ?flag_u32,
        single_term: ?flag_u32,
        ) PostingsIterator {
        return PostingsIterator{ 
            .doc_ids = doc_ids,
            .term_positions = term_positions,
            .term_id = term_id,
            .current_idx = 0,
            .score = score,
            .col_idx = col_idx,
            .consumed = false,
            .single_doc = single_doc_id,
            .single_term = single_term,
        };
    }

    pub inline fn currentDocId(self: *const PostingsIterator) u32 {
        if (self.single_doc) |d| {
            return @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111;
        }

        if (self.current_idx >= self.doc_ids.len) {
            return std.math.maxInt(u32);
        }
        return self.doc_ids[self.current_idx];
    }

    pub inline fn currentTermPos(self: *const PostingsIterator) u16 {
        if (self.single_term) |t| {
            return @truncate(
                @as(u32, @bitCast(t)) & 0b00000000_00000000_11111111_11111111
            );
        }

        if (self.current_idx >= self.term_positions.len) {
            return std.math.maxInt(u16);
        }

        return self.term_positions[self.current_idx];
    }

    pub inline fn next(self: *PostingsIterator) Result {
        if (self.consumed) {
            return .{
                .doc_id = std.math.maxInt(u32),
                .term_pos = std.math.maxInt(u16),
            };
        }
        if (self.single_doc) |d| {
            return .{
                .doc_id = @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111,
                .term_pos = @truncate(@as(u32, @bitCast(self.single_term.?)) & 0b00000000_00000000_11111111_11111111),
            };
        }

        self.current_idx += 1;

        if (self.current_idx >= self.doc_ids.len) {
            self.consumed = true;
            return .{
                .doc_id = std.math.maxInt(u32),
                .term_pos = std.math.maxInt(u16),
            };
        }

        return .{
            .doc_id = self.doc_ids[self.current_idx],
            .term_pos = self.term_positions[self.current_idx],
        };
    }

    pub inline fn advanceTo(self: *PostingsIterator, target_id: u32) Result {
        if (self.current_idx >= self.doc_ids.len) {
            return .{
                .doc_id = std.math.maxInt(u32), 
                .term_pos = std.math.maxInt(u16),
            };
        }

        if (self.single_doc) |d| {
            return .{
                .doc_id = @as(u32, @bitCast(d)) & 0b01111111_11111111_11111111_11111111,
                .term_pos = @truncate(
                    @as(u32, @bitCast(self.single_term.?)) & 0b00000000_00000000_01111111_11111111
                ),
            };
        }

        if (self.doc_ids[self.current_idx] >= target_id) {
            return .{
                .doc_id   = self.doc_ids[self.current_idx],
                .term_pos = self.term_positions[self.current_idx],
            };
        }

        const found_idx_in_slice = linearLowerBound(
            self.doc_ids[self.current_idx..],
            target_id,
        );

        self.current_idx += found_idx_in_slice;

        if (self.current_idx >= self.doc_ids.len) {
            self.consumed = true;
            return .{
                .doc_id = std.math.maxInt(u32), 
                .term_pos = std.math.maxInt(u16),
            };
        }

        return .{
            .doc_id = self.doc_ids[self.current_idx], 
            .term_pos = self.term_positions[self.current_idx],
        };
    }

    pub inline fn len(self: *PostingsIterator) usize {
        if (self.single_doc) |_| return 1;
        return self.doc_ids.len;
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

    // pub fn eql(self: SliceAdapter, a_slice: []u8, b: u32) bool {
        // const b_slice = std.mem.span(
            // @as([*:0]u8, @ptrCast(&self.string_bytes.items[b])),
            // );
        // return std.mem.eql(u8, a_slice, b_slice);
    // }
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

pub const TP = packed struct(u16) {
    term_pos: u15,
    new_doc: u1,
};

pub const TermPosBlock = struct {
    // 512 bytes.
    // Leading bit of term_positions is `new_doc` flag.
    current_doc_id: u32,
    term_positions: [254]TP,
};

pub const PostingsV2 = struct {
    doc_id_ptrs:  []align(std.heap.page_size_min)flag_u32,
    doc_id_buf: []align(std.heap.page_size_min)u32,

    term_pos_ptrs:  []align(std.heap.page_size_min)flag_u32,
    term_positions: []align(std.heap.page_size_min)TP,
    // term_positions: []align(std.heap.page_size_min)u16,
    // term_positions: []align(std.heap.page_size_min)TermPosBlock,

    block_doc_id_offsets: []align(std.heap.page_size_min)u32,

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
        var val = self.term_positions[idx];
        while (0b10000000_00000000 & val > 0) {
            idx += 1;
            val = self.term_positions[idx];
        }
        return idx;
    }

    pub inline fn advanceToBlock(
        self: *const PostingsV2, 
        doc_id: usize, 
        start_idx: usize,
        ) !usize {
        // Return first position where the doc_id is greater than or equal to the given doc_id.
        
        // TODO: Handle boundary condition.
        const start_block_idx  = @divFloor(start_idx, 256);

        var current_buffer_idx = start_idx % 256;
        var current_block_idx  = @divFloor(start_idx, 256);

        var current_doc_id = self.block_doc_id_offsets[current_block_idx];
        var next_doc_id    = self.block_doc_id_offsets[current_block_idx + 1];

        while (next_doc_id < doc_id) {
            current_block_idx += 1;
            current_buffer_idx = 0;

            current_doc_id = next_doc_id;
            next_doc_id = self.block_doc_id_offsets[current_block_idx + 1];
        }

        const current_block = @as([*]@Vector(16, u16), @ptrCast(
            self.term_positions
        ));
        const mask: @Vector(16, u16) = comptime @splat(0b10000000_00000000);

        while (current_doc_id < doc_id) {
            while (current_buffer_idx < 256) {
                const skip_docs = @popCount(
                    current_block[@divExact(current_buffer_idx, 16)].* & mask
                    );

                if (skip_docs + current_doc_id >= doc_id) {
                    while (current_doc_id < doc_id) {
                        current_doc_id += @popCount(
                            0b10000000_00000000 & self.term_positions[current_buffer_idx]
                            );
                        current_buffer_idx += 1;

                        if (current_doc_id >= doc_id) {
                            return current_buffer_idx + 256 * (current_block_idx - start_block_idx);
                        }
                    }
                    unreachable;
                }

                current_doc_id += skip_docs;
                current_buffer_idx += 16;
            }
        }
        unreachable;
    }
};


pub const InvertedIndexV2 = struct {
    postings: PostingsV2,
    vocab: Vocab,
    prt_vocab: RadixTrie(u32),
    doc_freqs: std.ArrayListUnmanaged(u32),
    term_occurences: std.ArrayListUnmanaged(u32),
    doc_sizes: []u16,

    // TODO: Remove num_terms and num_docs.
    num_terms: u32,
    num_docs: u32,
    avg_doc_size: f32,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        ) !InvertedIndexV2 {
        var II = InvertedIndexV2{
            .postings = PostingsV2{
                .doc_id_ptrs = undefined,
                .doc_id_buf  = undefined,

                .term_pos_ptrs  = undefined,
                .term_positions = undefined,
            },
            .vocab = undefined,
            .prt_vocab = try RadixTrie(u32).init(allocator),
            .doc_freqs = std.ArrayListUnmanaged(u32){},
            .term_occurences = std.ArrayListUnmanaged(u32){},
            .doc_sizes = try allocator.alloc(u16, num_docs),
            .num_terms = 0,
            .num_docs = @intCast(num_docs),
            .avg_doc_size = 0.0,
        };
        @memset(II.doc_sizes, 0);

        // Guess capacity.
        II.doc_freqs = try std.ArrayListUnmanaged(u32).initCapacity(
            allocator, 
            @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
            );
        II.term_occurences = try std.ArrayListUnmanaged(u32).initCapacity(
            allocator, 
            @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
            );

        II.vocab = Vocab.init();

        // Guess capacity
        try II.vocab.string_bytes.ensureTotalCapacity(allocator, @intCast(num_docs));
        try II.vocab.map.ensureTotalCapacityContext(allocator, @intCast(num_docs / 25), II.vocab.getCtx());

        return II;
    }

    pub fn deinit(self: *InvertedIndexV2, allocator: std.mem.Allocator) void {

        allocator.free(self.postings.doc_id_ptrs);
        allocator.free(self.postings.doc_id_buf);
        allocator.free(self.postings.term_pos_ptrs);
        allocator.free(self.postings.term_positions);

        self.vocab.deinit(allocator);
        self.prt_vocab.deinit();

        self.doc_freqs.deinit(allocator);
        self.term_occurences.deinit(allocator);
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(self: *InvertedIndexV2, allocator: std.mem.Allocator) !void {
        self.num_terms = @intCast(self.doc_freqs.items.len);
        self.postings.doc_id_ptrs = try allocator.alignedAlloc(
            flag_u32,
            std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            self.num_terms + 1,
        );
        self.postings.term_pos_ptrs = try allocator.alignedAlloc(
            flag_u32,
            std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            self.num_terms + 1,
        );

        std.debug.assert(self.num_terms == self.vocab.map.count());

        // Num terms is now known.
        var docs_postings_size:  usize = 0;
        var terms_postings_size: usize = 0;
        for (0.., self.doc_freqs.items) |idx, doc_freq| {
            if (doc_freq == 1) {
                self.postings.doc_id_ptrs[idx] = flag_u32{
                    .is_inline = 1,
                    .value = undefined,
                };
            } else {
                self.postings.doc_id_ptrs[idx] = flag_u32{
                    .is_inline = 0,
                    .value = @truncate(docs_postings_size),
                };
                docs_postings_size += doc_freq;
            }

            std.debug.assert(docs_postings_size < (comptime 1 << 31));

            const num_occurences = self.term_occurences.items[idx];
            if (num_occurences == 1) {
                self.postings.term_pos_ptrs[idx] = flag_u32{
                    .is_inline = 1,
                    .value = undefined,
                };
            } else {
                self.postings.term_pos_ptrs[idx] = flag_u32{
                    .is_inline = 0,
                    .value = @truncate(terms_postings_size),
                };
                terms_postings_size += num_occurences;
            }
        }
        self.postings.doc_id_ptrs[self.num_terms] = flag_u32{
            .is_inline = 0,
            .value = @truncate(docs_postings_size),
        };
        self.postings.term_pos_ptrs[self.num_terms] = flag_u32{
            .is_inline = 0,
            .value = @truncate(terms_postings_size),
        };

        docs_postings_size  += 1;
        terms_postings_size += 1;

        self.postings.doc_id_buf     = try allocator.alignedAlloc(
            u32, 
            std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            docs_postings_size,
            );

        // 254 blocks tp's per block.
        const blocks_needed = try std.math.divCeil(
            usize,
            terms_postings_size,
            254,
        );
        self.postings.term_positions = try allocator.alignedAlloc(
            // u16, 
            TermPosBlock,
            std.mem.Alignment.fromByteUnits(std.heap.page_size_min),
            // terms_postings_size,
            blocks_needed,
            );

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.num_docs);
        self.avg_doc_size = @floatCast(avg_doc_size);
    }
};

pub const BM25Partition = struct {
    II: []InvertedIndexV2,
    num_records: usize,
    allocator: std.mem.Allocator,

    doc_store: DocStore,

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        num_records: usize,
    ) !BM25Partition {
        var partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .num_records = num_records,
            .allocator = allocator,
            .doc_store = undefined,
        };

        for (0..num_search_cols) |idx| {
            partition.II[idx] = try InvertedIndexV2.init(
                allocator, 
                num_records,
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

    pub fn resizeNumSearchCols(self: *BM25Partition, num_search_cols: usize) !void {
        const current_length = self.II.len;
        if (num_search_cols <= current_length) return;

        self.II = try self.allocator.realloc(self.II, num_search_cols);
        for (current_length..num_search_cols) |idx| {
            self.II[idx] = try InvertedIndexV2.init(
                self.allocator, 
                self.num_records,
                );
        }
    }

    inline fn addTerm(
        self: *BM25Partition,
        term: []u8,
        term_len: usize,
        doc_id: u32,
        term_pos: u16,
        col_idx: usize,
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
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
            gop.value_ptr.* = self.II[col_idx].num_terms;

            self.II[col_idx].num_terms += 1;
            try self.II[col_idx].doc_freqs.append(self.allocator, 1);
            try self.II[col_idx].term_occurences.append(self.allocator, 1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);

        } else {

            const val = gop.value_ptr.*;

            self.II[col_idx].term_occurences.items[val] += 1;
            try token_stream.addToken(new_doc.*, term_pos, val, col_idx);

            if (!terms_seen.checkOrInsertSIMD(val)) {
                self.II[col_idx].doc_freqs.items[val] += 1;
            }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
        new_doc.* = false;
    }

    inline fn addToken(
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
        if (cntr.* == 0) {
            return;
        }

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
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        buffer: []u8,
        byte_idx: *usize,
        doc_id: u32,
        col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        var buffer_idx: usize = byte_idx.*;

        if (
            (buffer[buffer_idx] == ',') 
                or 
            (buffer[buffer_idx] == '\n')
            ) {
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                col_idx,
            );
            byte_idx.* += 1;
            return;
        }

        var term_pos: u16 = 0;
        const is_quoted = (buffer[buffer_idx] == '"');
        buffer_idx += @intFromBool(is_quoted);

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[col_idx].doc_sizes[doc_id] >= MAX_NUM_TERMS) {
                    csv._iterFieldCSV(buffer, byte_idx);
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
                    @branchHint(.cold);

                    try self.flushLargeToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
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
                                        token_stream, 
                                        terms_seen,
                                        &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                    try self.flushLargeToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                token_stream, 
                terms_seen,
                &new_doc,
                );
        }

        if (new_doc) {
            // No terms found. Add null token.
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                col_idx,
            );
        }

        byte_idx.* = buffer_idx;
    }


    pub fn processDocVbyte(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
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
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                search_col_idx,
            );
            return;
        }

        var term_pos: u16 = 0;

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        var buffer_idx: usize = 0;
        while (buffer_idx < buffer.len) {
            std.debug.assert(
                self.II[search_col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS
                );

            if (cntr > MAX_TERM_LENGTH - 4) {
                @branchHint(.cold);
                try self.flushLargeToken(
                    buffer[buffer_idx - cntr..], 
                    &cntr, 
                    doc_id, 
                    &term_pos, 
                    search_col_idx, 
                    token_stream, 
                    terms_seen,
                    &new_doc,
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
                        token_stream, 
                        terms_seen,
                        &new_doc,
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
                token_stream, 
                terms_seen,
                &new_doc,
                );
        }

        if (new_doc) {
            // No terms found. Add null token.
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                search_col_idx,
            );
        }
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
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
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                II_idx,
            );

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
        var new_doc: bool = (doc_id != 0);

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
                    try self.flushLargeToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                    try self.flushLargeToken(
                        buffer[buffer_idx - cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        II_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                            token_stream, 
                            terms_seen,
                            &new_doc,
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
                token_stream,
                terms_seen,
                &new_doc,
                );
        }

        if (new_doc) {
            // No terms found. Add null token.
            try token_stream.addToken(
                true,
                std.math.maxInt(u16),
                std.math.maxInt(u31),
                II_idx,
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

    pub fn constructFromTokenStream(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStreamV2(file_utils.token_32t_v2),
        ) !void {

        var term_cntr_doc_ids    = try self.allocator.alloc(u32, self.II[0].num_terms);
        var term_cntr_occurences = try self.allocator.alloc(u32, self.II[0].num_terms);
        defer self.allocator.free(term_cntr_doc_ids);
        defer self.allocator.free(term_cntr_occurences);

        for (0.., self.II) |col_idx, *II| {
            try II.resizePostings(self.allocator);

            if (II.num_terms > term_cntr_doc_ids.len) {
                term_cntr_doc_ids = try self.allocator.realloc(
                    term_cntr_doc_ids, 
                    @as(usize, @intCast(II.num_terms)),
                    );
                term_cntr_occurences = try self.allocator.realloc(
                    term_cntr_occurences,
                    @as(usize, @intCast(II.num_terms)),
                    );
            }
            @memset(term_cntr_doc_ids, 0);
            @memset(term_cntr_occurences, 0);

            // Create index.
            const output_file = &token_stream.output_files[col_idx];
            try output_file.seekTo(0);

            const doc_id_tokens   = token_stream.doc_id_tokens[col_idx];
            const term_pos_tokens = token_stream.term_pos_tokens[col_idx];

            var bytes_read: usize = 0;

            var num_tokens: usize = file_utils.TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

            while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
                var _num_tokens: [4]u8 = undefined;
                bytes_read = try output_file.read(std.mem.asBytes(&_num_tokens));
                std.debug.assert(bytes_read == 4);

                num_tokens = std.mem.readInt(u32, &_num_tokens, ENDIANESS);

                bytes_read = try output_file.read(
                    std.mem.sliceAsBytes(doc_id_tokens[0..num_tokens])
                    );
                bytes_read += try output_file.read(
                    std.mem.sliceAsBytes(term_pos_tokens[0..num_tokens])
                    );

                terms_seen_bitset.clear();

                for (0..num_tokens) |idx| {
                    if (@as(*u32, @ptrCast(&doc_id_tokens[idx])).* == std.math.maxInt(u32)) {
                        // Null token.
                        current_doc_id += 1;
                        terms_seen_bitset.clear();
                        continue;
                    }

                    const new_doc  = doc_id_tokens[idx].new_doc;
                    const term_pos = term_pos_tokens[idx];
                    const term_id  = doc_id_tokens[idx].term_id;

                    const tp = TP{
                        .term_pos = @truncate(term_pos),
                        .new_doc = new_doc,
                    };

                    if (new_doc == 1) {
                        current_doc_id += 1;
                        terms_seen_bitset.clear();
                    }

                    if (!terms_seen_bitset.checkOrInsertSIMD(term_id)) {

                        const is_inline_doc_id = II.postings.doc_id_ptrs[term_id].is_inline;
                        if (is_inline_doc_id == 1) {
                            II.postings.doc_id_ptrs[term_id].value = @truncate(current_doc_id);
                        } else {
                            const doc_id_offset = @as(usize, @intCast(@as(u32, @bitCast(II.postings.doc_id_ptrs[term_id])))) + term_cntr_doc_ids[term_id];
                            std.debug.assert(doc_id_offset < II.postings.doc_id_buf.len);

                            II.postings.doc_id_buf[doc_id_offset] = @truncate(current_doc_id);
                            term_cntr_doc_ids[term_id] += 1;
                        }
                    }

                    const is_inline_term_pos = II.postings.term_pos_ptrs[term_id].is_inline;
                    if (is_inline_term_pos == 1) {
                        II.postings.term_pos_ptrs[term_id].value = @intCast(term_pos);
                    } else {
                        const tp_idx = (
                            @as(usize, @intCast(@as(u32, @bitCast(II.postings.term_pos_ptrs[term_id])))) + term_cntr_occurences[term_id]
                        );
                        if (tp_idx % 256 == 0) {
                            II.postings.block_doc_id_offsets[@divExact(tp_idx, 256)] = current_doc_id;
                        }

                        II.postings.term_positions[tp_idx] = tp;
                        term_cntr_occurences[term_id] += 1;
                    }

                    std.debug.assert(current_doc_id <= II.num_docs);
                }
            }
            std.debug.assert(
                (current_doc_id == II.num_docs - 1)
                    or
                (current_doc_id == II.num_docs)
            );
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
