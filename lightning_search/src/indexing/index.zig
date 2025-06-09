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


pub fn CategoricalColumn(comptime T: type) type {
    return struct {
        const Self = @This();

        is_literal_type: bool,
        col_idx: u32,
        total_count: u64,

        value_map_u8: std.AutoArrayHashMapUnmanaged(T, u8),
        values_literal: std.ArrayListUnmanaged(u8),

        value_idx_map: std.AutoHashMapUnmanaged(T, std.ArrayListUnmanaged(u8)),


        pub fn init(col_idx: u32) !Self {
            return Self{
                .is_literal_type = true,
                .col_idx = col_idx,
                .total_count = 0,

                .value_map_u8 = std.AutoArrayHashMapUnmanaged(T, u8){},
                .values_literal = std.ArrayListUnmanaged(u8){},

                .value_idx_map = std.AutoHashMapUnmanaged(
                    T, 
                    std.ArrayListUnmanaged(u8),
                    ),

            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            if (self.is_literal_type) {
                self.value_map_u8.deinit(allocator);
                self.value_idx_map.deinit(allocator);
            } else {
                while (self.value_idx_map.iterator()) |item| {
                    item.value_ptr.*.deinit(allocator);
                }
                self.value_idx_map.deinit(allocator);
            }
        }

        pub inline fn add(
            self: *Self, 
            allocator: std.mem.Allocator, 
            value: T,
            ) !void {
            if (self.is_literal_type) {
                var gop = try self.value_map_u8.getOrPut(value);
                if (!gop.found_existing) {
                    gop.key_ptr.*   = value;
                    gop.value_ptr.* = @truncate(self.value_map_u8.count());
                }
                try self.values_literal.append(allocator, gop.value_ptr.*);

                if (self.value_map_u8.count() == 255) {
                    self.is_literal_type = false;
                    self.total_count = self.values_literal.items.len;

                    // Transfer to value_idx_map.
                    for (0.., try self.value_map_u8.iterator()) |idx, T_val| {
                        try self.value_idx_map.put(
                            T_val,
                            std.ArrayListUnmanaged(u32),
                        );
                        for (0.., self.values_literal) |jdx, map_val| {
                            if (map_val == @as(T, @intCast(idx))) {
                                try self.value_idx_map.get(
                                    T_val,
                                ).?.append(allocator, jdx);
                            }
                        }
                    }
                    gop = try self.value_idx_map.getOrPut(
                        self.value_map_u8.items[value].key,
                        );
                    if (!gop.found_existing) {
                        gop.key_ptr.* = self.value_map_u8.items[value].key;
                        try gop.value_ptr.*.append(allocator, @truncate(self.value_idx_map.count()));
                    }

                    self.values_literal.deinit(allocator);
                    self.value_map_u8.deinit(allocator);
                }
            } else {
                try self.value_idx_map.getPtr(
                    value,
                ).append(allocator, self.total_count);
                self.total_count += 1;
            }
        }
    };
}

// inline fn binarySearch(
    // comptime T: type,
    // arr: []T,
    // value: T,
    // ) usize {
    // var low: usize = 0;
    // var high: usize = arr.len;
// 
    // while (low < high) {
        // const mid = low + (high - low) / 2;
// 
        // if (arr[mid] == value) return mid;
// 
        // if (arr[mid] > value) {
            // low = mid + 1;
        // } else {
            // high = mid;
        // }
    // }
    // return low;
// }
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

        existing_values = @ptrFromInt(@intFromPtr(slice.ptr) + 32);
        idx += 8;
    }
    return slice.len;
}

pub fn PostingsIterator(comptime T: type, comptime term_pos_T: type) type {
    return struct {
        const Self = @This();

        doc_ids: []T,
        term_positions: []term_pos_T,
        current_idx: usize,
        score: usize,
        term_id: u32,
        col_idx: u32,
        query_term_pos: term_pos_T,
        consumed: bool,

        pub const Result = struct {
            doc_id: T,
            term_pos: term_pos_T,
        };

        pub fn init(
            doc_ids: []T, 
            term_positions: []term_pos_T, 
            term_id: u32,
            col_idx: u32,
            query_term_pos: term_pos_T,
            score: usize,
            ) Self {
            return Self{ 
                .doc_ids = doc_ids,
                .term_positions = term_positions,
                .term_id = term_id,
                .current_idx = 0,
                .score = score,
                .col_idx = col_idx,
                .query_term_pos = query_term_pos,
                .consumed = false,
            };
        }

        pub inline fn currentDocId(self: *const Self) T {
            if (self.current_idx >= self.doc_ids.len) {
                return std.math.maxInt(T);
            }
            return self.doc_ids[self.current_idx];
        }

        pub inline fn currentTermPos(self: *const Self) term_pos_T {
            if (self.current_idx >= self.term_positions.len) {
                return std.math.maxInt(term_pos_T);
            }
            return self.term_positions[self.current_idx];
        }

        pub inline fn next(self: *Self) Result {
            if (self.consumed) {
                return .{
                    .doc_id = std.math.maxInt(T), 
                    .term_pos = std.math.maxInt(term_pos_T),
                };
            }

            if (self.current_idx < self.doc_ids.len) {
                self.current_idx += 1;
                return .{
                    .doc_id = self.doc_ids[self.current_idx - 1],
                    .term_pos = self.term_positions[self.current_idx - 1],
                };
            }
            self.consumed = true;
            return .{
                .doc_id = std.math.maxInt(T), 
                .term_pos = std.math.maxInt(term_pos_T),
            };
        }

        pub inline fn advanceTo(self: *Self, target_id: T) Result {
            if (self.current_idx >= self.doc_ids.len) {
                return .{
                    .doc_id = std.math.maxInt(T), 
                    .term_pos = std.math.maxInt(term_pos_T),
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
            // if (found_idx_in_slice > 1) {
                // std.debug.print(
                    // "Found idx in slice: {d} {any} {d}\n", 
                    // .{
                        // found_idx_in_slice, 
                        // self.doc_ids[self.current_idx..][0..32],
                        // target_id,
                    // },
                    // );
            // }

            self.current_idx += found_idx_in_slice;

            if (self.current_idx >= self.doc_ids.len) {
                self.consumed = true;
                return .{
                    .doc_id = std.math.maxInt(T), 
                    .term_pos = std.math.maxInt(term_pos_T),
                };
            }

            return .{
                .doc_id = self.doc_ids[self.current_idx], 
                .term_pos = self.term_positions[self.current_idx],
            };
        }

        // pub inline fn advanceTo(self: *Self, next_item: T) Result {
            // if (self.consumed) {
                // return .{
                    // .doc_id = std.math.maxInt(T), 
                    // .term_pos = std.math.maxInt(term_pos_T),
                // };
            // }
// 
            // if (self.current_idx == self.doc_ids.len) {
                // self.consumed = true;
            // }
// 
            // self.current_idx += lowerBound(
                // T,
                // self.doc_ids[self.current_idx..],
                // next_item,
            // );
            // if (self.current_idx == self.doc_ids.len) {
                // self.consumed = true;
                // return .{
                    // .doc_id = std.math.maxInt(T), 
                    // .term_pos = std.math.maxInt(term_pos_T),
                // };
            // }
// 
            // return .{
                // .doc_id = self.doc_ids[self.current_idx],
                // .term_pos = self.term_positions[self.current_idx],
            // };
        // }

        pub inline fn len(self: *Self) usize {
            return self.doc_ids.len;
        }
    };
}

pub fn PostingsIteratorMinHeap(comptime IteratorPtrType: type) type {
    return struct {
        const Self = @This();

        fn compareIterators(
            _: void, 
            a: IteratorPtrType, 
            b: IteratorPtrType
            ) std.math.Order {
            return std.math.order(a.currentDocId(), b.currentDocId());
        }

        pq: std.PriorityQueue(IteratorPtrType, void, compareIterators),

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .pq = std.PriorityQueue(
                    IteratorPtrType,
                    void,
                    compareIterators,
                ).init(
                    allocator,
                    {},
                ),
            };
        }

        pub fn deinit(self: *Self) void {
            self.pq.deinit();
        }

        pub inline fn push(self: *Self, iterator: IteratorPtrType) !void {
            try self.pq.add(iterator);
        }

        pub inline fn pop(self: *Self) ?IteratorPtrType {
            if (self.pq.count() == 0) {
                return null;
            }
            return self.pq.remove();
        }

        pub inline fn peek(self: *const Self) ?IteratorPtrType {
            if (self.pq.count() == 0) {
                return null;
            }
            return self.pq.items[0];
        }

        pub inline fn peekMid(self: *const Self) ?IteratorPtrType {
            if (self.pq.count() == 0) {
                return null;
            }
            const idx = @divFloor(self.pq.items.len, 2);
            return self.pq.items[idx];
        }

        pub inline fn peekThreshold(
            self: *const Self, 
            min_score: usize,
            ) ?IteratorPtrType {
            if (self.pq.count() == 0) {
                return null;
            }
            var pq_idx: usize = 0;
            var score: usize = 0;
            while (score < min_score) {
                score = self.pq.items[pq_idx].score;
                pq_idx += 1;
            }
            return self.pq.items[pq_idx];
        }

        pub inline fn len(self: *const Self) usize {
            return self.pq.count();
        }

        pub inline fn is_empty(self: *const Self) bool {
            return self.pq.count() == 0;
        }
    };
}

pub const HashNGram = extern struct {
    hashes: [8]u16,
};

pub const ScoringInfo = packed struct {
    score: f32,
    term_pos: u8,

    pub fn init() ScoringInfo {
        return .{
            .score = 0.0,
            .term_pos = 0,
        };
    }
};

pub const QueryResult = packed struct(u64){
    doc_id: u32,
    partition_idx: u32,
};

// pub const SHM = std.HashMap([]const u8, u32, std.hash.XxHash64, 80);
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
    term_positions: []u8,
};

pub const PostingsV2 = struct {
    doc_id_buf: []align(std.heap.page_size_min)u32,
    term_positions: []align(std.heap.page_size_min)u8,
};

pub const PostingsDynamic = struct {
    doc_ids: std.ArrayListUnmanaged(std.ArrayListUnmanaged(u32)),
    term_positions: std.ArrayListUnmanaged(std.ArrayListUnmanaged(u8)),
};


pub const InvertedIndexV2 = struct {
    postings: Postings,
    vocab: Vocab,
    prt_vocab: RadixTrie(u32),
    term_offsets: []usize,
    doc_freqs: std.ArrayList(u32),
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
            .postings = Postings{
                .doc_ids = undefined,
                .term_positions = undefined,
            },
            .vocab = undefined,
            .prt_vocab = try RadixTrie(u32).init(allocator),
            .term_offsets = &[_]usize{},
            .doc_freqs = try std.ArrayList(u32).initCapacity(
                allocator, @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
                ),
            .doc_sizes = try allocator.alloc(u16, num_docs),
            .num_terms = 0,
            .num_docs = @intCast(num_docs),
            .avg_doc_size = 0.0,
        };
        @memset(II.doc_sizes, 0);

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
        allocator.free(self.postings.term_positions);
        allocator.free(self.postings.doc_ids);

        self.vocab.deinit(allocator);
        self.prt_vocab.deinit();

        allocator.free(self.term_offsets);
        self.doc_freqs.deinit();
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(
        self: *InvertedIndexV2,
        allocator: std.mem.Allocator,
        ) !void {
        self.num_terms = @intCast(self.doc_freqs.items.len);
        self.term_offsets = try allocator.alloc(usize, self.num_terms + 1);

        std.debug.assert(self.num_terms == self.vocab.map.count());

        // Num terms is now known.
        var postings_size: usize = 0;
        for (0.., self.doc_freqs.items) |i, doc_freq| {
            self.term_offsets[i] = postings_size;
            postings_size += doc_freq;
        }
        self.term_offsets[self.num_terms] = postings_size;

        self.postings.doc_ids = try allocator.alloc(u32, postings_size + 1);
        self.postings.term_positions = try allocator.alloc(u8, postings_size + 1);

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.num_docs);
        self.avg_doc_size = @floatCast(avg_doc_size);

        // try self.buildPRT();
    }

    // pub fn buildPRT(self: *InvertedIndexV2) !void {
        // const start = std.time.milliTimestamp();
        // var vocab_iterator = self.vocab.iterator();
        // while (vocab_iterator.next()) |val| {
            // const df = @as(f32, @floatFromInt(self.doc_freqs.items[val.value_ptr.*]));
            // if (df >= 5.0) {
                // // try self.prt_vocab.insert(
                    // // val.key_ptr.*, 
                    // // val.value_ptr.*, 
                    // // df,
                    // // );
                // try self.prt_vocab.insert(
                    // val.key_ptr.*, 
                    // val.value_ptr.*, 
                    // // df,
                    // );
            // }
        // }
        // const end = std.time.milliTimestamp();
        // std.debug.print("Build PRT in {}ms\n", .{end - start});
    // }
};

pub const BM25Partition = struct {
    II: []InvertedIndexV2,
    num_records: usize,
    allocator: std.mem.Allocator,
    doc_score_map: std.AutoHashMap(u32, ScoringInfo),
    doc_score_sa: [2]SortedIntMultiArray(ScoringInfo, false),

    scratch_array: SortedIntMultiArray(ScoringInfo, false),

    doc_store: DocStore,

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        num_records: usize,
    ) !BM25Partition {
        var doc_score_map = std.AutoHashMap(u32, ScoringInfo).init(allocator);
        try doc_score_map.ensureTotalCapacity(50_000);

        var partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .num_records = num_records,
            .allocator = allocator,
            .doc_score_map = doc_score_map,
            .doc_score_sa = undefined,
            .scratch_array = try SortedIntMultiArray(ScoringInfo, false).init(
                allocator,
                1_048_576,
            ),

            .doc_store = undefined,
        };
        partition.doc_score_sa[0] = try SortedIntMultiArray(ScoringInfo, false).init(
            allocator,
            1_048_576,
        );
        partition.doc_score_sa[1] = try SortedIntMultiArray(ScoringInfo, false).init(
            allocator,
            1_048_576,
        );

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
        self.doc_score_map.deinit();

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
        term_pos: u8,
        col_idx: usize,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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
            try self.II[col_idx].doc_freqs.append(1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);

        } else {

            const val = gop.value_ptr.*;
            if (!terms_seen.checkOrInsertSIMD(val)) {
                self.II[col_idx].doc_freqs.items[val] += 1;
                try token_stream.addToken(new_doc.*, term_pos, val, col_idx);
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
        term_pos: *u8,
        col_idx: usize,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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

        term_pos.* += @intFromBool(term_pos.* != 255);
        cntr.* = 0;
    }

    inline fn addTermPq(
        self: *BM25Partition,
        term: []u8,
        term_len: usize,
        doc_id: u32,
        term_pos: u8,
        col_idx: usize,
        token_stream: *file_utils.ParquetTokenStream(file_utils.token_32t),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {
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
            try self.II[col_idx].doc_freqs.append(1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);

        } else {

            const val = gop.value_ptr.*;
            if (!terms_seen.checkOrInsertSIMD(val)) {
                self.II[col_idx].doc_freqs.items[val] += 1;
                try token_stream.addToken(new_doc.*, term_pos, val, col_idx);
            }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
        new_doc.* = false;
    }

    inline fn addTokenPq(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *file_utils.ParquetTokenStream(file_utils.token_32t),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {
        if (cntr.* == 0) {
            return;
        }

        try self.addTermPq(
            term, 
            cntr.*, 
            doc_id, 
            term_pos.*, 
            col_idx, 
            token_stream, 
            terms_seen,
            new_doc,
            );

        term_pos.* += @intFromBool(term_pos.* != 255);
        cntr.* = 0;
    }

    inline fn flushLargeToken(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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

        term_pos.* += @intFromBool(term_pos.* != 255);
        cntr.* = 0;
    }

    inline fn flushLargeTokenPq(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *file_utils.ParquetTokenStream(file_utils.token_32t),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {
        try self.addTermPq(
            term, 
            cntr.*, 
            doc_id, 
            term_pos.*, 
            col_idx, 
            token_stream, 
            terms_seen,
            new_doc,
            );

        term_pos.* += @intFromBool(term_pos.* != 255);
        cntr.* = 0;
    }


    pub fn processDocRfc4180(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                col_idx,
            );
            byte_idx.* += 1;
            return;
        }

        var term_pos: u8 = 0;
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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                col_idx,
            );
        }

        // byte_idx.* += buffer_idx;
        byte_idx.* = buffer_idx;
    }


    pub fn processDocVbyte(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                search_col_idx,
            );
            return;
        }

        var term_pos: u8 = 0;

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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                search_col_idx,
            );
        }
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
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

        var term_pos: u8 = 0;
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
                std.math.maxInt(u7),
                std.math.maxInt(u24),
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
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
        ) !void {

        var term_cntr = try self.allocator.alloc(usize, self.II[0].num_terms);
        defer self.allocator.free(term_cntr);
        for (0.., self.II) |col_idx, *II| {
            try II.resizePostings(self.allocator);

            if (II.num_terms > term_cntr.len) {
                term_cntr = try self.allocator.realloc(term_cntr, @as(usize, @intCast(II.num_terms)));
            }
            @memset(term_cntr, 0);

            // Create index.
            const output_file = &token_stream.output_files[col_idx];
            try output_file.seekTo(0);

            const tokens = &token_stream.tokens[col_idx];

            var bytes_read: usize = 0;

            var num_tokens: usize = file_utils.TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
                var _num_tokens: [4]u8 = undefined;
                bytes_read = try output_file.read(std.mem.asBytes(&_num_tokens));
                std.debug.assert(bytes_read == 4);

                num_tokens = std.mem.readInt(u32, &_num_tokens, ENDIANESS);

                bytes_read = try output_file.read(
                    std.mem.sliceAsBytes(tokens.*[0..num_tokens])
                    );

                for (0..num_tokens) |idx| {
                    if (@as(*u32, @ptrCast(&tokens.*[idx])).* == std.math.maxInt(u32)) {
                        // Null token.
                        current_doc_id += 1;
                        continue;
                    }

                    const new_doc  = tokens.*[idx].new_doc;
                    const term_pos = tokens.*[idx].term_pos;
                    const term_id: usize = @intCast(tokens.*[idx].term_id);

                    current_doc_id += @intCast(new_doc);

                    const postings_offset = II.term_offsets[term_id] + term_cntr[term_id];
                    std.debug.assert(postings_offset < II.postings.doc_ids.len);

                    std.debug.assert(current_doc_id <= II.num_docs);

                    term_cntr[term_id] += 1;

                    II.postings.doc_ids[postings_offset]        = @truncate(current_doc_id);
                    II.postings.term_positions[postings_offset] = @intCast(term_pos);
                }
            }
            std.debug.assert(
                (current_doc_id == II.num_docs - 1)
                    or
                (current_doc_id == II.num_docs)
            );
        }
    }

    pub fn constructFromTokenStreamPq(
        self: *BM25Partition,
        token_stream: *file_utils.ParquetTokenStream(file_utils.token_32t),
        ) !void {

        var term_cntr = try self.allocator.alloc(usize, self.II[0].num_terms);
        defer self.allocator.free(term_cntr);
        for (0.., self.II) |col_idx, *II| {
            try II.resizePostings(self.allocator);

            if (II.num_terms > term_cntr.len) {
                term_cntr = try self.allocator.realloc(term_cntr, @as(usize, @intCast(II.num_terms)));
            }
            @memset(term_cntr, 0);

            // Create index.
            const output_file = &token_stream.output_files[col_idx];
            try output_file.seekTo(0);

            const tokens = &token_stream.tokens[col_idx];

            var bytes_read: usize = 0;

            var num_tokens: usize = file_utils.TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
                var _num_tokens: [4]u8 = undefined;
                _ = try output_file.read(std.mem.asBytes(&_num_tokens));
                num_tokens = std.mem.readInt(u32, &_num_tokens, ENDIANESS);

                bytes_read = try output_file.read(
                    std.mem.sliceAsBytes(tokens.*[0..num_tokens])
                    );

                for (0..num_tokens) |idx| {
                    if (@as(*u32, @ptrCast(&tokens.*[idx])).* == std.math.maxInt(u32)) {
                        // Null token.
                        current_doc_id += 1;
                        continue;
                    }

                    const new_doc  = tokens.*[idx].new_doc;
                    const term_pos = tokens.*[idx].term_pos;
                    const term_id: usize = @intCast(tokens.*[idx].term_id);

                    current_doc_id += @intCast(new_doc);

                    const postings_offset = II.term_offsets[term_id] + term_cntr[term_id];
                    std.debug.assert(postings_offset < II.postings.doc_ids.len);

                    std.debug.assert(current_doc_id <= II.num_docs);

                    term_cntr[term_id] += 1;

                    II.postings.doc_ids[postings_offset] = @intCast(current_doc_id);
                    II.postings.term_positions[postings_offset] = @intCast(term_pos);
                }
            }
            // std.debug.print("\n\ncurrent_doc_id: {d}\n", .{current_doc_id});
            // std.debug.print("II.num_docs:    {d}\n", .{II.num_docs});
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
            // record_string.items[0..],
            record_string,
            self.allocator,
            result_positions,

            bit_sizes,
        ) catch {
            @panic("Error fetching document.");
        };
    }

    // pub fn fetchRecords(
        // self: *BM25Partition,
        // result_positions: []TermPos,
        // file_handle: *std.fs.File,
        // query_result: QueryResult,
        // record_string: *std.ArrayListUnmanaged(u8),
        // file_type: file_utils.FileType,
        // reference_dict: *const RadixTrie(u32),
    // ) !void {
        // const doc_id:    usize = @intCast(query_result.doc_id);
        // const byte_offset      = self.line_offsets[doc_id];
        // const next_byte_offset = self.line_offsets[doc_id + 1];
        // const bytes_to_read    = next_byte_offset - byte_offset;
// 
        // std.debug.assert(bytes_to_read < MAX_LINE_LENGTH);
// 
        // try file_handle.seekTo(byte_offset);
        // if (bytes_to_read > record_string.capacity) {
            // try record_string.resize(self.allocator, bytes_to_read);
        // }
        // _ = try file_handle.read(record_string.items[0..bytes_to_read]);
// 
        // switch (file_type) {
            // file_utils.FileType.CSV => {
                // record_string.items[bytes_to_read - 1] = '\n';
                // try csv.parseRecordCSV(
                    // record_string.items[0..bytes_to_read], 
                    // result_positions,
                    // );
            // },
            // file_utils.FileType.JSON => {
                // record_string.items[bytes_to_read - 1] = '{';
                // try json.parseRecordJSON(
                    // record_string.items[0..bytes_to_read], 
                    // result_positions,
                    // reference_dict,
                    // );
            // },
            // file_utils.FileType.PARQUET => unreachable,
        // }
    // }
};
