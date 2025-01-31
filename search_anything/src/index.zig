const std = @import("std");
const builtin = @import("builtin");

const TokenStream = @import("file_utils.zig").TokenStream;
const TOKEN_STREAM_CAPACITY = @import("file_utils.zig").TOKEN_STREAM_CAPACITY;
const token_t = @import("file_utils.zig").token_t;

const csv = @import("csv.zig");
const json = @import("json.zig");
const string_utils = @import("string_utils.zig");
const file_utils = @import("file_utils.zig");

const TermPos = @import("server.zig").TermPos;
const StaticIntegerSet = @import("static_integer_set.zig").StaticIntegerSet;
const PruningRadixTrie = @import("pruning_radix_trie.zig").PruningRadixTrie;
const RadixTrie = @import("radix_trie.zig").RadixTrie;

pub const MAX_TERM_LENGTH = 256;
pub const MAX_NUM_TERMS   = 4096;
pub const MAX_LINE_LENGTH = 1_048_576;

const ENDIANESS = builtin.cpu.arch.endian();

pub const ScoringInfo = packed struct {
    score: f32,
    term_pos: u8,
};

pub const QueryResult = packed struct(u64){
    doc_id: u32,
    partition_idx: u32,
};

// const SHM = std.HashMap([]const u8, u32, std.hash.XxHash64, 80);
const SHM = struct {
    pub fn hash(self: @This(), key: []const u8) u64 {
        _ = self;
        return std.hash.Wyhash.hash(0, key);
    }

    pub fn eql(self: @This(), a: []const u8, b: []const u8) bool {
        _ = self;
        return std.mem.eql(u8, a, b);
    }
};

pub const token_64t = packed struct(u64) {
    new_doc: u1,
    term_pos: u8,
    doc_id: u32,
};

pub const Postings = struct {
    doc_ids: []u32,
    term_positions: []u8,
    // new_docs: []u1,
};

pub const InvertedIndexV2 = struct {
    postings: Postings,

    vocab: std.hash_map.HashMap([]const u8, u32, SHM, 80),
    // prt_vocab: PruningRadixTrie(u32),
    prt_vocab: RadixTrie(u32),
    term_offsets: []usize,
    doc_freqs: std.ArrayList(u32),
    doc_sizes: []u16,

    num_terms: u32,
    num_docs: u32,
    avg_doc_size: f32,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        ) !InvertedIndexV2 {
        var vocab = std.hash_map.HashMap([]const u8, u32, SHM, 80).init(allocator);

        // Guess capacity
        try vocab.ensureTotalCapacity(@intCast(num_docs / 25));

        const II = InvertedIndexV2{
            .postings = Postings{
                .doc_ids = undefined,
                .term_positions = undefined,
            },
            .vocab = vocab,
            .prt_vocab = try RadixTrie(u32).init(std.heap.c_allocator),
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
        return II;
    }

    pub fn deinit(
        self: *InvertedIndexV2,
        allocator: std.mem.Allocator,
        ) void {
        // allocator.free(self.postings.new_docs);
        allocator.free(self.postings.term_positions);
        allocator.free(self.postings.doc_ids);

        self.vocab.deinit();
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

        std.debug.assert(self.num_terms == self.vocab.count());

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

        try self.buildPRT();
    }

    pub fn buildPRT(self: *InvertedIndexV2) !void {
        const start = std.time.milliTimestamp();
        var vocab_iterator = self.vocab.iterator();
        while (vocab_iterator.next()) |val| {
            const df = @as(f32, @floatFromInt(self.doc_freqs.items[val.value_ptr.*]));
            if (df >= 5.0) {
                // try self.prt_vocab.insert(
                    // val.key_ptr.*, 
                    // val.value_ptr.*, 
                    // df,
                    // );
                try self.prt_vocab.insert(
                    val.key_ptr.*, 
                    val.value_ptr.*, 
                    // df,
                    );
            }
        }
        const end = std.time.milliTimestamp();
        std.debug.print("Build PRT in {}ms\n", .{end - start});
    }
};

pub const InvertedIndex = struct {
    postings: []token_t,
    vocab: std.hash_map.HashMap([]const u8, u32, SHM, 80),
    // prt_vocab: PruningRadixTrie(u32),
    prt_vocab: RadixTrie(u32),
    term_offsets: []usize,
    doc_freqs: std.ArrayList(u32),
    doc_sizes: []u16,

    num_terms: u32,
    num_docs: u32,
    avg_doc_size: f32,

    pub fn init(
        allocator: std.mem.Allocator,
        num_docs: usize,
        ) !InvertedIndex {
        // var vocab = std.StringHashMap(u32).init(allocator);
        var vocab = std.hash_map.HashMap([]const u8, u32, SHM, 80).init(allocator);

        // Guess capacity
        try vocab.ensureTotalCapacity(@intCast(num_docs / 25));

        const II = InvertedIndex{
            .postings = &[_]token_t{},
            .vocab = vocab,
            // .prt_vocab = try PruningRadixTrie(u32).init(allocator),
            .prt_vocab = try RadixTrie(u32).init(std.heap.c_allocator),
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
        return II;
    }

    pub fn deinit(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) void {
        allocator.free(self.postings);
        self.vocab.deinit();
        self.prt_vocab.deinit();

        allocator.free(self.term_offsets);
        self.doc_freqs.deinit();
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) !void {
        self.num_terms = @intCast(self.doc_freqs.items.len);
        self.term_offsets = try allocator.alloc(usize, self.num_terms + 1);

        std.debug.assert(self.num_terms == self.vocab.count());

        // Num terms is now known.
        var postings_size: usize = 0;
        for (0.., self.doc_freqs.items) |i, doc_freq| {
            self.term_offsets[i] = postings_size;
            postings_size += doc_freq;
        }
        self.term_offsets[self.num_terms] = postings_size;
        self.postings = try allocator.alloc(token_t, postings_size + 1);

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.num_docs);
        self.avg_doc_size = @floatCast(avg_doc_size);

        try self.buildPRT();
    }

    pub fn buildPRT(self: *InvertedIndex) !void {
        const start = std.time.milliTimestamp();
        var vocab_iterator = self.vocab.iterator();
        while (vocab_iterator.next()) |val| {
            const df = @as(f32, @floatFromInt(self.doc_freqs.items[val.value_ptr.*]));
            if (df >= 5.0) {
                // try self.prt_vocab.insert(
                    // val.key_ptr.*, 
                    // val.value_ptr.*, 
                    // df,
                    // );
                try self.prt_vocab.insert(
                    val.key_ptr.*, 
                    val.value_ptr.*, 
                    // df,
                    );
            }
        }
        const end = std.time.milliTimestamp();
        std.debug.print("Build PRT in {}ms\n", .{end - start});
    }
};

pub const BM25Partition = struct {
    // II: []InvertedIndex,
    II: []InvertedIndexV2,
    line_offsets: []usize,
    allocator: std.mem.Allocator,
    string_arena: std.heap.ArenaAllocator,
    doc_score_map: std.AutoHashMap(u32, ScoringInfo),

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        line_offsets: []usize,
    ) !BM25Partition {
        var doc_score_map = std.AutoHashMap(u32, ScoringInfo).init(allocator);
        try doc_score_map.ensureTotalCapacity(50_000);

        const partition = BM25Partition{
            // .II = try allocator.alloc(InvertedIndex, num_search_cols),
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .line_offsets = line_offsets,
            .allocator = allocator,
            .string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .doc_score_map = doc_score_map,
        };

        for (0..num_search_cols) |idx| {
            // partition.II[idx] = try InvertedIndex.init(
            partition.II[idx] = try InvertedIndexV2.init(
                allocator, 
                line_offsets.len - 1,
                // line_offsets.len,
                );
        }

        return partition;
    }

    pub fn deinit(self: *BM25Partition) void {
        self.allocator.free(self.line_offsets);
        for (0..self.II.len) |i| {
            self.II[i].deinit(self.allocator);
        }
        self.allocator.free(self.II);
        self.string_arena.deinit();
        self.doc_score_map.deinit();
    }

    pub fn resizeNumSearchCols(self: *BM25Partition, num_search_cols: usize) !void {
        const current_length = self.II.len;
        if (num_search_cols <= current_length) return;

        self.II = try self.allocator.realloc(self.II, num_search_cols);
        for (current_length..num_search_cols) |idx| {
            // self.II[idx] = try InvertedIndex.init(
            self.II[idx] = try InvertedIndexV2.init(
                self.allocator, 
                self.line_offsets.len - 1,
                // self.line_offsets.len,
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
        token_stream: *TokenStream,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {

        const gop = try self.II[col_idx].vocab.getOrPut(term[0..term_len]);

        if (!gop.found_existing) {
            const term_copy = try self.string_arena.allocator().dupe(
                u8, 
                term[0..term_len],
                );

            gop.key_ptr.* = term_copy;
            gop.value_ptr.* = self.II[col_idx].num_terms;
            self.II[col_idx].num_terms += 1;
            try self.II[col_idx].doc_freqs.append(1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);

        } else {

            const val = gop.value_ptr.*;
            if (!terms_seen.checkOrInsert(val)) {
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
        token_stream: *TokenStream,
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

    inline fn flushLargeToken(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *TokenStream,
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


    pub fn processDocRfc4180(
        self: *BM25Partition,
        token_stream: *TokenStream,
        byte_idx: *usize,
        doc_id: u32,
        col_idx: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        const buffer = try token_stream.getBuffer(byte_idx.*);
        var buffer_idx: usize = 0;

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
                    buffer_idx = 0;
                    try token_stream.iterFieldCSV(&buffer_idx);
                    byte_idx.* += buffer_idx;
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
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
                        cntr += 1;
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

        byte_idx.* += buffer_idx;
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        token_stream: *TokenStream,
        trie: *const RadixTrie(u32),
        search_col_mapping: *const std.AutoHashMap(u32, u32),
        byte_idx: *usize,
        doc_id: u32,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        const buffer = try token_stream.getBuffer(byte_idx.*);
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
        const II_idx = search_col_mapping.get(matched_col_idx) orelse {
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
        // std.debug.print("BUFFER 4: {s}\n", .{buffer[buffer_idx..][0..64]});

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
                    try token_stream.iterFieldJSON(&buffer_idx);
                    byte_idx.* += buffer_idx;
                    return;
                }

                if (cntr > MAX_TERM_LENGTH - 4) {
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
        token_stream: *TokenStream,
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

            var num_tokens: usize = TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            while (num_tokens == TOKEN_STREAM_CAPACITY) {
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
                    const term_id: usize = @intCast(tokens.*[idx].doc_id);

                    current_doc_id += @intCast(new_doc);

                    // const token = token_t{
                        // .new_doc = 0,
                        // .term_pos = term_pos,
                        // .doc_id = @truncate(current_doc_id),
                    // };

                    const postings_offset = II.term_offsets[term_id] + term_cntr[term_id];
                    std.debug.assert(postings_offset < II.postings.doc_ids.len);

                    // std.debug.assert(current_doc_id < II.num_docs);
                    std.debug.assert(current_doc_id <= II.num_docs);

                    term_cntr[term_id] += 1;

                    // II.postings[postings_offset] = token;
                    II.postings.doc_ids[postings_offset] = @intCast(term_pos);
                    II.postings.term_positions[postings_offset] = @intCast(current_doc_id);
                }
            }
            std.debug.assert(
                (current_doc_id == II.num_docs - 1)
                    or
                (current_doc_id == II.num_docs)
            );
        }
    }

    pub fn fetchRecords(
        self: *const BM25Partition,
        result_positions: []TermPos,
        file_handle: *std.fs.File,
        query_result: QueryResult,
        record_string: *std.ArrayList(u8),
        file_type: file_utils.FileType,
        reference_dict: *const RadixTrie(u32),
    ) !void {
        const doc_id: usize = @intCast(query_result.doc_id);
        const byte_offset = self.line_offsets[doc_id];
        const next_byte_offset = self.line_offsets[doc_id + 1];
        const bytes_to_read = next_byte_offset - byte_offset;

        std.debug.assert(bytes_to_read < MAX_LINE_LENGTH);

        try file_handle.seekTo(byte_offset);
        if (bytes_to_read > record_string.capacity) {
            try record_string.resize(bytes_to_read);
        }
        _ = try file_handle.read(record_string.items[0..bytes_to_read]);

        switch (file_type) {
            file_utils.FileType.CSV => {
                record_string.items[bytes_to_read - 1] = '\n';
                try csv.parseRecordCSV(
                    record_string.items[0..bytes_to_read], 
                    result_positions,
                    );
            },
            file_utils.FileType.JSON => {
                record_string.items[bytes_to_read - 1] = '{';
                try json.parseRecordJSON(
                    record_string.items[0..bytes_to_read], 
                    result_positions,
                    reference_dict,
                    );
            },
        }
    }
};
