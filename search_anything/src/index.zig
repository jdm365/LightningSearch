const std = @import("std");
const builtin = @import("builtin");

const TokenStream = @import("file_utils.zig").TokenStream;
const ParquetTokenStream = @import("file_utils.zig").ParquetTokenStream;
const TOKEN_STREAM_CAPACITY = @import("file_utils.zig").TOKEN_STREAM_CAPACITY;

const csv  = @import("csv.zig");
const json = @import("json.zig");
const pq   = @import("parquet.zig");
const string_utils = @import("string_utils.zig");
const file_utils = @import("file_utils.zig");

const TermPos = @import("server.zig").TermPos;
const StaticIntegerSet = @import("static_integer_set.zig").StaticIntegerSet;
const PruningRadixTrie = @import("pruning_radix_trie.zig").PruningRadixTrie;
const RadixTrie = @import("radix_trie.zig").RadixTrie;

pub const MAX_TERM_LENGTH = 256;
pub const MAX_NUM_TERMS   = 4096;
pub const MAX_LINE_LENGTH = 1_048_576;

pub const ENDIANESS = builtin.cpu.arch.endian();

pub const ScoringInfo = packed struct {
    score: f32,
    term_pos: u8,
};

pub const QueryResult = packed struct(u64){
    doc_id: u32,
    partition_idx: u32,
};

const SHM = std.HashMap([]const u8, u32, std.hash.XxHash64, 80);

const IndexContext = struct {
    string_bytes: *const std.ArrayListUnmanaged(u8),

    pub fn eql(_: IndexContext, a: u32, b: u32) bool {
        return a == b;
    }

    pub fn hash(self: IndexContext, x: u32) u64 {
        const x_slice = std.mem.span(
            @as([*:0]u8, @ptrCast(&self.string_bytes.items[x]))
            );
        return std.hash_map.hashString(x_slice);
    }
};

const SliceAdapter = struct {
    string_bytes: *const std.ArrayListUnmanaged(u8),

    pub fn eql(self: SliceAdapter, a_slice: []u8, b: u32) bool {
        const b_slice = std.mem.span(
            @as([*:0]u8, @ptrCast(&self.string_bytes.items[b])),
            );
        return std.mem.eql(u8, a_slice, b_slice);
    }

    pub fn hash(_: SliceAdapter, adapted_key: []u8) u64 {
        return std.hash_map.hashString(adapted_key);
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


pub const InvertedIndexV2 = struct {
    postings: Postings,

    vocab: Vocab,
    // vocab: std.hash_map.HashMap([]const u8, u32, SHM, 80),
    // prt_vocab: PruningRadixTrie(u32),
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
        // var vocab = std.hash_map.HashMap([]const u8, u32, SHM, 80).init(allocator);
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
        // allocator.free(self.postings.new_docs);
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
    line_offsets: []usize,
    num_records: usize,
    allocator: std.mem.Allocator,
    string_arena: std.heap.ArenaAllocator,
    doc_score_map: std.AutoHashMap(u32, ScoringInfo),

    pub fn init(
        allocator: std.mem.Allocator,
        num_search_cols: usize,
        line_offsets: []usize,
        num_records: usize,
    ) !BM25Partition {
        var doc_score_map = std.AutoHashMap(u32, ScoringInfo).init(allocator);
        try doc_score_map.ensureTotalCapacity(50_000);

        const partition = BM25Partition{
            .II = try allocator.alloc(InvertedIndexV2, num_search_cols),
            .line_offsets = line_offsets,
            .num_records = num_records,
            .allocator = allocator,
            .string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .doc_score_map = doc_score_map,
        };

        for (0..num_search_cols) |idx| {
            partition.II[idx] = try InvertedIndexV2.init(
                allocator, 
                // num_records - 1,
                num_records,
                // line_offsets.len - 1,
                // line_offsets.len,
                );
        }

        return partition;
    }

    pub fn deinit(self: *BM25Partition) void {
        // self.allocator.free(self.line_offsets);
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
            self.II[idx] = try InvertedIndexV2.init(
                self.allocator, 
                self.num_records,
                // self.line_offsets.len - 1,
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
        token_stream: *TokenStream(file_utils.token_32t),
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {
        // const gop = try self.II[col_idx].vocab.getOrPut(term[0..term_len]);
        const gop = try self.II[col_idx].vocab.map.getOrPutContextAdapted(
            self.allocator,
            term[0..term_len],
            self.II[col_idx].vocab.getAdapter(),
            self.II[col_idx].vocab.getCtx(),
            );

        if (!gop.found_existing) {
            // const term_copy = try self.string_arena.allocator().dupe(
                // u8, 
                // term[0..term_len],
                // );
            try self.II[col_idx].vocab.string_bytes.appendSlice(self.allocator, term[0..term_len]);
            try self.II[col_idx].vocab.string_bytes.append(self.allocator, 0);

            // gop.key_ptr.* = term_copy;
            gop.key_ptr.* = @truncate(
                self.II[col_idx].vocab.string_bytes.items.len - term_len - 1,
                );
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
        token_stream: *TokenStream(file_utils.token_32t),
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
        token_stream: *ParquetTokenStream(file_utils.token_32t),
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
            // const term_copy = try self.string_arena.allocator().dupe(
                // u8, 
                // term[0..term_len],
                // );
            try self.II[col_idx].vocab.string_bytes.appendSlice(self.allocator, term[0..term_len]);
            try self.II[col_idx].vocab.string_bytes.append(self.allocator, 0);

            // gop.key_ptr.* = term_copy;
            gop.key_ptr.* = @truncate(
                self.II[col_idx].vocab.string_bytes.items.len - term_len - 1,
                );
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

    inline fn addTokenPq(
        self: *BM25Partition,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *ParquetTokenStream(file_utils.token_32t),
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
        token_stream: *TokenStream(file_utils.token_32t),
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
        token_stream: *ParquetTokenStream(file_utils.token_32t),
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
        token_stream: *TokenStream(file_utils.token_32t),
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

    pub fn processDocVbyte(
        self: *BM25Partition,
        token_stream: *ParquetTokenStream(file_utils.token_32t),
        doc_id: u32,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        const field_len = pq.decodeVbyte(token_stream.column_buffer, &token_stream.current_idx);
        string_utils.stringToUpper(
            @ptrCast(token_stream.column_buffer[token_stream.current_idx..][0..field_len]), 
            field_len,
            );
        var buffer = std.mem.bytesAsSlice(
            u8,
            token_stream.column_buffer[token_stream.current_idx..][0..field_len],
        );

        defer token_stream.current_idx += field_len;

        var buffer_idx: usize = 0;
        const col_idx = token_stream.current_col_idx;

        if (field_len == 0) {
            try token_stream.addToken(
                true,
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                col_idx,
            );
            return;
        }

        var term_pos: u8 = 0;

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        while (buffer_idx < field_len) {
            std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

            if (cntr > MAX_TERM_LENGTH - 4) {
                try self.flushLargeTokenPq(
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
                0...47, 58...64, 91...96, 123...126 => {
                    if (cntr == 0) {
                        buffer_idx += 1;
                        cntr = 0;
                        continue;
                    }

                    const start_idx = buffer_idx - @min(buffer_idx, cntr);
                    try self.addTokenPq(
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

        if (cntr > 0) {
            std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

            const start_idx = buffer_idx - @min(buffer_idx, cntr + 1);
            try self.addTermPq(
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
    }

    pub fn processDocRfc8259(
        self: *BM25Partition,
        token_stream: *TokenStream(file_utils.token_32t),
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
        token_stream: *TokenStream(file_utils.token_32t),
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
            std.debug.assert(
                (current_doc_id == II.num_docs - 1)
                    or
                (current_doc_id == II.num_docs)
            );
        }
    }

    pub fn constructFromTokenStreamPq(
        self: *BM25Partition,
        token_stream: *ParquetTokenStream(file_utils.token_32t),
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
            // std.debug.assert(
                // (current_doc_id == II.num_docs - 1)
                    // or
                // (current_doc_id == II.num_docs)
            // );
        }
    }

    pub fn fetchRecords(
        self: *BM25Partition,
        result_positions: []TermPos,
        file_handle: *std.fs.File, // TODO: Consider replacing this with union of parquet open reader obj.
        filename: [*:0]const u8,
        query_result: QueryResult,
        record_string: *std.ArrayList(u8),
        file_type: file_utils.FileType,
        reference_dict: *const RadixTrie(u32),
    ) !void {
        if (file_type == .PARQUET) {
            try self.fetchRecordsParquet(
                result_positions,
                filename,
                query_result,
                record_string,
            );
            return;
        }
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
            file_utils.FileType.PARQUET => unreachable,
        }
    }

    pub fn fetchRecordsParquet(
        self: *BM25Partition,
        result_positions: []TermPos,
        filename: [*:0]const u8,
        query_result: QueryResult,
        record_string: *std.ArrayList(u8),
    ) !void {
        // TODO: Batch together all requests to a particular row group.
        var row_count_rem: usize = @intCast(query_result.doc_id);

        var local_row_num: usize = 0;
        var rg_idx: usize = 0;
        while (true) {
            // Currently when parquet, line_offsets is actually row_group sizes.
            if (self.line_offsets[rg_idx] <= row_count_rem) {
                local_row_num = row_count_rem;
                break;
            }
            row_count_rem -= self.line_offsets[rg_idx];
            rg_idx += 1;
        }

        pq.fetchRowFromRowGroup(
            filename,
            rg_idx,
            local_row_num,
            record_string.items.ptr,
            @alignCast(@ptrCast(result_positions.ptr)),
        );
    }
};
