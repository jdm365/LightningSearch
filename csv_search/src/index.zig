const std = @import("std");
const builtin = @import("builtin");

const csv = @import("csv.zig");
const TermPos = @import("server.zig").TermPos;
const StaticIntegerSet = @import("static_integer_set.zig").StaticIntegerSet;

pub const MAX_TERM_LENGTH = 256;
pub const MAX_NUM_TERMS   = 4096;
pub const MAX_LINE_LENGTH = 1_048_576;

pub const ScoringInfo = packed struct {
    score: f32,
    term_pos: u8,
};

pub const QueryResult = struct {
    doc_id: u32,
    score: f32,
    partition_idx: usize,
};

pub const InvertedIndex = struct {
    postings: []csv.token_t,
    vocab: std.StringHashMap(u32),
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
        var vocab = std.StringHashMap(u32).init(allocator);

        // Guess capacity
        try vocab.ensureTotalCapacity(@intCast(num_docs / 25));

        const II = InvertedIndex{
            .postings = &[_]csv.token_t{},
            .vocab = vocab,
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

        allocator.free(self.term_offsets);
        self.doc_freqs.deinit();
        allocator.free(self.doc_sizes);
    }

    pub fn resizePostings(
        self: *InvertedIndex,
        allocator: std.mem.Allocator,
        ) !void {
        self.num_terms = @intCast(self.doc_freqs.items.len);
        self.term_offsets = try allocator.alloc(usize, self.num_terms);

        // Num terms is now known.
        var postings_size: usize = 0;
        for (0.., self.doc_freqs.items) |i, doc_freq| {
            self.term_offsets[i] = postings_size;
            postings_size += doc_freq;
        }
        self.term_offsets[self.num_terms - 1] = postings_size;
        self.postings = try allocator.alloc(csv.token_t, postings_size + 1);

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.num_docs);
        self.avg_doc_size = @floatCast(avg_doc_size);
    }
};

pub const BM25Partition = struct {
    II: []InvertedIndex,
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
            .II = try allocator.alloc(InvertedIndex, num_search_cols),
            .line_offsets = line_offsets,
            .allocator = allocator,
            .string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .doc_score_map = doc_score_map,
        };

        for (0..num_search_cols) |idx| {
            partition.II[idx] = try InvertedIndex.init(allocator, line_offsets.len - 1);
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

    inline fn addTerm(
        self: *BM25Partition,
        // term: *[MAX_TERM_LENGTH]u8,
        term: []u8,
        term_len: usize,
        doc_id: u32,
        term_pos: u8,
        col_idx: usize,
        token_stream: *csv.TokenStream,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
        new_doc: *bool,
    ) !void {

        const gop = try self.II[col_idx].vocab.getOrPut(term[0..term_len]);

        if (!gop.found_existing) {
            const term_copy = try self.string_arena.allocator().dupe(u8, term[0..term_len]);

            gop.key_ptr.* = term_copy;
            gop.value_ptr.* = self.II[col_idx].num_terms;
            self.II[col_idx].num_terms += 1;
            try self.II[col_idx].doc_freqs.append(1);
            try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);
        } else {

            if (!terms_seen.checkOrInsert(gop.value_ptr.*)) {
                self.II[col_idx].doc_freqs.items[gop.value_ptr.*] += 1;
                try token_stream.addToken(new_doc.*, term_pos, gop.value_ptr.*, col_idx);
            }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
        new_doc.* = false;
    }

    inline fn addToken(
        self: *BM25Partition,
        // term: *[MAX_TERM_LENGTH]u8,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *csv.TokenStream,
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
        // term: *[MAX_TERM_LENGTH]u8,
        term: []u8,
        cntr: *usize,
        doc_id: u32,
        term_pos: *u8,
        col_idx: usize,
        token_stream: *csv.TokenStream,
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
        token_stream: *csv.TokenStream,
        doc_id: u32,
        // byte_idx: *usize,
        _: *usize,
        col_idx: usize,
        _: *[MAX_TERM_LENGTH]u8,
        max_byte: usize,
        terms_seen: *StaticIntegerSet(MAX_NUM_TERMS),
    ) !void {
        const byte_idx: *usize = &token_stream.buffer_idx;

        // if (byte_idx.* == max_byte) {
        if (token_stream.f_data[byte_idx.*] == ',') {
            try token_stream.addToken(
                true,
                std.math.maxInt(u7),
                std.math.maxInt(u24),
                col_idx,
            );
            byte_idx.* += 1;
            return;
        }

        const start_byte = byte_idx.*;

        var term_pos: u8 = 0;
        const is_quoted = (token_stream.f_data[byte_idx.*] == '"');
        byte_idx.* += @intFromBool(is_quoted);
        try token_stream.incBufferIdx();

        var cntr: usize = 0;
        var new_doc: bool = (doc_id != 0);

        terms_seen.clear();

        if (is_quoted) {

            outer_loop: while (true) {
                if (self.II[col_idx].doc_sizes[doc_id] >= MAX_NUM_TERMS) {
                    byte_idx.* = start_byte;
                    token_stream.iterFieldCSV(byte_idx);
                    try token_stream.incBufferIdx();
                    return;
                }
                std.debug.assert(byte_idx.* <= max_byte);

                if (cntr > MAX_TERM_LENGTH - 4) {
                    try self.flushLargeToken(
                        token_stream.f_data[byte_idx.*-cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
                        );
                    continue;
                }

                // std.debug.print("Buffer: {s}\n", .{token_stream.f_data[byte_idx.*-@min(byte_idx.*, 8)..][0..64]});
                // std.debug.print("Buffer: {s}\n", .{token_stream.f_data[byte_idx.*..][0..64]});
                switch (token_stream.f_data[byte_idx.*]) {
                    '"' => {
                        byte_idx.* += 1;
                        // try token_stream.incBufferIdx();

                        byte_idx.* += 1;
                        std.debug.print("Buffer: {s}\n", .{token_stream.f_data[byte_idx.*-1..][0..64]});
                        switch (token_stream.f_data[byte_idx.* - 1]) {
                            ',', '\n' => break :outer_loop,
                            '"' => {
                                continue;
                            },
                            else => return error.UnexpectedQuote,
                        }
                    },
                    0...33, 35...47, 58...64, 91...96, 123...126 => {
                        std.debug.print("Buffer 2: {s}\n", .{token_stream.f_data[byte_idx.*..][0..64]});
                        if (cntr == 0) {
                            byte_idx.* += 1;
                            continue;
                        }

                        try self.addToken(
                            token_stream.f_data[byte_idx.*-@min(byte_idx.*, cntr)..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            col_idx, 
                            token_stream, 
                            terms_seen,
                            &new_doc,
                            );
                        try token_stream.incBufferIdx();
                    },
                    else => {
                        std.debug.print("Buffer 3: {s}\n", .{token_stream.f_data[byte_idx.*..][0..64]});
                        cntr += 1;
                        byte_idx.* += 1;
                        // try token_stream.incBufferIdx();
                    },
                }
            }

        } else {

            outer_loop: while (true) {
                std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);
                std.debug.assert(byte_idx.* <= max_byte);

                std.debug.print("Buffer 4: {s}\n", .{token_stream.f_data[byte_idx.*..][0..64]});

                if (cntr > MAX_TERM_LENGTH - 4) {
                    try self.flushLargeToken(
                        token_stream.f_data[byte_idx.*-cntr..], 
                        &cntr, 
                        doc_id, 
                        &term_pos, 
                        col_idx, 
                        token_stream, 
                        terms_seen,
                        &new_doc,
                        );
                    continue;
                }


                switch (token_stream.f_data[byte_idx.*]) {
                    ',', '\n' => break :outer_loop,
                    0...9, 11...43, 45...47, 58...64, 91...96, 123...126 => {
                        if (cntr == 0) {
                            byte_idx.* += 1;
                            continue;
                        }

                        try self.addToken(
                            token_stream.f_data[byte_idx.*-cntr..], 
                            &cntr, 
                            doc_id, 
                            &term_pos, 
                            col_idx, 
                            token_stream, 
                            terms_seen,
                            &new_doc,
                            );
                        try token_stream.incBufferIdx();
                    },
                    else => {
                        cntr += 1;
                        byte_idx.* += 1;
                        // try token_stream.incBufferIdx();
                    },
                }
            }
        }

        if (cntr > 0) {
            std.debug.assert(self.II[col_idx].doc_sizes[doc_id] < MAX_NUM_TERMS);

            try self.addTerm(
                token_stream.f_data[byte_idx.*-cntr..], 
                cntr, 
                doc_id, 
                term_pos, 
                col_idx, 
                token_stream, 
                terms_seen,
                &new_doc,
                );
            try token_stream.incBufferIdx();
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


    pub fn constructFromTokenStream(
        self: *BM25Partition,
        token_stream: *csv.TokenStream,
        ) !void {

        for (0.., self.II) |col_idx, *II| {
            try II.resizePostings(self.allocator);
            var term_offsets = try self.allocator.alloc(usize, II.num_terms);
            defer self.allocator.free(term_offsets);
            @memset(term_offsets, 0);

            // Create index.
            // const ts = token_stream.*;
            const output_file = &token_stream.output_files[col_idx];
            try output_file.seekTo(0);

            const tokens = &token_stream.tokens[col_idx];

            var bytes_read: usize = 0;

            var num_tokens: usize = csv.TOKEN_STREAM_CAPACITY;
            var current_doc_id: usize = 0;

            while (num_tokens == csv.TOKEN_STREAM_CAPACITY) {
                var _num_tokens: [4]u8 = undefined;
                _ = try output_file.read(std.mem.asBytes(&_num_tokens));
                const endianness = builtin.cpu.arch.endian();
                num_tokens = std.mem.readInt(u32, &_num_tokens, endianness);

                bytes_read = try output_file.read(
                    std.mem.sliceAsBytes(tokens.*[0..num_tokens])
                    );
                std.debug.assert(bytes_read == 4 * num_tokens);

                var token_count: usize = 0;
                for (token_count..token_count + num_tokens) |idx| {
                    if (@as(*u32, @ptrCast(&tokens.*[idx])).* == std.math.maxInt(u32)) {
                        // Null token.
                        current_doc_id += 1;
                        continue;
                    }

                    const new_doc  = tokens.*[idx].new_doc;
                    const term_pos = tokens.*[idx].term_pos;
                    const term_id: usize = @intCast(tokens.*[idx].doc_id);


                    current_doc_id += @intCast(new_doc);

                    const token = csv.token_t{
                        .new_doc = 0,
                        .term_pos = term_pos,
                        .doc_id = @truncate(current_doc_id),
                    };

                    const postings_offset = II.term_offsets[term_id] + term_offsets[term_id];
                    std.debug.assert(postings_offset < II.postings.len);
                    std.debug.assert(current_doc_id < II.num_docs);

                    term_offsets[term_id] += 1;

                    II.postings[postings_offset] = token;
                }

                token_count += num_tokens;
            }
        }
    }

    pub fn fetchRecords(
        self: *const BM25Partition,
        result_positions: []TermPos,
        file_handle: *std.fs.File,
        query_result: QueryResult,
        record_string: *std.ArrayList(u8),
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
        try csv.parseRecordCSV(record_string.items[0..bytes_to_read], result_positions);
    }
};


