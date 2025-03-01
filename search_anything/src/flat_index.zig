const std = @import("std");
const string_utils = @import("string_utils.zig");
const file_utils = @import("file_utils.zig");
const progress = @import("progress.zig");
const rt = @import("radix_trie.zig");
const index = @import("index.zig");
const manager = @import("index_manager.zig");
const SortedScoreMultiArray = @import("sorted_array.zig").SortedScoreMultiArray;

///////////////////////////////////////////
// Keep single threaded for now. Wait    // 
// until it is clear that multithreaded  //
// is needed for large non tabular docs. //
///////////////////////////////////////////

const FlatToken = struct {
    token: u32,
    term_pos: u32,
};

pub const FlatScoringInfo = packed struct {
    term_pos: u64,
    score: f32,
};

pub fn RLE(comptime T: type) type {
    return struct{
        const Self = @This();

        value: T,
        run_length: u32,

        pub fn init() Self {
            return Self{
                .value = 0,
                .run_length = 1,
            };
        }
    };
}


pub const PostingsDynamic = struct {
    doc_ids: std.ArrayList(std.ArrayListUnmanaged(RLE(u32))),
    term_positions: std.ArrayList(std.ArrayListUnmanaged(u64)),

    pub fn init(allocator: std.mem.Allocator) !PostingsDynamic {
        const PD = PostingsDynamic{
            .doc_ids = std.ArrayList(std.ArrayListUnmanaged(RLE(u32))).init(allocator),
            .term_positions = std.ArrayList(std.ArrayListUnmanaged(u64)).init(allocator),
        };
        return PD;
    }

    pub fn deinit(self: *PostingsDynamic) void {
        for (self.doc_ids.items) |*doc_ids| {
            doc_ids.deinit(self.doc_ids.allocator);
        }
        for (self.term_positions.items) |*term_positions| {
            term_positions.deinit(self.term_positions.allocator);
        }

        self.doc_ids.deinit();
        self.term_positions.deinit();
    }
};

pub const FlatFileIndexManager = struct {
    gpa: *std.heap.GeneralPurposeAllocator(.{}),
    string_arena: std.heap.ArenaAllocator,
    postings: PostingsDynamic,

    vocab: std.StringHashMap(u32),
    doc_freqs: std.ArrayList(u32),
    doc_sizes: std.ArrayList(u64),
    avg_doc_size: f32,

    doc_score_map: std.ArrayHashMap(u32, FlatScoringInfo),

    pub fn init() !FlatFileIndexManager {
        var string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        var gpa = try string_arena.allocator().create(std.heap.GeneralPurposeAllocator(.{}));
        gpa.* = std.heap.GeneralPurposeAllocator(.{}){};

        const FFI = FlatFileIndexManager {
            .gpa = gpa,
            .string_arena = string_arena,

            .postings = try PostingsDynamic.init(gpa.allocator()),
            .vocab = std.StringHashMap(u32).init(gpa.allocator()),
            .doc_freqs = std.ArrayList(u32).init(gpa.allocator()),
            .doc_sizes = std.ArrayList(u64).init(gpa.allocator()),
            .avg_doc_size = 0.0,

            .doc_score_map = std.AutoHashMap(u32, FlatScoringInfo).init(gpa.allocator()),
        };
        return FFI;
    }

    pub fn deinit(
        self: *FlatFileIndexManager,
        ) void {
        self.postings.deinit();
        self.vocab.deinit();

        self.doc_freqs.deinit();
        self.doc_sizes.deinit();

        _ = self.gpa.deinit();
        self.string_arena.deinit();
        self.doc_score_map.deinit();
    }

    inline fn addTerm(
        self: *FlatFileIndexManager,
        term: []const u8,
        term_len: usize,
        term_pos: u64,
        token_stream: *file_utils.TokenStream(file_utils.token_64t),
        file_terms: *std.AutoHashMap(u32, void),
    ) !void {
        std.debug.assert(term_len > 0);
        std.debug.assert(term_len < index.MAX_TERM_LENGTH);

        const gop = try self.vocab.getOrPut(term[0..term_len]);

        if (!gop.found_existing) {
            const term_copy = try self.string_arena.allocator().dupe(
                u8, 
                term[0..term_len],
                );

            gop.key_ptr.* = term_copy;
            gop.value_ptr.* = @truncate(self.vocab.count() - 1);
            try self.doc_freqs.append(1);
            try token_stream.addToken64(term_pos, gop.value_ptr.*);

        } else {

            const val = gop.value_ptr.*;

            const gop2 = try file_terms.getOrPut(val);
            if (!gop2.found_existing) {
                self.doc_freqs.items[val] += 1;
                try token_stream.addToken64(term_pos, val);
            } else {
                gop2.key_ptr.* = val;
            }
        }
    }

    inline fn tokenize(
        self: *FlatFileIndexManager,
        buffer: []const u8,
        token_stream: *file_utils.TokenStream(file_utils.token_64t),
        file_terms: *std.AutoHashMap(u32, void),
        doc_size: *usize,
        ) !usize {
        var buffer_pos: usize = 0;
        while (buffer_pos < buffer.len) {
            const skip_chars = @min(string_utils.simdFindCharInRangesEscapedFull(
                buffer[buffer_pos..], 
                &string_utils.SEP_RANGES,
                ), index.MAX_TERM_LENGTH);

            if (skip_chars > 0) {
                try self.addTerm(
                    buffer[buffer_pos..],
                    skip_chars,
                    doc_size.*,
                    token_stream,
                    file_terms,
                    );
            }
            buffer_pos += skip_chars + 1;
            if (buffer_pos >= buffer.len) break;
            while (
                (string_utils.SCALAR_LOOKUP_TABLE(&string_utils.SEP_RANGES)[buffer[buffer_pos]] == 1)
                    and
                (buffer_pos < buffer.len - 1)
                ) buffer_pos += 1;
        }
        return buffer_pos;
    }

    pub fn indexFile(
        self: *FlatFileIndexManager,
        filename: []const u8,
        ) !void {
        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(filename, &hash, .{});
            break :blk hash;
        };
        const tmp_dir = try std.fmt.allocPrint(
            self.string_arena.allocator(),
            ".{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
            );
        const output_filename = try std.fmt.allocPrint(
            self.string_arena.allocator(),
            "{s}/output", 
            .{tmp_dir}
            );

        std.fs.cwd().makeDir(tmp_dir) catch {
            try std.fs.cwd().deleteTree(tmp_dir);
            try std.fs.cwd().makeDir(tmp_dir);
        };
        defer {
            std.fs.cwd().deleteTree(tmp_dir) catch |err| {
                std.debug.print("Error deleting tmp dir: {}\n", .{err});
            };
        }

        var token_stream = try file_utils.TokenStream(file_utils.token_64t).init(
            filename,
            output_filename,
            self.gpa.allocator(),
            1,
            0,
            ' ',
        );

        var file_pos: usize = 0;
        var file_terms = std.AutoHashMap(u32, void).init(self.gpa.allocator());
        defer file_terms.deinit();

        const start = std.time.milliTimestamp();
        var doc_size: usize = 0;
        const file_size = try token_stream.input_file.getEndPos();
        while (file_pos < file_size - 1) {
            const buffer = try token_stream.getBuffer(file_pos);
            file_pos += try self.tokenize(
                buffer, 
                &token_stream, 
                &file_terms, 
                &doc_size,
                );
            std.debug.print("File pos: {d}\n", .{file_pos});
        }
        try token_stream.flushTokenStream(0);
        const end = std.time.milliTimestamp();
        const execution_time_ms = end - start;
        const mb_s: usize = @as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(execution_time_ms))));
        std.debug.print("{d}MB/s\n", .{mb_s});

        try self.updateFromTokenStream(&token_stream);
    }

    pub fn updateFromTokenStream(
        self: *FlatFileIndexManager,
        token_stream: *file_utils.TokenStream(file_utils.token_64t),
        ) !void {
        // This works like constructFromTokenStream, but all will have the same doc_id.
        // This works incrementally.
        try self.postings.doc_ids.resize(self.vocab.count());
        try self.postings.term_positions.resize(self.vocab.count());

        var avg_doc_size: f64 = 0.0;
        for (self.doc_sizes.items) |doc_size| {
            avg_doc_size += @floatFromInt(doc_size);
        }
        avg_doc_size /= @floatFromInt(self.doc_sizes.items.len);
        self.avg_doc_size = @floatCast(avg_doc_size);

        // Create index.
        const output_file = &token_stream.output_files[0];
        try output_file.seekTo(0);

        const tokens = token_stream.tokens[0];

        var bytes_read: usize = 0;
        var token_count: usize = 0;

        var num_tokens: u32 = file_utils.TOKEN_STREAM_CAPACITY;
        const current_doc_id: u32 = @truncate(self.doc_sizes.items.len);

        while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
            var _num_tokens: [4]u8 = undefined;
            _ = try output_file.read(std.mem.asBytes(&_num_tokens));
            num_tokens = std.mem.readInt(u32, &_num_tokens, index.ENDIANESS);

            bytes_read = try output_file.read(
                std.mem.sliceAsBytes(tokens[0..num_tokens])
                );

            for (0..num_tokens) |idx| {
                const token = tokens[idx];

                const term_pos = token.term_pos;
                const term_id: usize = @intCast(token.term_id);

                const prev_doc_id = self.postings.doc_ids.items[term_id].getLast();

                if (current_doc_id == prev_doc_id) {
                    self.postings.doc_ids.items[term_id].getLast().run_length += 1;
                } else {
                    try self.postings.doc_ids.items[term_id].append(
                        self.postings.doc_ids.allocator,
                        RLE(u32).init(),
                        );
                }
                try self.postings.term_positions.items[term_id].append(
                    self.postings.term_positions.allocator,
                    @intCast(term_pos)
                    );

                token_count += 1;
            }
        }

        try self.doc_sizes.append(@intCast(token_count));
    }

    pub fn _query(
        self: *FlatFileIndexManager,
        query_str: []const u8,
        query_results: *SortedScoreMultiArray(manager.QueryResult),
    ) !void {
        const num_search_cols = self.search_cols.count();
        std.debug.assert(num_search_cols > 0);

        // Tokenize query.
        var tokens = std.ArrayList(FlatToken).init(self.gpa.allocator());
        defer tokens.deinit();

        var term_buffer: [manager.MAX_TERM_LENGTH]u8 = undefined;

        var empty_query = true; 

        var term_len: usize = 0;
        var term_pos: u32 = 0;
        for (query_str) |c| {
            switch (c) {
                0...33, 35...47, 58...64, 91...96, 123...126 => {
                    if (term_len == 0) continue;

                    const token = self.vocab.get(
                        term_buffer[0..term_len]
                        );
                    if (token != null) {
                        try tokens.append(FlatToken{
                            .token = token.?,
                            .term_pos = term_pos,
                        });
                        term_pos += 1;
                        empty_query = false;
                    }
                    term_len = 0;
                    continue;
                },
                else => {
                    term_buffer[term_len] = std.ascii.toUpper(c);
                    term_len += 1;

                    if (term_len == index.MAX_TERM_LENGTH) {
                        const token = self.vocab.get(
                            term_buffer[0..term_len]
                            );
                        if (token != null) {
                            try tokens.append(FlatToken{
                                .token = token.?,
                                .term_pos = term_pos,
                            });
                            term_pos += 1;
                            empty_query = false;
                        }
                        term_len = 0;
                    }
                },
            }
        }

        if (term_len > 0) {
            const token = self.vocab.get(
                term_buffer[0..term_len]
                );
            if (token != null) {
                try tokens.append(FlatToken{
                    .token = token.?,
                    .term_pos = term_pos,
                });

                term_pos += 1;
                empty_query = false;
            }
        }
        if (empty_query) return;

        // For each token in each II, get relevant docs and add to score.
        var doc_scores: *std.AutoHashMap(u32, FlatScoringInfo) = &self.doc_score_map;
        doc_scores.clearRetainingCapacity();

        var sorted_scores = try SortedScoreMultiArray(void).init(
            self.gpa.allocator(), 
            query_results.capacity,
            );
        defer sorted_scores.deinit();


        var token_scores = try self.gpa.allocator().alloc(f32, tokens.items.len);
        defer self.gpa.allocator().free(token_scores);

        var idf_remaining: f32 = 0.0;
        for (0.., tokens.items) |idx, _token| {
            const token: usize = @intCast(_token.token);
            const inner_term = @as(f32, @floatFromInt(self.doc_sizes.items.len)) / 
                               @as(f32, @floatFromInt(self.doc_freqs.items[token]));
            const idf: f32 = 1.0 + std.math.log2(inner_term);
            token_scores[idx] = idf;

            idf_remaining += idf;
        }
        const idf_sum = idf_remaining;

        var done = false;

        var last_II_idx: usize = 0;
        for (0..tokens.items.len) |idx| {
            const score = token_scores[idx];
            const col_score_pair = tokens.items[idx];

            const II_idx = @as(usize, @intCast(col_score_pair.col_idx));
            const token   = @as(usize, @intCast(col_score_pair.token));

            const II = &self.index_partitions[partition_idx].II[II_idx];

            const offset      = II.term_offsets[token];
            const last_offset = II.term_offsets[token + 1];

            const is_high_df_term: bool = (
                score < 0.4 * idf_sum / @as(f32, @floatFromInt(tokens.items.len))
                );

            var prev_doc_id: u32 = std.math.maxInt(u32);
            // for (II.postings[offset..last_offset]) |doc_token| {
            for (
                II.postings.doc_ids[offset..last_offset],
                II.postings.term_positions[offset..last_offset],
                ) |doc_id, term_pos| {
                // const doc_id:   u32 = @intCast(doc_token.doc_id);
                // const term_pos: u8  = @intCast(doc_token.term_pos);

                prev_doc_id = doc_id;

                const _result = doc_scores.getPtr(doc_id);
                if (_result) |result| {
                    // TODO: Consider front of record boost.

                    // Phrase boost.
                    const last_term_pos = result.*.term_pos;
                    result.*.score += @as(f32, @floatFromInt(@intFromBool((term_pos == last_term_pos + 1) and (II_idx == last_II_idx) and (doc_id == prev_doc_id)))) * score * 0.75;

                    // Does tf scoring effectively.
                    result.*.score += score;

                    result.*.term_pos = term_pos;

                    const score_copy = result.*.score;
                    sorted_scores.insert({}, score_copy);

                } else {
                    if (!done and !is_high_df_term and !col_score_pair.shallow_query) {

                        if (sorted_scores.count == sorted_scores.capacity - 1) {
                            const min_score = sorted_scores.scores[sorted_scores.count - 1];
                            if (min_score > idf_remaining) {
                                done = true;
                                continue;
                            }
                        }

                        try doc_scores.put(
                            doc_id,
                            ScoringInfo{
                                .score = score,
                                .term_pos = term_pos,
                            }
                        );
                        sorted_scores.insert({}, score);
                    }
                }
            }

            // std.debug.print("TOTAL TERMS SCORED: {d}\n", .{doc_scores.count()});
            // std.debug.print("WAS HIGH DF TERM:   {}\n\n", .{is_high_df_term});
            idf_remaining -= score;
            last_II_idx = II_idx;
        }

        // std.debug.print("\nTOTAL TERMS SCORED: {d}\n", .{doc_scores.count()});

        var score_it = doc_scores.iterator();
        while (score_it.next()) |entry| {

            const result = QueryResult{
                .doc_id = entry.key_ptr.*,
                .partition_idx = @intCast(partition_idx),
            };
            query_results.insert(result, entry.value_ptr.*.score);
        }
    }


    pub fn query(
        self: *FlatFileIndexManager,
        query_str: []const u8,
        k: usize,
        boost_factors: std.ArrayList(f32),
    ) !void {
        if (k > manager.MAX_NUM_RESULTS) {
            std.debug.print("k must be less than or equal to {d}\n", .{manager.MAX_NUM_RESULTS});
            return error.InvalidArgument;
        }

        self.results_arrays.clear();
        self.results_arrays.resize(k);
        self._query(
            query_str,
            boost_factors,
            &self.results_arrays,
        );

        std.debug.print("FOUND {d} results\n", .{self.results_arrays.count});
        if (self.results_arrays.count == 0) return;

        for (0..self.results_arrays.count) |idx| {
            const result = self.results_arrays.items[idx];

            std.debug.assert(self.result_strings[idx].capacity > 0);
            try self.fetchRecords(
                self.result_positions[idx],
                &self.file_handles,
                result,
                &self.result_strings[idx],
                self.file_type,
                &self.col_map,
            );
            // std.debug.print("Score {d}: {d} - Doc id: {d}\n", .{idx, self.results_arrays[0].scores[idx], self.results_arrays[0].items[idx].doc_id});
        }
        std.debug.print("\n", .{});
    }
};


test "index_txt" {
    // Treat as txt flat file.
    const filename: []const u8 = "../data/mb_small.csv";
    // const filename: []const u8 = "../data/mb.csv";

    var index_manager = try FlatFileIndexManager.init();
    defer index_manager.deinit();

    try index_manager.indexFile(filename);

    std.debug.print("Vocab size: {d}\n", .{index_manager.vocab.count()});
}
