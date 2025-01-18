const std   = @import("std");
const print = std.debug.print;

const StaticIntegerSet = @import("static_integer_set.zig").StaticIntegerSet;

const progress = @import("progress.zig");

const csv = @import("csv.zig");

const TermPos = @import("server.zig").TermPos;

const BM25Partition   = @import("index.zig").BM25Partition;
const InvertedIndex   = @import("index.zig").InvertedIndex;
const QueryResult     = @import("index.zig").QueryResult;
const ScoringInfo     = @import("index.zig").ScoringInfo;
const MAX_TERM_LENGTH = @import("index.zig").MAX_TERM_LENGTH;
const MAX_NUM_TERMS   = @import("index.zig").MAX_NUM_TERMS;

// const SortedScoreArray = @import("sorted_array.zig").SortedScoreArray;
const SortedScoreMultiArray = @import("sorted_array.zig").SortedScoreMultiArray;
const ScorePair        = @import("sorted_array.zig").ScorePair;

const AtomicCounter = std.atomic.Value(u64);

pub const MAX_NUM_RESULTS = 1000;
const IDF_THRESHOLD: f32  = 1.0 + std.math.log2(100);
// const IDF_THRESHOLD: f32  = 1.0 + std.math.log2(10);

const Column = struct {
    csv_idx: usize,
    II_idx: usize,
};

const ColTokenPair = packed struct {
    col_idx: u24,
    term_pos: u8,
    token: u32,
};

const score_f32 = struct {
    score: f32,
};


pub const IndexManager = struct {
    index_partitions: []BM25Partition,
    input_filename: []const u8,
    gpa: std.heap.GeneralPurposeAllocator(.{}),
    string_arena: std.heap.ArenaAllocator,
    search_cols: std.StringHashMap(Column),
    cols: std.ArrayList([]const u8),
    file_handles: []std.fs.File,
    tmp_dir: []const u8,
    result_positions: [MAX_NUM_RESULTS][]TermPos,
    result_strings: [MAX_NUM_RESULTS]std.ArrayList(u8),
    results_arrays: []SortedScoreMultiArray(QueryResult),
    // thread_pool: std.Thread.Pool,
    header_bytes: usize,

    pub fn init(
        input_filename: []const u8,
        // search_cols: *std.ArrayList([]u8),
        ) !IndexManager {
        var gpa = std.heap.GeneralPurposeAllocator(.{.thread_safe = true}){};
        var string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);

        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(input_filename, &hash, .{});
            break :blk hash;
        };
        const dir_name = try std.fmt.allocPrint(
            gpa.allocator(),
            ".{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
            );

        std.fs.cwd().makeDir(dir_name) catch {
            try std.fs.cwd().deleteTree(dir_name);
            try std.fs.cwd().makeDir(dir_name);
        };

        return IndexManager{
            .index_partitions = undefined,
            .input_filename = input_filename,
            .gpa = gpa,
            .string_arena = string_arena,
            .search_cols = std.StringHashMap(Column).init(string_arena.allocator()),
            .cols = std.ArrayList([]const u8).init(string_arena.allocator()),
            .tmp_dir = dir_name,
            .file_handles = undefined,
            .result_positions = undefined,
            .result_strings = undefined,
            .results_arrays = undefined,
            .header_bytes = undefined,
        };
    }
    
    pub fn deinit(self: *IndexManager) !void {
        for (0..self.index_partitions.len) |idx| {
            self.results_arrays[idx].deinit();
            self.index_partitions[idx].deinit();
            self.file_handles[idx].close();
        }
        self.gpa.allocator().free(self.file_handles);
        self.gpa.allocator().free(self.index_partitions);
        self.gpa.allocator().free(self.results_arrays);

        self.search_cols.deinit();
        self.cols.deinit();

        try std.fs.cwd().deleteTree(self.tmp_dir);
        self.gpa.allocator().free(self.tmp_dir);

        for (0..MAX_NUM_RESULTS) |idx| {
            self.gpa.allocator().free(self.result_positions[idx]);
            self.result_strings[idx].deinit();
        }

        self.string_arena.deinit();
        _ = self.gpa.deinit();
    }

    pub fn readHeader(self: *IndexManager, search_cols: *std.ArrayList([]u8)) !void {
        self.search_cols = try readCSVHeader(
            self.input_filename, 
            search_cols, 
            &self.cols,
            self.gpa.allocator(),
            self.string_arena.allocator(),
            &self.header_bytes,
            );
        std.debug.assert(self.cols.items.len > 0);

        var result_positions: [MAX_NUM_RESULTS][]TermPos = undefined;
        var result_strings: [MAX_NUM_RESULTS]std.ArrayList(u8) = undefined;
        for (0..MAX_NUM_RESULTS) |idx| {
            result_positions[idx] = try self.gpa.allocator().alloc(TermPos, self.cols.items.len);
            result_strings[idx] = try std.ArrayList(u8).initCapacity(self.gpa.allocator(), 4096);
            try result_strings[idx].resize(4096);
        }
    }

    pub fn printDebugInfo(self: *const IndexManager) !void {
        std.debug.print("\n=====================================================\n", .{});

        var num_terms: usize = 0;
        var num_docs: usize = 0;
        var avg_doc_size: f32 = 0.0;

        var num_terms_all_cols = std.ArrayList(u32).init(self.gpa.allocator());
        defer num_terms_all_cols.deinit();

        for (0..self.index_partitions.len) |idx| {

            num_docs += self.index_partitions[idx].II[0].num_docs;
            for (0..self.index_partitions[idx].II.len) |jdx| {
                num_terms    += self.index_partitions[idx].II[jdx].num_terms;
                avg_doc_size += self.index_partitions[idx].II[jdx].avg_doc_size;

                try num_terms_all_cols.append(self.index_partitions[idx].II[jdx].num_terms);
            }

        }

        std.debug.print("Num Partitions:  {d}\n", .{self.index_partitions.len});

        var col_iterator = self.search_cols.iterator();
        var idx: usize = 0;
        while (col_iterator.next()) |item| {
            std.debug.print("Column:          {s}\n", .{item.key_ptr.*});
            std.debug.print("---------------------------------------------\n", .{});
            std.debug.print("Num terms:       {d}\n", .{num_terms_all_cols.items[idx]});
            std.debug.print("Num docs:        {d}\n", .{num_docs});
            std.debug.print("Avg doc size:    {d}\n\n", .{avg_doc_size / @as(f32, @floatFromInt(self.index_partitions.len))});

            idx += 1;
        }

        std.debug.print("=====================================================\n\n\n\n", .{});
    }


    fn readCSVHeader(
        input_filename: []const u8,
        search_cols: *std.ArrayList([]u8),
        cols: *std.ArrayList([]const u8),
        allocator: std.mem.Allocator,
        string_arena: std.mem.Allocator,
        num_bytes: *usize,
        ) !std.StringHashMap(Column) {

        var col_map = std.StringHashMap(Column).init(allocator);
        var col_idx: usize = 0;

        const file = try std.fs.cwd().openFile(input_filename, .{});
        defer file.close();

        const file_size = try file.getEndPos();

        const f_data = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0
        );
        defer std.posix.munmap(f_data);

        var term: [MAX_TERM_LENGTH]u8 = undefined;
        var cntr: usize = 0;

        for (search_cols.items) |*col| {
            _ = std.ascii.upperString(col.*, col.*);
        }


        var byte_idx: usize = 0;
        line: while (true) {
            const is_quoted = f_data[byte_idx] == '"';
            byte_idx += @intFromBool(is_quoted);

            while (true) {
                if (is_quoted) {

                    if (f_data[byte_idx] == '"') {
                        byte_idx += 2;
                        if (f_data[byte_idx - 1] != '"') {
                            // ADD TERM
                            try cols.append(
                                try string_arena.dupe(u8, term[0..cntr])
                                );

                            for (0..search_cols.items.len) |idx| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[idx])) {
                                    std.debug.print("Found search col: {s}\n", .{search_cols.items[idx]});
                                    const copy_term = try string_arena.dupe(u8, term[0..cntr]);
                                    try col_map.put(
                                        copy_term, 
                                        Column{
                                            .csv_idx = col_idx,
                                            .II_idx = col_map.count(),
                                            },
                                        );
                                    break;
                                }
                            }
                            cntr = 0;
                            byte_idx += 1;
                            col_idx += 1;
                            continue :line;
                        }
                    } else {
                        // Add char.
                        term[cntr] = std.ascii.toUpper(f_data[byte_idx]);
                        cntr += 1;
                        byte_idx += 1;
                    }

                } else {

                    switch (f_data[byte_idx]) {
                        ',', '\n' => {
                            // ADD TERM.
                            try cols.append(
                                try string_arena.dupe(u8, term[0..cntr])
                                );

                            for (0..search_cols.items.len) |idx| {
                                if (std.mem.eql(u8, term[0..cntr], search_cols.items[idx])) {
                                    std.debug.print("Found search col: {s}\n", .{search_cols.items[idx]});
                                    const copy_term = try string_arena.dupe(u8, term[0..cntr]);
                                    try col_map.put(
                                        copy_term, 
                                        Column{
                                            .csv_idx = col_idx,
                                            .II_idx = col_map.count(),
                                            },
                                        );
                                    break;
                                }
                            }
                            if (f_data[byte_idx] == '\n') {
                                num_bytes.* = byte_idx + 1;
                                return col_map;
                            }

                            cntr = 0;
                            byte_idx += 1;
                            col_idx += 1;
                            continue :line;
                        },
                        else => {
                            // Add char
                            term[cntr] = std.ascii.toUpper(f_data[byte_idx]);
                            cntr += 1;
                            byte_idx += 1;
                        }
                    }
                }
            }

            col_idx += 1;
        }

        num_bytes.* = byte_idx;
    }
        

    fn readPartition(
        self: *IndexManager,
        partition_idx: usize,
        chunk_size: usize,
        final_chunk_size: usize,
        num_partitions: usize,
        num_search_cols: usize,
        search_col_idxs: []usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 30;

        const current_chunk_size = switch (partition_idx != num_partitions - 1) {
            true => chunk_size,
            false => final_chunk_size,
        };

        const start_doc = partition_idx * chunk_size;
        const end_doc   = start_doc + current_chunk_size;

        const output_filename = try std.fmt.allocPrint(
            self.gpa.allocator(), 
            "{s}/output_{d}", 
            .{self.tmp_dir, partition_idx}
            );
        defer self.gpa.allocator().free(output_filename);

        var token_stream = try csv.TokenStream.init(
            self.input_filename,
            output_filename,
            self.gpa.allocator(),
            search_col_idxs.len,
        );
        defer token_stream.deinit();

        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

        // Sort search_col_idxs
        std.sort.insertion(usize, search_col_idxs, {}, comptime std.sort.asc(usize));

        const start_byte = self.index_partitions[partition_idx].line_offsets[0];
        try token_stream.input_file.seekTo(start_byte);

        const bytes_read = try token_stream.input_file.read(token_stream.f_data);
        if (bytes_read < token_stream.f_data.len) {
            // Add newline charachter to end of file to ensure last line is parsed correctly.
            token_stream.f_data[bytes_read] = '\n';
        }

        csv.stringToUpper(token_stream.f_data[0..].ptr, token_stream.f_data.len);

        var prev_doc_id: usize = 0;
        for (0.., start_doc..end_doc) |doc_id, _| {

            if (timer.read() >= interval_ns) {
                const current_docs_read = total_docs_read.fetchAdd(
                    doc_id - prev_doc_id, 
                    .monotonic
                    ) + (doc_id - prev_doc_id);
                prev_doc_id = doc_id;
                timer.reset();

                if (partition_idx == 0) {
                    progress_bar.update(current_docs_read);
                }
            }

            // const line_offset = self.index_partitions[partition_idx].line_offsets[doc_id];
            // const next_line_offset = self.index_partitions[partition_idx].line_offsets[doc_id + 1];

            var search_col_idx: usize = 0;
            var prev_col: usize = 0;

            while (search_col_idx < num_search_cols) {
                for (prev_col..search_col_idxs[search_col_idx]) |_| {
                    token_stream.iterFieldCSV(&token_stream.buffer_idx);
                    try token_stream.incBufferIdx();
                }

                try self.index_partitions[partition_idx].processDocRfc4180(
                    &token_stream,
                    @intCast(doc_id), 
                    search_col_idx,
                    &terms_seen_bitset,
                    );

                // Add one because we just iterated over the last field.
                prev_col = search_col_idxs[search_col_idx] + 1;
                search_col_idx += 1;
            }

            for (prev_col..self.cols.items.len) |_| {
                token_stream.iterFieldCSV(&token_stream.buffer_idx);
                try token_stream.incBufferIdx();
            }
        }

        // Flush remaining tokens.
        for (0..token_stream.num_terms.len) |search_col_idx| {
            try token_stream.flushTokenStream(search_col_idx);
        }
        _ = total_docs_read.fetchAdd(end_doc - (start_doc + prev_doc_id), .monotonic);

        // Construct II
        try self.index_partitions[partition_idx].constructFromTokenStream(&token_stream);
    }


    pub fn readFile(self: *IndexManager) !void {
        const file = try std.fs.cwd().openFile(self.input_filename, .{});
        defer file.close();

        const file_size = try file.getEndPos();

        const f_data = try std.posix.mmap(
            null,
            file_size,
            std.posix.PROT.READ,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0
        );
        defer std.posix.munmap(f_data);

        // const f_data = try self.gpa.allocator().alignedAlloc(u8, @intCast(std.mem.page_size), file_size);
        // defer self.gpa.allocator().free(f_data);

        // const read_start = std.time.milliTimestamp();
        // const num_bytes = try file.readAll(f_data);
        // std.debug.assert(num_bytes == file_size);
        // const read_end = std.time.milliTimestamp();
        // std.debug.print("Read file in {d}ms\n", .{read_end - read_start});
        // std.debug.print("{d}MB/s\n", .{@as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(read_end - read_start))))});

        // const read_end = std.time.milliTimestamp();
        // std.debug.print("Read file in {d}ms\n", .{read_end - read_start});
        // std.debug.print("{d}MB/s\n", .{@as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(read_end - read_start))))});


        var line_offsets = std.ArrayList(usize).init(self.gpa.allocator());
        defer line_offsets.deinit();

        var file_pos: usize = self.header_bytes;

        // Time read.
        const start_time = std.time.milliTimestamp();

        while (file_pos < file_size - 1) {
            try line_offsets.append(file_pos);
            try csv.iterLineCSV(f_data, &file_pos);
        }
        try line_offsets.append(file_size);

        const end_time = std.time.milliTimestamp();
        const execution_time_ms = end_time - start_time;
        const mb_s: usize = @as(usize, @intFromFloat(0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(execution_time_ms))));

        const num_lines = line_offsets.items.len - 1;

        const num_partitions = if (num_lines > 50_000) try std.Thread.getCpuCount() else 1;

        self.file_handles = try self.gpa.allocator().alloc(std.fs.File, num_partitions);
        self.index_partitions = try self.gpa.allocator().alloc(BM25Partition, num_partitions);
        self.results_arrays = try self.gpa.allocator().alloc(SortedScoreMultiArray(QueryResult), num_partitions);
        for (0..num_partitions) |idx| {
            self.file_handles[idx]   = try std.fs.cwd().openFile(self.input_filename, .{});
            self.results_arrays[idx] = try SortedScoreMultiArray(QueryResult).init(self.gpa.allocator(), MAX_NUM_RESULTS);
        }

        std.debug.print("Writing {d} partitions\n", .{num_partitions});

        std.debug.print("Read {d} lines in {d}ms\n", .{num_lines, execution_time_ms});
        std.debug.print("{d}MB/s\n", .{mb_s});

        const chunk_size: usize = num_lines / num_partitions;
        const final_chunk_size: usize = chunk_size + (num_lines % num_partitions);

        var partition_boundaries = std.ArrayList(usize).init(self.gpa.allocator());
        defer partition_boundaries.deinit();

        const num_search_cols = self.search_cols.count();

        for (0..num_partitions) |i| {
            try partition_boundaries.append(line_offsets.items[i * chunk_size]);

            const current_chunk_size = switch (i != num_partitions - 1) {
                true => chunk_size,
                false => final_chunk_size,
            };

            const start = i * chunk_size;
            const end   = start + current_chunk_size + 1;

            const partition_line_offsets = try self.gpa.allocator().alloc(usize, current_chunk_size + 1);
            @memcpy(partition_line_offsets, line_offsets.items[start..end]);

            self.index_partitions[i] = try BM25Partition.init(
                self.gpa.allocator(), 
                num_search_cols, 
                partition_line_offsets
                );
        }
        try partition_boundaries.append(file_size);

        std.debug.assert(partition_boundaries.items.len == num_partitions + 1);

        const search_col_idxs = try self.gpa.allocator().alloc(usize, num_search_cols);
        defer self.gpa.allocator().free(search_col_idxs);
        
        var map_it = self.search_cols.iterator();
        var idx: usize = 0;
        while (map_it.next()) |*item| {
            search_col_idxs[idx] = item.value_ptr.csv_idx;
            idx += 1;
        }

        const time_start = std.time.milliTimestamp();

        var threads = try self.gpa.allocator().alloc(std.Thread, num_partitions);
        defer self.gpa.allocator().free(threads);

        var total_docs_read = AtomicCounter.init(0);
        var progress_bar = progress.ProgressBar.init(num_lines);

        for (0..num_partitions) |partition_idx| {

            threads[partition_idx] = try std.Thread.spawn(
                .{},
                readPartition,
                .{
                    self,
                    partition_idx,
                    chunk_size,
                    final_chunk_size,
                    num_partitions,
                    num_search_cols,
                    search_col_idxs,
                    &total_docs_read,
                    &progress_bar,
                    },
                );
        }

        for (threads) |thread| {
            thread.join();
        }

        const _total_docs_read = total_docs_read.load(.acquire);
        std.debug.assert(_total_docs_read == num_lines);
        progress_bar.update(_total_docs_read);

        const time_end = std.time.milliTimestamp();
        const time_diff = time_end - time_start;
        std.debug.print("Processed {d} documents in {d}ms\n", .{_total_docs_read, time_diff});
    }

    pub fn queryPartitionOrdered(
        self: *IndexManager,
        queries: std.StringHashMap([]const u8),
        boost_factors: std.ArrayList(f32),
        partition_idx: usize,
        query_results: *SortedScoreMultiArray(QueryResult),
    ) !void {
        const num_search_cols = self.search_cols.count();
        std.debug.assert(num_search_cols > 0);

        // Tokenize query.
        var tokens = std.ArrayList(ColTokenPair).init(self.gpa.allocator());
        defer tokens.deinit();

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;

        var empty_query = true; 

        var query_it = queries.iterator();
        while (query_it.next()) |entry| {
            const _col_idx = self.search_cols.get(entry.key_ptr.*);
            if (_col_idx == null) continue;
            const col_idx = _col_idx.?.II_idx;

            var term_len: usize = 0;

            var term_pos: u8 = 0;
            for (entry.value_ptr.*) |c| {
                switch (c) {
                    0...33, 35...47, 58...64, 91...96, 123...126 => {
                        if (term_len == 0) continue;

                        const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                            term_buffer[0..term_len]
                            );
                        if (token != null) {
                            try tokens.append(ColTokenPair{
                                .col_idx = @intCast(col_idx),
                                .term_pos = term_pos,
                                .token = token.?,
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

                        if (term_len == MAX_TERM_LENGTH) {
                            const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                                term_buffer[0..term_len]
                                );
                            if (token != null) {
                                try tokens.append(ColTokenPair{
                                    .col_idx = @intCast(col_idx),
                                    .term_pos = term_pos,
                                    .token = token.?,
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
                const token = self.index_partitions[partition_idx].II[col_idx].vocab.get(
                    term_buffer[0..term_len]
                    );
                if (token != null) {
                    try tokens.append(ColTokenPair{
                        .col_idx = @intCast(col_idx),
                        .term_pos = term_pos,
                        .token = token.?,
                    });
                    term_pos += 1;
                    empty_query = false;
                }
            }
        }

        if (empty_query) {
            // std.debug.print("EMPTY QUERY\n", .{}); 
            return;
        }

        // For each token in each II, get relevant docs and add to score.
        var doc_scores: *std.AutoHashMap(u32, ScoringInfo) = &self.index_partitions[partition_idx].doc_score_map;
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
            const col_idx: usize = @intCast(_token.col_idx);
            const token:   usize = @intCast(_token.token);

            const II: *InvertedIndex = &self.index_partitions[partition_idx].II[col_idx];
            const boost_weighted_idf: f32 = (
                1.0 + std.math.log2(@as(f32, @floatFromInt(II.num_docs)) / @as(f32, @floatFromInt(II.doc_freqs.items[token])))
                ) * boost_factors.items[col_idx];
            token_scores[idx] = boost_weighted_idf;

            idf_remaining += boost_weighted_idf;
        }
        const idf_sum = idf_remaining;

        var done = false;

        var last_col_idx: usize = 0;
        for (0..tokens.items.len) |idx| {
            const score = token_scores[idx];
            const col_score_pair = tokens.items[idx];

            const col_idx = @as(usize, @intCast(col_score_pair.col_idx));
            const token   = @as(usize, @intCast(col_score_pair.token));

            const II: *InvertedIndex = &self.index_partitions[partition_idx].II[col_idx];

            const offset      = II.term_offsets[token];
            const last_offset = II.term_offsets[token + 1];

            // const is_high_df_term: bool = (score < IDF_THRESHOLD) or
                                          // (score < 0.4 * idf_sum / @as(f32, @floatFromInt(tokens.items.len)));
            const is_high_df_term: bool = (score < 0.4 * idf_sum / @as(f32, @floatFromInt(tokens.items.len)));

            var prev_doc_id: u32 = std.math.maxInt(u32);
            for (II.postings[offset..last_offset]) |doc_token| {
                const doc_id:   u32 = @intCast(doc_token.doc_id);
                const term_pos: u8  = @intCast(doc_token.term_pos);

                prev_doc_id = doc_id;

                const _result = doc_scores.getPtr(doc_id);
                if (_result) |result| {
                    // TODO: Consider front of record boost.

                    // Phrase boost.
                    const last_term_pos = result.*.term_pos;
                    result.*.score += @as(f32, @floatFromInt(@intFromBool((term_pos == last_term_pos + 1) and (col_idx == last_col_idx) and (doc_id == prev_doc_id)))) * score * 0.75;

                    // Does tf scoring effectively.
                    result.*.score += score;

                    result.*.term_pos = term_pos;

                    const score_copy = result.*.score;
                    sorted_scores.insert({}, score_copy);

                } else {
                    if (!done and !is_high_df_term) {

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
            last_col_idx = col_idx;
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
        self: *IndexManager,
        queries: std.StringHashMap([]const u8),
        k: usize,
        boost_factors: std.ArrayList(f32),
    ) !void {
        if (k > MAX_NUM_RESULTS) {
            std.debug.print("k must be less than or equal to {d}\n", .{MAX_NUM_RESULTS});
            return error.InvalidArgument;
        }

        // Init num_partitions threads.
        const num_partitions = self.index_partitions.len;
        var threads = try self.gpa.allocator().alloc(std.Thread, num_partitions);
        defer self.gpa.allocator().free(threads);

        for (0..num_partitions) |partition_idx| {
            self.results_arrays[partition_idx].clear();
            self.results_arrays[partition_idx].resize(k);
            threads[partition_idx] = try std.Thread.spawn(
                .{},
                queryPartitionOrdered,
                .{
                    self,
                    queries,
                    boost_factors,
                    partition_idx,
                    &self.results_arrays[partition_idx],
                },
            );
        }

        for (threads) |thread| {
            thread.join();
        }

        if (self.index_partitions.len > 1) {
            for (self.results_arrays[1..]) |*tr| {
                for (0.., tr.items[0..tr.count]) |idx, r| {
                    self.results_arrays[0].insert(r, tr.scores[idx]);
                }
            }
        }
        if (self.results_arrays[0].count == 0) return;

        for (0..self.results_arrays[0].count) |idx| {
            const result = self.results_arrays[0].items[idx];

            try self.index_partitions[result.partition_idx].fetchRecords(
                self.result_positions[idx],
                &self.file_handles[result.partition_idx],
                result,
                @constCast(&self.result_strings[idx]),
            );
            std.debug.print("Score {d}: {d} - Doc id: {d}\n", .{idx, self.results_arrays[0].scores[idx], self.results_arrays[0].items[idx].doc_id});
        }
        std.debug.print("\n", .{});
    }
};
