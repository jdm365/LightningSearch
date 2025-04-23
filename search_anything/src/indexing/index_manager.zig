const std = @import("std");

const string_utils = @import("../utils/string_utils.zig");
const fu           = @import("../storage/file_utils.zig");

const StaticIntegerSet = @import("../utils/static_integer_set.zig").StaticIntegerSet;

const progress = @import("../utils/progress.zig");

const csv  = @import("../parsing/csv.zig");
const json = @import("../parsing/json.zig");
const pq   = @import("../parsing/parquet.zig");

const rt = @import("../utils/radix_trie.zig");

const TermPos = @import("../server/server.zig").TermPos;

const BM25Partition   = @import("index.zig").BM25Partition;
const QueryResult     = @import("index.zig").QueryResult;
const ScoringInfo     = @import("index.zig").ScoringInfo;
const MAX_TERM_LENGTH = @import("index.zig").MAX_TERM_LENGTH;
const MAX_NUM_TERMS   = @import("index.zig").MAX_NUM_TERMS;


const SortedScoreMultiArray = @import("../utils/sorted_array.zig").SortedScoreMultiArray;
const ScorePair             = @import("../utils/sorted_array.zig").ScorePair;
const findSorted            = @import("../utils/misc_utils.zig").findSorted;

const AtomicCounter = std.atomic.Value(u64);

pub const MAX_NUM_RESULTS = 1000;
const IDF_THRESHOLD: f32  = 1.0 + std.math.log2(100);

// const MAX_NUM_THREADS: usize = 1;
const MAX_NUM_THREADS: usize = std.math.maxInt(usize);


const Column = struct {
    csv_idx: usize,
    II_idx: usize,
};

const ColTokenPair = struct {
    col_idx: u32,
    token: u32,
    term_pos: u8,
    shallow_query: bool,
};

pub const IndexManager = struct {
    index_partitions: []BM25Partition,
    allocators: Allocators,
    file_data: FileData,
    query_state: QueryState,
    indexing_state: IndexingState,

    columns: rt.RadixTrie(u32),
    search_col_idxs: std.ArrayListUnmanaged(u32),

    const Allocators = struct {
        gpa: *std.heap.GeneralPurposeAllocator(.{.thread_safe = true}),
        string_arena: *std.heap.ArenaAllocator,
        scratch_arena: *std.heap.ArenaAllocator,
    };

    const PQData = struct {
        pq_file_handle: *anyopaque,
        num_row_groups: usize,
    };

    const FileData = struct {
        input_filename_c: [*:0]const u8,
        tmp_dir: []const u8,
        file_handles: []std.fs.File,
        file_type: fu.FileType,
        pq_data: ?PQData = null,
        header_bytes: ?usize = null,

        inline fn inputFilename(self: *FileData) []const u8 {
            return std.mem.span(self.input_filename_c);
        }
    };

    const QueryState = struct {
        result_positions: [MAX_NUM_RESULTS][]TermPos,
        result_strings: [MAX_NUM_RESULTS]std.ArrayListUnmanaged(u8),
        results_arrays: []SortedScoreMultiArray(QueryResult),
        thread_pool: std.Thread.Pool,
    };

    const IndexingState = struct {
        last_progress: usize,
    };

    pub fn init(allocator: std.mem.Allocator) !IndexManager {
        var manager = IndexManager{
            .index_partitions = undefined,

            .allocators = Allocators{
                .gpa           = try allocator.create(std.heap.GeneralPurposeAllocator(.{.thread_safe = true})),
                .string_arena  = try allocator.create(std.heap.ArenaAllocator),
                .scratch_arena = try allocator.create(std.heap.ArenaAllocator),
            },

            .file_data = FileData{
                .input_filename_c = undefined,
                .tmp_dir          = undefined,
                .file_handles     = undefined,
                .file_type        = undefined,
            },

            .query_state = QueryState{
                .result_positions = undefined,
                .result_strings   = undefined,
                .results_arrays   = undefined,
                .thread_pool      = undefined,
            },

            .indexing_state = IndexingState{
                .last_progress = 0,
            },

            .columns         = undefined,
            .search_col_idxs = std.ArrayListUnmanaged(u32){},
        };
        manager.allocators.gpa.*           = std.heap.GeneralPurposeAllocator(.{.thread_safe = true}){};
        manager.allocators.string_arena.*  = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        manager.allocators.scratch_arena.* = std.heap.ArenaAllocator.init(std.heap.page_allocator);

        try manager.search_col_idxs.ensureTotalCapacity(
            manager.allocators.gpa.allocator(), 
            4,
            );
        manager.columns = try rt.RadixTrie(u32).initCapacity(
            manager.allocators.gpa.allocator(),
            32,
            );

        return manager;
    }

    pub inline fn gpa(self: *IndexManager) std.mem.Allocator {
        return self.allocators.gpa.allocator();
    }

    pub inline fn scratchArena(self: *IndexManager) std.mem.Allocator {
        return self.allocators.scratch_arena.allocator();
    }

    pub inline fn stringArena(self: *IndexManager) std.mem.Allocator {
        return self.allocators.string_arena.allocator();
    }
    
    pub fn deinit(self: *IndexManager, allocator: std.mem.Allocator) !void {
        for (0..self.index_partitions.len) |idx| {
            self.query_state.results_arrays[idx].deinit();
            self.index_partitions[idx].deinit();
            self.file_data.file_handles[idx].close();
        }
        self.gpa().free(self.file_data.file_handles);
        self.gpa().free(self.index_partitions);
        self.gpa().free(self.query_state.results_arrays);

        try std.fs.cwd().deleteTree(self.file_data.tmp_dir);

        for (0..MAX_NUM_RESULTS) |idx| {
            if (self.query_state.result_positions[idx].len > 0) {
                self.gpa().free(self.query_state.result_positions[idx]);
            }
        }

        self.search_col_idxs.deinit(self.gpa());
        self.columns.deinit();
        self.query_state.thread_pool.deinit();

        self.allocators.string_arena.deinit();
        self.allocators.scratch_arena.deinit();
        _ = self.allocators.gpa.deinit();

        allocator.destroy(self.allocators.gpa);
        allocator.destroy(self.allocators.string_arena);
        allocator.destroy(self.allocators.scratch_arena);
    }


    pub fn readHeader(
        self: *IndexManager, 
        filename: []const u8,
        filetype: fu.FileType,
        ) !void {
        self.file_data.input_filename_c = try self.stringArena().dupeZ(u8, filename);
        self.file_data.file_type = filetype;

        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(self.file_data.inputFilename(), &hash, .{});
            break :blk hash;
        };
        self.file_data.tmp_dir = try std.fmt.allocPrint(
            self.stringArena(),
            ".{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
            );

        std.fs.cwd().makeDir(self.file_data.tmp_dir) catch {
            try std.fs.cwd().deleteTree(self.file_data.tmp_dir);
            try std.fs.cwd().makeDir(self.file_data.tmp_dir);
        };

        switch (self.file_data.file_type) {
            fu.FileType.CSV => {
                try self.readCSVHeader();
           },
            fu.FileType.JSON => {
                try self.getUniqueJSONKeys(16384);
            },
            fu.FileType.PARQUET => {
                var cols = std.ArrayListUnmanaged([]const u8){};
                try pq.getParquetCols(
                    self.stringArena(),
                    &cols,
                    self.file_data.input_filename_c,
                    );
                for (0.., cols.items) |idx, col| {
                    try self.columns.insert(col, @truncate(idx));
                }

                self.file_data.pq_data.?.pq_file_handle = pq.getSerializedReader(self.file_data.input_filename_c);
            },
        }

        std.debug.assert(self.columns.num_keys > 0);

        for (0..MAX_NUM_RESULTS) |idx| {
            self.query_state.result_positions[idx] = try self.gpa().alloc(
                TermPos, 
                self.columns.num_keys,
                );
            self.query_state.result_strings[idx] = std.ArrayListUnmanaged(u8){};
            try self.query_state.result_strings[idx].resize(self.gpa(), 4096);
        }
    }

    pub fn addSearchCol(
        self: *IndexManager,
        col_name: []const u8,
    ) !void {
        const col_name_upper = try self.stringArena().dupe(u8, col_name);

        string_utils.stringToUpper(
            col_name_upper.ptr,
            col_name_upper.len,
        );

        switch (self.file_data.file_type) {
            fu.FileType.CSV, fu.FileType.JSON, fu.FileType.PARQUET => {
                const matched_col_idx = self.columns.find(col_name_upper) catch {
                    return error.ColumnNotFound;
                };
                try self.search_col_idxs.append(self.stringArena(), @truncate(matched_col_idx));
            },
        }
    }

    // pub fn printDebugInfo(self: *IndexManager) !void {
        // std.debug.print("\n=====================================================\n", .{});
// 
        // var num_terms: usize = 0;
        // var num_docs: usize = 0;
        // var avg_doc_size: f32 = 0.0;
// 
        // var num_terms_all_cols = std.ArrayList(u32).init(self.gpa.allocator());
        // defer num_terms_all_cols.deinit();
// 
        // for (0..self.index_partitions.len) |idx| {
// 
            // num_docs += self.index_partitions[idx].II[0].num_docs;
            // for (0..self.index_partitions[idx].II.len) |jdx| {
                // num_terms    += self.index_partitions[idx].II[jdx].num_terms;
                // avg_doc_size += self.index_partitions[idx].II[jdx].avg_doc_size;
// 
                // try num_terms_all_cols.append(self.index_partitions[idx].II[jdx].num_terms);
            // }
// 
        // }
// 
        // std.debug.print("Num Partitions:  {d}\n", .{self.index_partitions.len});
// 
        // var col_iterator = self.search_cols.iterator();
        // var idx: usize = 0;
        // while (col_iterator.next()) |item| {
            // std.debug.print("Column:          {s}\n", .{item.key_ptr.*});
            // std.debug.print("---------------------------------------------\n", .{});
            // std.debug.print("Num terms:       {d}\n", .{num_terms_all_cols.items[idx]});
            // std.debug.print("Num docs:        {d}\n", .{num_docs});
            // std.debug.print("Avg doc size:    {d}\n\n", .{avg_doc_size / @as(f32, @floatFromInt(self.index_partitions.len))});
// 
            // idx += 1;
        // }
// 
        // std.debug.print("=====================================================\n\n\n\n", .{});
    // }
// 

    fn readCSVHeader(self: *IndexManager) !void {
        // TODO: Add `has_header` option.

        const file = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
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

        var byte_idx: usize = 0;
        while (true) {
            const prev_byte_idx = byte_idx;
            csv._iterFieldCSV(f_data, &byte_idx);
            const field_len = byte_idx - 1 - prev_byte_idx;

            @memcpy(term[0..field_len], f_data[prev_byte_idx..byte_idx-1]);
            std.debug.assert(field_len < MAX_TERM_LENGTH);

            string_utils.stringToUpper(term[0..], field_len);
            try self.columns.insert(
                term[0..field_len],
                @truncate(self.columns.num_keys),
                );

            if (f_data[byte_idx - 1] == '\n') break;
        }
        self.file_data.header_bytes = byte_idx;
    }


    fn getUniqueJSONKeys(self: *IndexManager, max_docs_scan: usize) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        const file = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
        defer file.close();

        const file_size = try file.getEndPos();

        var buffered_reader = try fu.DoubleBufferedReader.init(
            self.scratchArena(),
            file,
            '{',
            false,
        );

        var doc_id:   usize = 0;
        var file_pos: usize = 0;
        const buf = try buffered_reader.getBuffer(file_pos);
        while (buf[file_pos] != '{') file_pos += 1;

        while (file_pos < file_size) {
            const buffer = try buffered_reader.getBuffer(file_pos);

            var index: usize = 0;
            try json.iterLineJSONGetUniqueKeys(
                buffer,
                &index,
                &self.columns,
                true,
                );
            file_pos += index;

            doc_id += 1;
            if (doc_id == max_docs_scan) break;
        }
    }


    fn scanJSONFile(self: *IndexManager) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        const file = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
        defer file.close();

        const file_size = try file.getEndPos();

        var buffered_reader = try fu.DoubleBufferedReader.init(
            self.scratchArena(),
            file,
            '{',
            false,
        );

        var line_offsets = std.ArrayList(usize).init(self.scratchArena());

        try line_offsets.ensureTotalCapacity(16384);

        const start_time = std.time.milliTimestamp();

        var file_pos: usize = 0;
        const buf = try buffered_reader.getBuffer(file_pos);
        while (buf[file_pos] != '{') file_pos += 1;

        while (file_pos < file_size) {
            try line_offsets.append(file_pos);
            const buffer = try buffered_reader.getBuffer(file_pos);

            if (line_offsets.items.len == 16384) {
                const estimated_lines = @as(usize, @intFromFloat(@as(f32, @floatFromInt(file_size)) /
                                        @as(f32, @floatFromInt(file_pos))));
                try line_offsets.ensureTotalCapacity(estimated_lines);
            }

            var index: usize = 0;
            try json.iterLineJSON(buffer, &index);
            file_pos += index;
        }
        try line_offsets.append(file_size);

        const end_time = std.time.milliTimestamp();
        const execution_time_ms = end_time - start_time;
        const mb_s: usize = @intFromFloat(
            0.001 * @as(f32, @floatFromInt(file_size)) / @as(f32, @floatFromInt(execution_time_ms))
            );

        std.debug.print("Read {d} lines in {d}ms\n", .{line_offsets.items.len - 1, execution_time_ms});
        std.debug.print("{d}MB/s\n", .{mb_s});

        try self.initPartitions(&line_offsets, file_size);
    }
        

    fn readPartition(
        self: *IndexManager,
        partition_idx: usize,
        chunk_size: usize,
        final_chunk_size: usize,
        num_partitions: usize,
        num_search_cols: usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 30;

        const current_chunk_size = switch (partition_idx != num_partitions - 1) {
            true => chunk_size,
            false => final_chunk_size,
        };

        const start_doc = partition_idx * chunk_size;
        const end_doc   = start_doc + current_chunk_size;

        const output_filename = try std.fmt.allocPrint(
            self.scratchArena(), 
            "{s}/output_{d}", 
            .{self.file_data.tmp_dir, partition_idx}
            );

        const end_token: u8 = switch (self.file_data.file_type) {
            fu.FileType.CSV => '\n',
            fu.FileType.JSON => '{',
            fu.FileType.PARQUET => @panic("For parquet, readPartitionParquet must be called."),
        };

        const start_byte = self.index_partitions[partition_idx].line_offsets[0];
        var token_stream = try fu.TokenStream(fu.token_32t).init(
            self.file_data.inputFilename(),
            output_filename,
            self.gpa(),
            self.search_col_idxs.items.len,
            start_byte,
            end_token,
        );
        defer token_stream.deinit();

        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

        var byte_idx: usize = 0;

        var prev_doc_id: usize = 0;
        for (0.., start_doc..end_doc) |doc_id, _| {

            if (timer.read() >= interval_ns) {
                const current_docs_read = total_docs_read.fetchAdd(
                    doc_id - prev_doc_id, 
                    .monotonic
                    ) + (doc_id - prev_doc_id);
                prev_doc_id = doc_id;
                timer.reset();

                self.indexing_state.last_progress = current_docs_read;

                if (partition_idx == 0) {
                    progress_bar.update(current_docs_read);
                    self.indexing_state.last_progress = current_docs_read;
                }
            }

            switch (self.file_data.file_type) {
                fu.FileType.CSV => {
                    var search_col_idx: usize = 0;
                    var prev_col:       usize = 0;

                    while (search_col_idx < num_search_cols) {
                        for (prev_col..self.search_col_idxs.items[search_col_idx]) |_| {
                            try token_stream.iterFieldCSV(&byte_idx);
                        }

                        try self.index_partitions[partition_idx].processDocRfc4180(
                            &token_stream,
                            &byte_idx,
                            @intCast(doc_id), 
                            search_col_idx,
                            &terms_seen_bitset,
                            );

                        // Add one because we just iterated over the last field.
                        prev_col = self.search_col_idxs.items[search_col_idx] + 1;
                        search_col_idx += 1;
                    }

                    for (prev_col..self.columns.num_keys) |_| {
                        try token_stream.iterFieldCSV(&byte_idx);
                    }
                },
                fu.FileType.JSON => {
                    const buffer = try token_stream.getBuffer(byte_idx);
                    byte_idx += string_utils.simdFindCharIdxEscapedFull(
                        buffer,
                        '"',
                    );

                    while (true) {
                        self.index_partitions[partition_idx].processDocRfc8259(
                            &token_stream,
                            &self.columns,
                            &self.search_col_idxs,
                            &byte_idx,
                            @intCast(doc_id), 
                            &terms_seen_bitset,
                            ) catch break;
                    }
                },
                fu.FileType.PARQUET => @panic("For parquet, readPartitionParquet must be called."),
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

    fn readPartitionParquet(
        self: *IndexManager,
        partition_idx: usize,
        min_row_group: usize,
        max_row_group: usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
        std.debug.assert(self.file_data.file_type == .PARQUET);

        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 30;

        const output_filename = try std.fmt.allocPrint(
            self.scratchArena(), 
            "{s}/output_{d}", 
            .{self.file_data.tmp_dir, partition_idx}
            );

        var token_stream = try fu.ParquetTokenStream(fu.token_32t).init(
            self.file_data.input_filename_c,
            output_filename,
            self.gpa(),
            self.search_col_idxs.items,
            min_row_group,
            max_row_group,
        );
        defer token_stream.deinit();

        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

        var first = true;
        var docs_read: usize = 0;
        while (token_stream.getNextBuffer(first)) {
            first = false;

            var row_idx: u32 = 0;
            while (token_stream.current_idx < token_stream.buffer_len) {

                if (timer.read() >= interval_ns) {
                    const current_docs_read = total_docs_read.fetchAdd(
                        docs_read,
                        .monotonic
                        );
                    docs_read = 0;
                    timer.reset();

                    self.indexing_state.last_progress = current_docs_read;

                    if (partition_idx == 0) {
                        progress_bar.update(current_docs_read);
                        self.indexing_state.last_progress = current_docs_read;
                    }
                }

                try self.index_partitions[partition_idx].processDocVbyte(
                    &token_stream,
                    row_idx, 
                    &terms_seen_bitset,
                    );
                row_idx += 1;
                if (token_stream.current_col_idx == 0) {
                    docs_read += 1;
                }
            }
        }

        // Flush remaining tokens.
        for (0..token_stream.num_terms.len) |search_col_idx| {
            try token_stream.flushTokenStream(search_col_idx);
        }
        _ = total_docs_read.fetchAdd(docs_read, .monotonic);

        // Construct II
        // TODO: Make all ***Pq functions unioned on token stream, instead of duplicated.
        try self.index_partitions[partition_idx].constructFromTokenStreamPq(&token_stream);
    }

    pub fn scanFile(self: *IndexManager) !void {
        switch (self.file_data.file_type) {
            fu.FileType.CSV => try self.scanCSVFile(),
            fu.FileType.JSON => try self.scanJSONFile(),
            fu.FileType.PARQUET => try self.scanParquetFile(),
        }
    }

    pub fn scanCSVFile(self: *IndexManager) !void {
        const file = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
        defer file.close();

        const file_size = try file.getEndPos();

        var buffered_reader = try fu.DoubleBufferedReader.init(
            self.gpa(),
            file,
            '\n',
            false,
        );
        defer buffered_reader.deinit(self.gpa());

        var line_offsets = std.ArrayList(usize).init(self.gpa());
        defer line_offsets.deinit();

        var file_pos: usize = self.file_data.header_bytes.?;

        // Time read.
        const start_time = std.time.microTimestamp();

        try line_offsets.ensureTotalCapacity(16384);

        while (file_pos < file_size - 1) {
            try line_offsets.append(file_pos);
            const buffer = try buffered_reader.getBuffer(file_pos);

            if (line_offsets.items.len == 16384) {
                const estimated_lines = @as(usize, @intFromFloat(@as(f32, @floatFromInt(file_size)) /
                                        @as(f32, @floatFromInt(file_pos))));
                try line_offsets.ensureTotalCapacity(estimated_lines);
            }

            var index: usize = 0;
            try csv.iterLineCSV(buffer, &index);
            file_pos += index;
        }
        try line_offsets.append(file_size);

        const end_time = std.time.microTimestamp();
        const execution_time_us = end_time - start_time;
        const mb_s: usize = @intFromFloat(0.000_001 * @as(f64, @floatFromInt(file_size)) 
                            / @as(f64, @floatFromInt(execution_time_us)));

        std.debug.print("Read {d} lines in {d}ms\n", .{line_offsets.items.len - 1, @divFloor(execution_time_us, 1000)});
        std.debug.print("{d}MB/s\n", .{mb_s});

        try self.initPartitions(&line_offsets, file_size);
    }

    pub fn scanParquetFile(self: *IndexManager) !void {
        self.file_data.pq_data.?.num_row_groups = pq.getNumRowGroupsParquet(
            self.file_data.input_filename_c,
        );
        const num_rg = self.file_data.pq_data.?.num_row_groups;

        // const num_partitions = @min(
            // try std.Thread.getCpuCount(), 
            // num_rg,
            // );
        const num_partitions = @min(
            try std.Thread.getCpuCount(), 
            MAX_NUM_THREADS,
            num_rg,
            );
        // const num_partitions = 1;

        self.file_data.file_handles     = try self.gpa().alloc(std.fs.File, num_partitions);
        self.index_partitions           = try self.gpa().alloc(BM25Partition, num_partitions);
        self.query_state.results_arrays = try self.gpa().alloc(SortedScoreMultiArray(QueryResult), num_partitions);

        for (0..num_partitions) |idx| {
            self.file_data.file_handles[idx]     = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
            self.query_state.results_arrays[idx] = try SortedScoreMultiArray(QueryResult).init(self.gpa(), MAX_NUM_RESULTS);
            const min_row_group = @divFloor(idx * num_rg, num_partitions);
            const max_row_group = @min(
                @divFloor((idx + 1) * num_rg, num_partitions), 
                num_rg,
                );

            var row_group_sizes = try self.stringArena().alloc(
                usize, 
                max_row_group - min_row_group,
                );

            var num_rows: usize = 0;
            for (0.., min_row_group..max_row_group) |i, rg_idx| {
                row_group_sizes[i] = pq.getNumRowGroupsInRowGroup(
                    self.file_data.input_filename_c,
                    rg_idx,
                );
                num_rows += row_group_sizes[i];
            }

            self.index_partitions[idx] = try BM25Partition.init(
                self.gpa(), 
                1, 
                row_group_sizes,
                num_rows,
                );
        }
    }

    fn initPartitions(
        self: *IndexManager, 
        line_offsets: *const std.ArrayList(usize),
        _: usize,
        ) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        const num_lines = line_offsets.items.len - 1;

        const num_partitions = if (num_lines > 50_000) @min(try std.Thread.getCpuCount(), MAX_NUM_THREADS) else 1;

        self.file_data.file_handles     = try self.gpa().alloc(std.fs.File, num_partitions);
        self.index_partitions           = try self.gpa().alloc(BM25Partition, num_partitions);
        self.query_state.results_arrays = try self.gpa().alloc(SortedScoreMultiArray(QueryResult), num_partitions);
        for (0..num_partitions) |idx| {
            self.file_data.file_handles[idx]     = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
            self.query_state.results_arrays[idx] = try SortedScoreMultiArray(QueryResult).init(self.gpa(), MAX_NUM_RESULTS);
        }

        const chunk_size:       usize = @divFloor(num_lines, num_partitions);
        const final_chunk_size: usize = chunk_size + (num_lines % num_partitions);

        for (0..num_partitions) |i| {
            const current_chunk_size = switch (i != num_partitions - 1) {
                true  => chunk_size,
                false => final_chunk_size,
            };

            const start = i * chunk_size;
            const end   = start + current_chunk_size + 1;

            const partition_line_offsets = try self.gpa().alloc(usize, current_chunk_size + 1);
            @memcpy(partition_line_offsets, line_offsets.items[start..end]);

            self.index_partitions[i] = try BM25Partition.init(
                self.gpa(), 
                1, 
                partition_line_offsets,
                partition_line_offsets.len - 1,
                );
        }
    }


    pub fn indexFile(self: *IndexManager) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        const num_partitions = self.index_partitions.len;
        std.debug.print("Indexing {d} partitions\n", .{num_partitions});

        var num_lines: usize = 0;
        var num_rg:    usize = 0;
        if (self.file_data.file_type == .PARQUET) {
            num_rg = self.file_data.pq_data.?.num_row_groups;
            for (self.index_partitions) |*p| {
                for (p.line_offsets) |size| {
                    num_lines += size;
                }
            }
        } else {
            for (self.index_partitions) |*p| {
                num_lines += p.line_offsets.len - 1;
            }
        }

        const time_start = std.time.milliTimestamp();

        var threads = try self.scratchArena().alloc(std.Thread, num_partitions);

        var total_docs_read = AtomicCounter.init(0);
        var progress_bar = progress.ProgressBar.init(num_lines);

        const num_search_cols = self.search_col_idxs.items.len;
        
        for (0..num_partitions) |partition_idx| {
            try self.index_partitions[partition_idx].resizeNumSearchCols(
                num_search_cols
                );

            if (self.file_data.file_type == .PARQUET) {
                const min_row_group = @divFloor(partition_idx * num_rg, num_partitions);
                const max_row_group = @min(
                    @divFloor((partition_idx + 1) * num_rg, num_partitions), 
                    num_rg,
                    );

                threads[partition_idx] = try std.Thread.spawn(
                    .{},
                    readPartitionParquet,
                    .{
                        self,
                        partition_idx,
                        min_row_group,
                        max_row_group,
                        &total_docs_read,
                        &progress_bar,
                        },
                    );
            } else {
                threads[partition_idx] = try std.Thread.spawn(
                    .{},
                    readPartition,
                    .{
                        self,
                        partition_idx,
                        self.index_partitions[0].line_offsets.len - 1,
                        self.index_partitions[num_partitions - 1].line_offsets.len - 1,
                        num_partitions,
                        num_search_cols,
                        &total_docs_read,
                        &progress_bar,
                        },
                    );
            }
        }

        for (threads) |thread| {
            thread.join();
        }

        const _total_docs_read = total_docs_read.load(.acquire);
        std.debug.print("Processed {d} documents\n", .{_total_docs_read});
        std.debug.assert(_total_docs_read == num_lines);
        progress_bar.update(_total_docs_read);

        const time_end = std.time.milliTimestamp();
        const time_diff = time_end - time_start;
        std.debug.print("Processed {d} documents in {d}ms\n", .{_total_docs_read, time_diff});

        // Init thread pool.
        try self.query_state.thread_pool.init(
            .{
                .allocator = self.gpa(),
                .n_jobs = self.index_partitions.len,
            },
        );
    }

    pub fn queryPartitionOrdered(
        self: *IndexManager,
        queries: std.StringHashMap([]const u8),
        boost_factors: std.ArrayList(f32),
        partition_idx: usize,
        query_results: *SortedScoreMultiArray(QueryResult),
    ) void {
        const num_search_cols = self.search_col_idxs.items.len;
        std.debug.assert(num_search_cols > 0);

        // Tokenize query.
        var tokens = std.ArrayListUnmanaged(ColTokenPair){};

        var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;

        var empty_query = true; 

        var query_it = queries.iterator();
        while (query_it.next()) |entry| {
            const col_idx = self.columns.find(entry.key_ptr.*) catch {
                std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                continue;
            };
            const II_idx = findSorted(
                u32,
                self.search_col_idxs.items,
                col_idx,
            ) catch {
                std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                @panic("Column not found. Check findSorted function and self.columns map.");
            };

            std.debug.assert(II_idx <= self.search_col_idxs.items.len);

            var term_len: usize = 0;

            var term_pos: u8 = 0;
            for (entry.value_ptr.*) |c| {
                switch (c) {
                    0...33, 35...47, 58...64, 91...96, 123...126 => {
                        if (term_len == 0) continue;

                        const token = self.index_partitions[partition_idx].II[II_idx].vocab.get(
                            term_buffer[0..term_len],
                            self.index_partitions[partition_idx].II[II_idx].vocab.getAdapter(),
                            );
                        if (token != null) {
                            tokens.append(
                                self.gpa(),
                                ColTokenPair{
                                .col_idx       = @intCast(II_idx),
                                .term_pos      = term_pos,
                                .token         = token.?,
                                .shallow_query = false,
                            }) catch {
                                @panic("Failed to append token");
                            };
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
                            const token = self.index_partitions[partition_idx].II[II_idx].vocab.get(
                                term_buffer[0..term_len],
                                self.index_partitions[partition_idx].II[II_idx].vocab.getAdapter(),
                                );
                            if (token != null) {
                                tokens.append(
                                    self.gpa(),
                                    ColTokenPair{
                                    .col_idx       = @intCast(II_idx),
                                    .term_pos      = term_pos,
                                    .token         = token.?,
                                    .shallow_query = false,
                                }) catch {
                                    @panic("Failed to append token");
                                };
                                term_pos += 1;
                                empty_query = false;
                            }
                            term_len = 0;
                        }
                    },
                }
            }

            if (term_len > 0) {
                const token = self.index_partitions[partition_idx].II[II_idx].vocab.get(
                    term_buffer[0..term_len],
                    self.index_partitions[partition_idx].II[II_idx].vocab.getAdapter(),
                    );
                if (token != null) {
                    tokens.append(
                        self.gpa(),
                        ColTokenPair{
                        .col_idx = @intCast(II_idx),
                        .term_pos = term_pos,
                        .token = token.?,
                        .shallow_query = false,
                        // .shallow_query = tokens.items.len > 0,
                    }) catch {
                        @panic("Failed to append token");
                    };

                    // Add prefix matches.
                    // var trie = self.index_partitions[partition_idx].II[II_idx].prt_vocab;
                    // var matching_nodes = try self.gpa.allocator().alloc(
                        // rt.RadixTrie(u32).Entry, 
                        // 10,
                        // );
                    // defer self.gpa.allocator().free(matching_nodes);
// 
                    // const nodes_found = try trie.getPrefixNodes(
                        // term_buffer[0..term_len],
                        // &matching_nodes,
                        // );
// 
                    // const shallow = tokens.items.len > 1;
                    // for (0..nodes_found) |idx| {
                        // try tokens.append(ColTokenPair{
                            // .col_idx = @intCast(II_idx),
                            // .term_pos = term_pos,
                            // .token = matching_nodes[idx].value,
                            // .shallow_query = shallow,
                        // });
                        // std.debug.print("Node {d} - Key: {s} Value: {d}\n", .{idx, matching_nodes[idx].key, matching_nodes[idx].value});
                    // }
// 
                    term_pos += 1;
                    empty_query = false;
                }
            }
        }

        if (empty_query) return;

        // For each token in each II, get relevant docs and add to score.
        var doc_scores: *std.AutoHashMap(u32, ScoringInfo) = &self.index_partitions[partition_idx].doc_score_map;
        doc_scores.clearRetainingCapacity();

        var sorted_scores = SortedScoreMultiArray(void).init(
            self.gpa(), 
            query_results.capacity,
            ) catch {
                @panic("Failed to init sorted scores");
            };
        defer sorted_scores.deinit();


        var token_scores = self.gpa().alloc(f32, tokens.items.len) catch {
            @panic("Failed to alloc token scores");
        };

        var idf_remaining: f32 = 0.0;
        for (0.., tokens.items) |idx, _token| {
            const II_idx: usize = @intCast(_token.col_idx);
            const token:  usize = @intCast(_token.token);
            std.debug.assert(II_idx <= self.search_col_idxs.items.len);

            const II = &self.index_partitions[partition_idx].II[II_idx];

            const inner_term = switch (_token.shallow_query) {
                true => @as(f32, @floatFromInt(II.doc_freqs.items[token])),
                false => @as(f32, @floatFromInt(II.num_docs)) / 
                         @as(f32, @floatFromInt(II.doc_freqs.items[token])),
            };
            const boost_weighted_idf: f32 = (
                1.0 + std.math.log2(inner_term)
                ) * boost_factors.items[II_idx];
            token_scores[idx] = boost_weighted_idf;

            idf_remaining += boost_weighted_idf;
        }
        const idf_sum = idf_remaining;

        var done = false;

        var last_II_idx: usize = 0;
        for (0..tokens.items.len) |idx| {
            const score          = token_scores[idx];
            const col_score_pair = tokens.items[idx];

            const II_idx = @as(usize, @intCast(col_score_pair.col_idx));
            const token  = @as(usize, @intCast(col_score_pair.token));

            std.debug.assert(II_idx <= self.search_col_idxs.items.len);

            const II = &self.index_partitions[partition_idx].II[II_idx];

            const offset      = II.term_offsets[token];
            const last_offset = II.term_offsets[token + 1];

            const is_high_df_term: bool = (
                score < 0.4 * idf_sum / @as(f32, @floatFromInt(tokens.items.len))
                );

            var prev_doc_id: u32 = std.math.maxInt(u32);
            for (
                II.postings.doc_ids[offset..last_offset],
                II.postings.term_positions[offset..last_offset],
                ) |doc_id, term_pos| {

                prev_doc_id = doc_id;

                const _result = doc_scores.getPtr(doc_id);
                if (_result) |result| {
                    // TODO: Consider front of record boost.

                    // Phrase boost.
                    const last_term_pos = result.*.term_pos;
                    const do_phrase_boost = (term_pos == last_term_pos + 1) and 
                                            (II_idx == last_II_idx) and 
                                            (doc_id == prev_doc_id);
                    result.*.score += @as(f32, @floatFromInt(@intFromBool(do_phrase_boost))) * score * 0.75;

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

                        doc_scores.put(
                            doc_id,
                            ScoringInfo{
                                .score = score,
                                .term_pos = term_pos,
                            }
                        ) catch {
                            @panic("Failed to put doc score");
                        };
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
        self: *IndexManager,
        queries: std.StringHashMap([]const u8),
        k: usize,
        boost_factors: std.ArrayList(f32),
    ) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        if (k > MAX_NUM_RESULTS) {
            std.debug.print("k must be less than or equal to {d}\n", .{MAX_NUM_RESULTS});
            return error.InvalidArgument;
        }

        // Init num_partitions threads.
        const num_partitions = self.index_partitions.len;

        var wg: std.Thread.WaitGroup = .{};

        for (0..num_partitions) |partition_idx| {
            self.query_state.results_arrays[partition_idx].clear();
            self.query_state.results_arrays[partition_idx].resize(k);
            self.query_state.thread_pool.spawnWg(
                &wg,
                queryPartitionOrdered,
                .{
                    self,
                    queries,
                    boost_factors,
                    partition_idx,
                    &self.query_state.results_arrays[partition_idx],
                },
            );
        }

        wg.wait();

        if (self.index_partitions.len > 1) {
            for (self.query_state.results_arrays[1..]) |*tr| {
                for (0.., tr.items[0..tr.count]) |idx, r| {
                    self.query_state.results_arrays[0].insert(r, tr.scores[idx]);
                }
            }
        }
        std.debug.print("FOUND {d} results\n", .{self.query_state.results_arrays[0].count});
        if (self.query_state.results_arrays[0].count == 0) return;

        for (0..self.query_state.results_arrays[0].count) |idx| {
            const result = self.query_state.results_arrays[0].items[idx];

            std.debug.assert(self.query_state.result_strings[idx].capacity > 0);
            if (self.file_data.file_type == .PARQUET) {
                try self.index_partitions[result.partition_idx].fetchRecordsParquet(
                    self.query_state.result_positions[idx],
                    self.file_data.pq_data.?.pq_file_handle,
                    result,
                    &self.query_state.result_strings[idx],
                );
            } else {
                try self.index_partitions[result.partition_idx].fetchRecords(
                    self.query_state.result_positions[idx],
                    &self.file_data.file_handles[result.partition_idx],
                    result,
                    &self.query_state.result_strings[idx],
                    self.file_data.file_type,
                    &self.columns,
                );
            }
            std.debug.print(
                "Score {d}: {d} - Doc id: {d}\n", 
                .{
                    idx, 
                    self.query_state.results_arrays[0].scores[idx], 
                    self.query_state.results_arrays[0].items[idx].doc_id,
                }
                );
        }
        std.debug.print("\n", .{});
    }
};


test "index_csv" {
    const filename: []const u8 = "../data/mb_small.csv";
    // const filename: []const u8 = "../data/mb.csv";

    var index_manager = try IndexManager.init();

    try index_manager.readHeader(filename, fu.FileType.CSV);
    try index_manager.scanFile();

    try index_manager.addSearchCol("title");
    try index_manager.addSearchCol("artist");
    try index_manager.addSearchCol("album");

    try index_manager.indexFile();

    // try index_manager.deinit();
}

test "index_json" {
    @breakpoint();
    const filename: []const u8 = "../data/mb_small.json";
    // const filename: []const u8 = "../data/mb.json";

    var index_manager = try IndexManager.init();

    try index_manager.readHeader(filename, fu.FileType.JSON);
    try index_manager.scanFile();

    try index_manager.addSearchCol("title");
    try index_manager.addSearchCol("artist");
    try index_manager.addSearchCol("album");

    try index_manager.indexFile();
    @breakpoint();
    // try index_manager.deinit();
}
