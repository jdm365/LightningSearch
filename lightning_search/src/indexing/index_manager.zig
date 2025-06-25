const std = @import("std");
const builtin = @import("builtin");

const string_utils = @import("../utils/string_utils.zig");
const fu           = @import("../storage/file_utils.zig");

const DocStore = @import("../storage/doc_store.zig").DocStore;

const StaticIntegerSet = @import("../utils/static_integer_set.zig").StaticIntegerSet;

const progress = @import("../utils/progress.zig");

const csv  = @import("../parsing/csv.zig");
const json = @import("../parsing/json.zig");
const pq   = @import("../parsing/parquet.zig");

const rt = @import("../utils/radix_trie.zig");

const TermPos = @import("../server/server.zig").TermPos;

const ID = @import("index.zig").ID;
const PostingsIterator = @import("index.zig").PostingsIterator;
// const PostingsIteratorV2 = @import("index.zig").PostingsIteratorV2;
const fetchRecordsDocStore = @import("index.zig").BM25Partition.fetchRecordsDocStore;
const BM25Partition   = @import("index.zig").BM25Partition;
const QueryResult     = @import("index.zig").QueryResult;
const MAX_TERM_LENGTH = @import("index.zig").MAX_TERM_LENGTH;
const MAX_NUM_TERMS   = @import("index.zig").MAX_NUM_TERMS;


const SHM = @import("index.zig").SHM;
const SortedScoreMultiArray = @import("../utils/sorted_array.zig").SortedScoreMultiArray;
const SortedIntMultiArray   = @import("../utils/sorted_array.zig").SortedIntMultiArray;
const ScorePair             = @import("../utils/sorted_array.zig").ScorePair;
const findSorted            = @import("../utils/misc_utils.zig").findSorted;
const sortStruct            = @import("../utils/misc_utils.zig").sortStruct;
const printPercentiles      = @import("../utils/misc_utils.zig").printPercentiles;

const AtomicCounter = std.atomic.Value(u64);

pub const MAX_NUM_RESULTS = @import("index.zig").MAX_NUM_RESULTS;
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
    bw_idf: u32,
    term_pos: u16,
};


pub const IndexManager = struct {
    partitions: Partitions,
    allocators: Allocators,
    file_data: FileData,
    query_state: QueryState,
    indexing_state: IndexingState,

    columns: rt.RadixTrie(u32),
    search_col_idxs: std.ArrayListUnmanaged(u32),

    const Partitions = struct {
        index_partitions: []BM25Partition,
        row_offsets: []usize,
        byte_offsets: []usize,
    };

    const Allocators = struct {
        gpa: *std.heap.DebugAllocator(.{.thread_safe = true}),
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

        mmap_buffer: []u8,

        inline fn inputFilename(self: *FileData) []const u8 {
            return std.mem.span(self.input_filename_c);
        }
    };

    const QueryState = struct {
        bit_sizes: [MAX_NUM_RESULTS][]u32,

        result_positions: [MAX_NUM_RESULTS][]TermPos,
        result_strings: [MAX_NUM_RESULTS]std.ArrayListUnmanaged(u8),
        results_arrays: []SortedScoreMultiArray(QueryResult),
        thread_pool: std.Thread.Pool,
    };

    const IndexingState = struct {
        partition_is_indexing: []bool,
        last_progress: usize,
    };

    pub fn init(allocator: std.mem.Allocator) !IndexManager {
        var manager = IndexManager{
            .partitions = Partitions{
                .index_partitions = undefined,
                .row_offsets      = undefined,
                .byte_offsets     = undefined,
            },

            .allocators = Allocators{
                .gpa           = try allocator.create(
                    std.heap.DebugAllocator(.{.thread_safe = true})
                    ),
                .string_arena  = try allocator.create(std.heap.ArenaAllocator),
                .scratch_arena = try allocator.create(std.heap.ArenaAllocator),
            },

            .file_data = FileData{
                .input_filename_c = undefined,
                .tmp_dir          = undefined,
                .file_handles     = undefined,
                .file_type        = undefined,
                .mmap_buffer      = undefined,
            },

            .query_state = QueryState{
                .bit_sizes = undefined,

                .result_positions = undefined,
                .result_strings   = undefined,
                .results_arrays   = undefined,
                .thread_pool      = undefined,
            },

            .indexing_state = IndexingState{
                .last_progress = 0,
                .partition_is_indexing = undefined,
            },

            .columns         = undefined,
            .search_col_idxs = std.ArrayListUnmanaged(u32){},
        };
        manager.allocators.gpa.*           = std.heap.DebugAllocator(.{.thread_safe = true}){};
        manager.allocators.string_arena.*  = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        manager.allocators.scratch_arena.* = std.heap.ArenaAllocator.init(std.heap.page_allocator);

        try manager.search_col_idxs.ensureTotalCapacity(
            manager.gpa(), 
            4,
            );
        manager.columns = try rt.RadixTrie(u32).initCapacity(
            manager.gpa(),
            32,
            );


        return manager;
    }

    pub inline fn gpa(self: *IndexManager) std.mem.Allocator {
        switch (builtin.mode) {
            .ReleaseFast => {
                return std.heap.smp_allocator;
            },
            else => {
                return self.allocators.gpa.allocator();
            },
        }
    }

    pub inline fn scratchArena(self: *IndexManager) std.mem.Allocator {
        return self.allocators.scratch_arena.allocator();
    }

    pub inline fn stringArena(self: *IndexManager) std.mem.Allocator {
        return self.allocators.string_arena.allocator();
    }
    
    pub fn deinit(self: *IndexManager, allocator: std.mem.Allocator) !void {
        for (0..self.partitions.index_partitions.len) |idx| {
            self.query_state.results_arrays[idx].deinit();
            try self.partitions.index_partitions[idx].deinit();
            self.file_data.file_handles[idx].close();
        }
        self.gpa().free(self.partitions.row_offsets);
        self.gpa().free(self.partitions.byte_offsets);
        self.gpa().free(self.partitions.index_partitions);

        self.gpa().free(self.file_data.file_handles);
        std.posix.munmap(self.file_data.mmap_buffer);

        try std.fs.cwd().deleteTree(self.file_data.tmp_dir);

        self.gpa().free(self.query_state.results_arrays);
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

        std.fs.cwd().makeDir("ls_data") catch {
            std.debug.print("Dir ls_data already exists\n", .{});
        };

        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(self.file_data.inputFilename(), &hash, .{});
            break :blk hash;
        };
        self.file_data.tmp_dir = try std.fmt.allocPrint(
            self.stringArena(),
            "ls_data/{x:0>32}", .{std.fmt.fmtSliceHexLower(file_hash[0..16])}
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

                self.file_data.pq_data = PQData{
                    .num_row_groups = undefined,
                    .pq_file_handle = undefined,
                };
                self.file_data.pq_data.?.pq_file_handle = pq.getSerializedReader(self.file_data.input_filename_c);
            },
        }

        std.debug.assert(self.columns.num_keys > 0);

        for (0..MAX_NUM_RESULTS) |idx| {
            self.query_state.bit_sizes[idx] = try self.gpa().alloc(
                u32, 
                self.columns.num_keys,
                );
            self.query_state.result_positions[idx] = try self.gpa().alloc(
                TermPos, 
                self.columns.num_keys,
                );
            self.query_state.result_strings[idx] = std.ArrayListUnmanaged(u8){};
            try self.query_state.result_strings[idx].resize(self.gpa(), 4096);
            // try self.query_state.result_strings[idx].resize(self.gpa(), 65_536);
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
        // for (0..self.partitions.index_partitions.len) |idx| {
// 
            // num_docs += self.partitions.index_partitions[idx].II[0].num_docs;
            // for (0..self.partitions.index_partitions[idx].II.len) |jdx| {
                // num_terms    += self.partitions.index_partitions[idx].II[jdx].num_terms;
                // avg_doc_size += self.partitions.index_partitions[idx].II[jdx].avg_doc_size;
// 
                // try num_terms_all_cols.append(self.partitions.index_partitions[idx].II[jdx].num_terms);
            // }
// 
        // }
// 
        // std.debug.print("Num Partitions:  {d}\n", .{self.partitions.index_partitions.len});
// 
        // var col_iterator = self.search_cols.iterator();
        // var idx: usize = 0;
        // while (col_iterator.next()) |item| {
            // std.debug.print("Column:          {s}\n", .{item.key_ptr.*});
            // std.debug.print("---------------------------------------------\n", .{});
            // std.debug.print("Num terms:       {d}\n", .{num_terms_all_cols.items[idx]});
            // std.debug.print("Num docs:        {d}\n", .{num_docs});
            // std.debug.print("Avg doc size:    {d}\n\n", .{avg_doc_size / @as(f32, @floatFromInt(self.partitions.index_partitions.len))});
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

        const buffer = try self.gpa().alloc(u8, @min(file_size, 1 << 14));
        defer self.gpa().free(buffer);

        _ = try file.read(buffer);

        // self.file_data.mmap_buffer = try std.posix.mmap(
            // null,
            // file_size,
            // std.posix.PROT.READ,
            // .{ .TYPE = .PRIVATE },
            // file.handle,
            // 0
        // );
        // try std.posix.madvise(
            // @alignCast(self.file_data.mmap_buffer.ptr),
            // file_size,
            // std.posix.MADV.SEQUENTIAL,
        // );

        var term: [MAX_TERM_LENGTH]u8 = undefined;

        var byte_idx: usize = 0;
        while (true) {
            const prev_byte_idx = byte_idx;
            csv._iterFieldCSV(buffer, &byte_idx);
            const field_len = byte_idx - 1 - prev_byte_idx;

            @memcpy(term[0..field_len], buffer[prev_byte_idx..byte_idx-1]);
            std.debug.assert(field_len < MAX_TERM_LENGTH);

            string_utils.stringToUpper(term[0..], field_len);
            try self.columns.insert(
                term[0..field_len],
                @truncate(self.columns.num_keys),
                );

            if (buffer[byte_idx - 1] == '\n') break;
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

        const num_partitions =  if (file_size > (1 << 24)) 
            @min(
            try std.Thread.getCpuCount(), 
            MAX_NUM_THREADS,
            )
            else 1;

        self.partitions.row_offsets  = try self.gpa().alloc(usize, num_partitions + 1);
        self.partitions.byte_offsets = try self.gpa().alloc(usize, num_partitions + 1);

        const chunk_size: usize = @divFloor(file_size, num_partitions);
        self.partitions.byte_offsets[0] = 0;
        self.partitions.row_offsets[0]  = 0;

        const start_time = std.time.milliTimestamp();

        var file_pos: usize = 0;
        var partition_idx: usize = 0;
        var line_count: u64 = 0;

        const buf = try buffered_reader.getBuffer(file_pos);
        while (buf[file_pos] != '{') file_pos += 1;

        while (file_pos < file_size) {
            if (file_pos - self.partitions.byte_offsets[partition_idx] > chunk_size) {
                partition_idx += 1;

                self.partitions.byte_offsets[partition_idx] = file_pos;
                self.partitions.row_offsets[partition_idx]  = line_count;

            }
            const buffer = try buffered_reader.getBuffer(file_pos);

            var index: usize = 0;
            try json.iterLineJSON(buffer, &index);
            file_pos += index;
            line_count += 1;
        }

        self.partitions.byte_offsets[partition_idx + 1] = file_pos;
        self.partitions.row_offsets[partition_idx + 1]  = line_count;

        const end_time = std.time.milliTimestamp();
        const execution_time_ms: u64 = @intCast(end_time - start_time);
        const mb_s: usize = @divFloor(file_size, execution_time_ms * (1 << 10));

        std.debug.print(
            "Read {d} lines in {d}ms\n", 
            .{line_count, execution_time_ms},
            );
        std.debug.print("{d}MB/s\n", .{mb_s});

        try self.initPartitions();
    }
        

    fn readPartition(
        self: *IndexManager,
        partition_idx: usize,
        num_search_cols: usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        var timer = try std.time.Timer.start();
        const interval_ns: u64 = 1_000_000_000 / 30;

        const start_doc  = self.partitions.row_offsets[partition_idx];
        const end_doc    = self.partitions.row_offsets[partition_idx + 1];

        const start_byte = self.partitions.byte_offsets[partition_idx];
        // const end_byte   = self.partitions.byte_offsets[partition_idx + 1];

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
        const current_IP = &self.partitions.index_partitions[partition_idx];

        // const mmap_buffer = self.file_data.mmap_buffer[start_byte..end_byte];
        const file = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
        defer file.close();

        var reader = try fu.SingleThreadedDoubleBufferedReader.init(
            self.gpa(),
            file,
            start_byte,
            end_token,
        );
        defer reader.deinit(self.gpa());


        var token_stream = try fu.TokenStreamV2(fu.token_32t_v2).init(
            output_filename,
            self.gpa(),
            self.search_col_idxs.items.len,
        );
        defer token_stream.deinit();

        var freq_table: [256]u32 = undefined;
        @memset(freq_table[0..], 0);

        var buffer = try reader.getBuffer(start_byte);
        switch (self.file_data.file_type) {
            fu.FileType.CSV, fu.FileType.JSON => {
                const num_bytes = @min(buffer.len, 1 << 14);
                for (0..num_bytes) |idx| {
                    freq_table[buffer[idx]] += 1;
                }
            },
            fu.FileType.PARQUET => @panic("For parquet, readPartitionParquet must be called."),
        }

        // TODO: Implement type inference to allow literal types.
        var literal_byte_idxs = std.ArrayListUnmanaged(usize){};
        var literal_col_idxs  = std.ArrayListUnmanaged(usize){};
        var huffman_col_idxs  = std.ArrayListUnmanaged(usize){};
        defer literal_byte_idxs.deinit(self.gpa());
        defer literal_col_idxs.deinit(self.gpa());
        defer huffman_col_idxs.deinit(self.gpa());

        for (0..self.columns.num_keys) |idx| {
            try huffman_col_idxs.append(self.gpa(), idx);
        }

        current_IP.doc_store = try DocStore.init(
            self.allocators.gpa,
            &literal_byte_idxs,
            &literal_col_idxs,
            &huffman_col_idxs,
            self.file_data.tmp_dir,
            partition_idx,
        );

        try current_IP.doc_store.huffman_compressor.buildHuffmanTreeGivenFreqs(
            self.gpa(),
            &freq_table,
        );

        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();

        var file_pos:       usize = start_byte;
        var prev_doc_id:    usize = 0;
        var search_col_idx: usize = 0;
        var prev_col:       usize = 0;
        var row_byte_idx:   usize = 0;
        var is_quoted:      bool  = false;


        for (0.., start_doc..end_doc) |doc_id, _| {
            buffer = try reader.getBuffer(file_pos);
            row_byte_idx = 0;

            if (timer.read() >= interval_ns) {
                const current_docs_read = total_docs_read.fetchAdd(
                    doc_id - prev_doc_id, 
                    .monotonic
                    ) + (doc_id - prev_doc_id);
                prev_doc_id = doc_id;
                timer.reset();

                self.indexing_state.last_progress = current_docs_read;

                for (0..self.partitions.index_partitions.len) |idx| {
                    if (self.indexing_state.partition_is_indexing[idx]) {
                        progress_bar.update(current_docs_read, null);
                        break;
                    }
                }
            }

            switch (self.file_data.file_type) {
                fu.FileType.CSV => {
                    search_col_idx = 0;
                    prev_col       = 0;

                    while (search_col_idx < num_search_cols) {
                        for (prev_col..self.search_col_idxs.items[search_col_idx]) |col_idx| {
                            const init_byte_idx = row_byte_idx;
                            is_quoted = buffer[init_byte_idx] == '"';

                            csv._iterFieldCSV(buffer, &row_byte_idx);

                            self.query_state.result_positions[partition_idx][col_idx] = TermPos{
                                .start_pos = @truncate(init_byte_idx + @as(u32, @intFromBool(is_quoted))),
                                .field_len = @truncate(row_byte_idx - init_byte_idx - 1 - 2 * @as(u32, @intFromBool(is_quoted))),
                            };
                        }
                        const init_byte_idx = row_byte_idx;
                        is_quoted = buffer[init_byte_idx] == '"';

                        try current_IP.processDocRfc4180(
                            &token_stream,
                            buffer,
                            &row_byte_idx,
                            @intCast(doc_id), 
                            search_col_idx,
                            &terms_seen_bitset,
                            );

                        self.query_state.result_positions[partition_idx][self.search_col_idxs.items[search_col_idx]] = TermPos{
                            .start_pos = @truncate(init_byte_idx + @as(u32, @intFromBool(is_quoted))),
                            .field_len = @truncate(row_byte_idx - init_byte_idx - 1 - 2 * @as(u32, @intFromBool(is_quoted))),
                        };

                        // Add one because we just iterated over the last field.
                        prev_col = self.search_col_idxs.items[search_col_idx] + 1;
                        search_col_idx += 1;
                    }

                    for (prev_col..self.columns.num_keys) |col_idx| {
                        const init_byte_idx = row_byte_idx;
                        is_quoted = buffer[init_byte_idx] == '"';

                        csv._iterFieldCSV(buffer, &row_byte_idx);

                        self.query_state.result_positions[partition_idx][col_idx] = TermPos{
                            .start_pos = @truncate(init_byte_idx + @as(u32, @intFromBool(is_quoted))),
                            .field_len = @truncate(row_byte_idx - init_byte_idx - 1 - 2 * @as(u32, @intFromBool(is_quoted))),
                        };
                    }
                },
                fu.FileType.JSON => {
                    row_byte_idx += string_utils.simdFindCharIdxEscapedFull(
                        buffer,
                        '"',
                    );

                    while (true) {
                        current_IP.processDocRfc8259(
                            &token_stream,
                            buffer,
                            &self.columns,
                            &self.search_col_idxs,
                            &row_byte_idx,
                            @intCast(doc_id), 
                            &terms_seen_bitset,
                            ) catch break;
                    }

                    file_pos += row_byte_idx;
                },
                fu.FileType.PARQUET => @panic("For parquet, readPartitionParquet must be called."),
            }

            try current_IP.doc_store.addRow(
                self.query_state.result_positions[partition_idx],
                buffer[0..],
            );
            file_pos += row_byte_idx;
        }
        self.indexing_state.partition_is_indexing[partition_idx] = false;

        // Flush remaining tokens.
        for (0..token_stream.num_terms.len) |_search_col_idx| {
            // try token_stream.flushTokenStream(_search_col_idx);
            try current_IP.II[_search_col_idx].commit(
                current_IP.allocator,
            );
        }
        _ = total_docs_read.fetchAdd(end_doc - (start_doc + prev_doc_id), .monotonic);

        // Flush remaining doc storage.
        try current_IP.doc_store.flush();

        // Construct II
        // try current_IP.constructFromTokenStream(&token_stream);
    }


    fn readPartitionParquet(
        self: *IndexManager,
        partition_idx: usize,
        min_row_group: usize,
        max_row_group: usize,
        total_docs_read: *AtomicCounter,
        progress_bar: *progress.ProgressBar,
    ) !void {
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

        const current_IP = &self.partitions.index_partitions[partition_idx];

        var token_stream = try fu.TokenStreamV2(fu.token_32t_v2).init(
            output_filename,
            self.gpa(),
            self.search_col_idxs.items.len,
        );
        defer token_stream.deinit();

        var freq_table: [256]u32 = undefined;
        @memset(freq_table[0..], 0);

        var buffer = pq.readRowGroup(
            self.file_data.input_filename_c,
            min_row_group,
        );
        const num_bytes = @min(buffer.len, 1 << 14);
        for (0..num_bytes) |idx| {
            freq_table[buffer[idx]] += 1;
        }

        // TODO: Implement type inference to allow literal types.
        var literal_byte_idxs = std.ArrayListUnmanaged(usize){};
        var literal_col_idxs  = std.ArrayListUnmanaged(usize){};
        var huffman_col_idxs  = std.ArrayListUnmanaged(usize){};
        defer literal_byte_idxs.deinit(self.gpa());
        defer literal_col_idxs.deinit(self.gpa());
        defer huffman_col_idxs.deinit(self.gpa());

        for (0..self.columns.num_keys) |idx| {
            try huffman_col_idxs.append(self.gpa(), idx);
        }

        current_IP.doc_store = try DocStore.init(
            self.allocators.gpa,
            &literal_byte_idxs,
            &literal_col_idxs,
            &huffman_col_idxs,
            self.file_data.tmp_dir,
            partition_idx,
        );

        try current_IP.doc_store.huffman_compressor.buildHuffmanTreeGivenFreqs(
            self.gpa(),
            &freq_table,
        );

        var terms_seen_bitset = StaticIntegerSet(MAX_NUM_TERMS).init();


        var prev_doc_id: usize = 0;
        var doc_id: usize = 0;

        var field_length: usize = 0;

        const positions = self.query_state.result_positions[partition_idx];

        for (0.., min_row_group..max_row_group) |idx, rg_idx| {
            if (idx != 0) {
                pq.freeRowGroup(buffer);
                buffer = pq.readRowGroup(
                    self.file_data.input_filename_c,
                    rg_idx,
                );
            }

            var byte_idx: usize = 0;
            while (byte_idx < buffer.len) {
                var search_col_idx: usize = 0;
                var col_idx: usize        = 0;
                const start_byte_idx = byte_idx;

                while (col_idx < self.columns.num_keys) {
                    var current_col_idx = col_idx;
                    for (current_col_idx..self.search_col_idxs.items[search_col_idx]) |_| {
                        field_length = pq.decodeVbyte(
                            buffer.ptr,
                            &byte_idx,
                        );
                        positions[col_idx].start_pos = @truncate(byte_idx - start_byte_idx);
                        positions[col_idx].field_len = @truncate(field_length);

                        byte_idx += field_length;
                        col_idx  += 1;
                    }

                    field_length = pq.decodeVbyte(
                        buffer.ptr,
                        &byte_idx,
                    );
                    positions[col_idx].start_pos = @truncate(byte_idx - start_byte_idx);
                    positions[col_idx].field_len = @truncate(field_length);

                    try current_IP.processDocVbyte(
                        &token_stream,
                        buffer[byte_idx..][0..field_length],
                        @truncate(doc_id), 
                        search_col_idx,
                        &terms_seen_bitset,
                        );

                    search_col_idx += 1;

                    byte_idx += field_length;
                    col_idx  += 1;

                    if (search_col_idx == self.search_col_idxs.items.len) {
                        current_col_idx = col_idx;
                        for (current_col_idx..self.columns.num_keys) |_| {
                            field_length = pq.decodeVbyte(
                                buffer.ptr,
                                &byte_idx,
                            );
                            positions[col_idx].start_pos = @truncate(byte_idx - start_byte_idx);
                            positions[col_idx].field_len = @truncate(field_length);

                            byte_idx += field_length;
                            col_idx  += 1;
                        }
                        std.debug.assert(col_idx == self.columns.num_keys);
                    }
                }
                doc_id += 1;

                if (timer.read() >= interval_ns) {
                    const current_docs_read = total_docs_read.fetchAdd(
                        doc_id - prev_doc_id, 
                        .monotonic
                        ) + (doc_id - prev_doc_id);
                    prev_doc_id = doc_id;
                    timer.reset();

                    self.indexing_state.last_progress = current_docs_read;

                    for (0..self.partitions.index_partitions.len) |_idx| {
                        if (self.indexing_state.partition_is_indexing[_idx]) {
                            progress_bar.update(current_docs_read, null);
                            break;
                        }
                    }
                }

                try current_IP.doc_store.addRow(
                    positions,
                    buffer[start_byte_idx..byte_idx],
                );
            }
        }
        pq.freeRowGroup(buffer);

        // Flush remaining tokens.
        for (0..self.search_col_idxs.items.len) |_search_col_idx| {
            try token_stream.flushTokenStream(_search_col_idx);
        }

        // Flush remaining doc storage.
        try current_IP.doc_store.flush();

        _ = total_docs_read.fetchAdd(doc_id - prev_doc_id, .monotonic);

        // Construct II
        // try current_IP.constructFromTokenStream(&token_stream);
    }

    pub fn scanFile(self: *IndexManager) !void {
        std.debug.print("Filetype: {any}\n", .{self.file_data.file_type});
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

        const num_partitions: usize =  if (file_size > (1 << 24)) 
            @min(
            try std.Thread.getCpuCount(), 
            MAX_NUM_THREADS,
            )
            else 1;

        self.partitions.row_offsets  = try self.gpa().alloc(usize, num_partitions + 1);
        self.partitions.byte_offsets = try self.gpa().alloc(usize, num_partitions + 1);

        const chunk_size: usize = @divFloor(file_size, num_partitions);
        self.partitions.byte_offsets[0] = 0;
        self.partitions.row_offsets[0]  = 0;

        var file_pos: u64 = @intCast(self.file_data.header_bytes.?);

        // Time read.
        const start_time = std.time.microTimestamp();

        var partition_idx: usize = 0;
        var line_count: u64 = 0;
        while (file_pos < file_size - 1) {
            const buffer = buffered_reader.getBuffer(@intCast(file_pos)) catch {
                std.debug.print("Error reading buffer at {d}\n", .{file_pos});
                return error.BufferError;
            };

            if (file_pos - self.partitions.byte_offsets[partition_idx] > chunk_size) {
                @branchHint(.unlikely);
                partition_idx += 1;

                self.partitions.byte_offsets[partition_idx] = file_pos;
                self.partitions.row_offsets[partition_idx]  = line_count;
            }

            var index: usize = 0;
            try csv.iterLineCSV(buffer, &index);
            file_pos += @intCast(index);
            line_count += 1;
        }

        self.partitions.byte_offsets[partition_idx + 1] = file_pos;
        self.partitions.row_offsets[partition_idx + 1]  = line_count;

        const end_time = std.time.microTimestamp();
        const execution_time_us: usize = @intCast(end_time - start_time);
        const mb_s = @divFloor(file_size, execution_time_us);

        std.debug.print(
            "Read {d} lines in {d}ms\n", 
            .{line_count, @divFloor(execution_time_us, 1000)},
            );
        std.debug.print("{d}MB/s\n", .{mb_s});

        try self.initPartitions();
    }

    pub fn scanParquetFile(self: *IndexManager) !void {
        self.file_data.pq_data.?.num_row_groups = pq.getNumRowGroupsParquet(
            self.file_data.input_filename_c,
        );
        const num_rg = self.file_data.pq_data.?.num_row_groups;

        const num_partitions: usize = @min(
            try std.Thread.getCpuCount(), 
            MAX_NUM_THREADS,
            num_rg,
            );
        // const num_partitions = 1;

        self.file_data.file_handles      = try self.gpa().alloc(std.fs.File, num_partitions);
        self.partitions.index_partitions = try self.gpa().alloc(BM25Partition, num_partitions);
        self.query_state.results_arrays  = try self.gpa().alloc(SortedScoreMultiArray(QueryResult), num_partitions);

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

            const reader = pq.getSerializedReader(self.file_data.input_filename_c);

            var num_rows: usize = 0;
            for (0.., min_row_group..max_row_group) |i, rg_idx| {
                row_group_sizes[i] = pq.getNumRowGroupsInRowGroup(
                    // self.file_data.input_filename_c,
                    reader,
                    rg_idx,
                );
                num_rows += row_group_sizes[i];
            }

            self.partitions.index_partitions[idx] = try BM25Partition.init(
                self.gpa(), 
                1, 
                num_rows,
                self.file_data.tmp_dir,
                );
        }
    }

    fn initPartitions(self: *IndexManager) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }

        const num_partitions = self.partitions.row_offsets.len - 1;

        self.file_data.file_handles      = try self.gpa().alloc(std.fs.File, num_partitions);
        self.partitions.index_partitions = try self.gpa().alloc(BM25Partition, num_partitions);
        self.query_state.results_arrays  = try self.gpa().alloc(SortedScoreMultiArray(QueryResult), num_partitions);

        for (0..num_partitions) |idx| {
            self.file_data.file_handles[idx]     = try std.fs.cwd().openFile(self.file_data.inputFilename(), .{});
            self.query_state.results_arrays[idx] = try SortedScoreMultiArray(QueryResult).init(self.gpa(), MAX_NUM_RESULTS);

            const start_row = self.partitions.row_offsets[idx];
            const end_row   = self.partitions.row_offsets[idx + 1];
            const num_rows  = end_row - start_row;

            self.partitions.index_partitions[idx] = try BM25Partition.init(
                self.gpa(), 
                1, 
                num_rows,
                self.file_data.tmp_dir,
                );
        }
    }


    pub fn indexFile(self: *IndexManager) !void {
        defer {
            _ = self.allocators.scratch_arena.reset(.retain_capacity);
        }
        self.indexing_state.partition_is_indexing = try self.scratchArena().alloc(
            bool, 
            self.partitions.index_partitions.len,
            );
        @memset(self.indexing_state.partition_is_indexing[0..], true);

        const num_partitions = self.partitions.index_partitions.len;
        std.debug.print("Indexing {d} partitions\n", .{num_partitions});

        const num_lines = if (self.file_data.file_type == .PARQUET)
            pq.getNumRowsParquet(self.file_data.input_filename_c)
            else self.partitions.row_offsets[self.partitions.row_offsets.len - 1];

        var num_rg: usize = 0;
        if (self.file_data.file_type == .PARQUET) {
            num_rg = self.file_data.pq_data.?.num_row_groups;
        }

        const time_start = std.time.milliTimestamp();

        var threads = try self.scratchArena().alloc(std.Thread, num_partitions);

        var total_docs_read = AtomicCounter.init(0);
        var progress_bar = progress.ProgressBar.init(num_lines, .K);

        const num_search_cols = self.search_col_idxs.items.len;
        
        for (0..num_partitions) |partition_idx| {
            try self.partitions.index_partitions[partition_idx].resizeNumSearchCols(
                num_search_cols,
                self.file_data.tmp_dir,
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
        progress_bar.update(_total_docs_read, null);

        const time_end = std.time.milliTimestamp();
        const time_diff = time_end - time_start;
        std.debug.print("Processed {d} documents in {d}ms\n", .{_total_docs_read, time_diff});

        // Init thread pool.
        try self.query_state.thread_pool.init(
            .{
                .allocator = self.gpa(),
                .n_jobs = self.partitions.index_partitions.len,
            },
        );
    }

    pub fn queryPartitionDAATUnion(
        _: *IndexManager,
        _: SHM,
        _: std.ArrayList(f32),
        _: usize,
        _: *SortedScoreMultiArray(QueryResult),
    ) void {}

    // pub fn queryPartitionDAATUnion(
        // self: *IndexManager,
        // queries: SHM,
        // boost_factors: std.ArrayList(f32),
        // partition_idx: usize,
        // query_results: *SortedScoreMultiArray(QueryResult),
    // ) void {
        // const num_search_cols = self.search_col_idxs.items.len;
        // std.debug.assert(num_search_cols > 0);
// 
        // var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;
// 
        // var search_col_counts = self.gpa().alloc(usize, num_search_cols) catch {
            // @panic("Failed to alloc search_col_counts.");
        // };
        // defer self.gpa().free(search_col_counts);
// 
        // @memset(search_col_counts, 0);
// 
        // const bigrams = self.gpa().alloc(
            // std.ArrayListUnmanaged(u64),
            // num_search_cols,
        // ) catch {
            // @panic("Failed to alloc bigrams.");
        // };
        // defer {
            // for (bigrams) |*inner_arr| {
                // inner_arr.deinit(self.gpa());
            // }
            // self.gpa().free(bigrams);
        // }
        // for (bigrams) |*bg| {
            // bg.* = std.ArrayListUnmanaged(u64){};
        // }
// 
        // var empty_query = true; 
// 
        // // TODO: Figure out meta typing to just map doc_id and term_pos types.
        // var iterators = std.ArrayListUnmanaged(
            // PostingsIterator,
            // // PostingsIteratorV2,
        // ){};
        // defer iterators.deinit(self.gpa());
// 
        // var idf_remaining: f32 = 0.0;
// 
        // var query_it = queries.iterator();
        // while (query_it.next()) |entry| {
            // const col_idx = self.columns.find(entry.key_ptr.*) catch {
                // std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                // continue;
            // };
            // const II_idx = findSorted(
                // u32,
                // self.search_col_idxs.items,
                // col_idx,
            // ) catch {
                // std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                // @panic("Column not found. Check findSorted function and self.columns map.");
            // };
// 
            // std.debug.assert(II_idx <= self.search_col_idxs.items.len);
// 
            // var term_len: usize = 0;
// 
            // var term_pos: u16 = 0;
            // for (entry.value_ptr.*) |c| {
                // switch (c) {
                    // 0...33, 35...47, 58...64, 91...96, 123...126 => {
                        // if (term_len == 0) continue;
// 
                        // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                        // const token = II.vocab.get(
                            // term_buffer[0..term_len],
                            // II.vocab.getAdapter(),
                            // );
                        // if (token) |_token| {
                            // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                               // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                            // const boost_weighted_idf: f32 = (
                                // 1.0 + std.math.log2(inner_term)
                                // ) * boost_factors.items[II_idx];
// 
                            // std.debug.assert(II_idx <= self.search_col_idxs.items.len);
// 
                            // const iterator_ptr = iterators.addOne(
                                // self.gpa(),
                            // ) catch @panic("Error adding iterator");
                            // // iterator_ptr.* = PostingsIteratorV2.init(
                            // iterator_ptr.* = PostingsIterator.init(
                                // II.postings,
                                // _token,
                                // @truncate(II_idx),
                                // @intFromFloat(boost_weighted_idf),
                                // // if (II.doc_freqs.items.len == 1) II.postings.doc_id_ptrs[_token] else null,
                                // // if (II.doc_freqs.items.len == 1) II.postings.term_pos_ptrs[_token] else null,
                            // );
// 
                            // term_pos += 1;
                            // empty_query = false;
                            // idf_remaining += boost_weighted_idf;
// 
                            // search_col_counts[II_idx] += 1;
                            // if (search_col_counts[II_idx] > 1) {
                                // bigrams[II_idx].append(
                                    // self.gpa(),
                                    // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                                    // @as(u64, @intCast(_token)),
                                // ) catch {
                                    // @panic("Failed to append bigram.");
                                // };
                            // }
                        // }
                        // term_len = 0;
                        // continue;
                    // },
                    // else => {
                        // term_buffer[term_len] = std.ascii.toUpper(c);
                        // term_len += 1;
// 
                        // if (term_len == MAX_TERM_LENGTH) {
                            // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                            // const token = II.vocab.get(
                                // term_buffer[0..term_len],
                                // II.vocab.getAdapter(),
                                // );
// 
                            // if (token) |_token| {
                                // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                                   // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                                // const boost_weighted_idf: f32 = (
                                    // 1.0 + std.math.log2(inner_term)
                                    // ) * boost_factors.items[II_idx];
// 
                                // const iterator_ptr = iterators.addOne(
                                    // self.gpa(),
                                // ) catch @panic("Error adding iterator");
                                // // iterator_ptr.* = PostingsIteratorV2.init(
                                // iterator_ptr.* = PostingsIterator.init(
                                    // II.postings,
                                    // _token,
                                    // @truncate(II_idx),
                                    // @intFromFloat(boost_weighted_idf),
                                    // // if (II.doc_freqs.items.len == 1) II.postings.doc_id_ptrs[_token] else null,
                                    // // if (II.doc_freqs.items.len == 1) II.postings.term_pos_ptrs[_token] else null,
                                // );
// 
                                // term_pos += 1;
                                // empty_query = false;
                                // idf_remaining += boost_weighted_idf;
// 
                                // search_col_counts[II_idx] += 1;
                                // if (search_col_counts[II_idx] > 1) {
                                    // bigrams[II_idx].append(
                                        // self.gpa(),
                                        // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                                        // @as(u64, @intCast(_token)),
                                    // ) catch {
                                        // @panic("Failed to append bigram.");
                                    // };
                                // }
                            // }
                            // term_len = 0;
                        // }
                    // },
                // }
            // }
// 
            // if (term_len > 0) {
                // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                // const token = II.vocab.get(
                    // term_buffer[0..term_len],
                    // II.vocab.getAdapter(),
                    // );
// 
                // if (token) |_token| {
                    // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                       // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                    // const boost_weighted_idf: f32 = (
                        // 1.0 + std.math.log2(inner_term)
                        // ) * boost_factors.items[II_idx];
// 
                    // const iterator_ptr = iterators.addOne(
                        // self.gpa(),
                    // ) catch @panic("Error adding iterator");
                    // // iterator_ptr.* = PostingsIteratorV2.init(
                    // iterator_ptr.* = PostingsIterator.init(
                        // II.postings,
                        // _token,
                        // @truncate(II_idx),
                        // @intFromFloat(boost_weighted_idf),
                        // // if (II.doc_freqs.items.len == 1) II.postings.doc_id_ptrs[_token] else null,
                        // // if (II.doc_freqs.items.len == 1) II.postings.term_pos_ptrs[_token] else null,
                    // );
// 
                    // term_pos += 1;
                    // empty_query = false;
                    // idf_remaining += boost_weighted_idf;
// 
                    // search_col_counts[II_idx] += 1;
                    // if (search_col_counts[II_idx] > 1) {
                        // bigrams[II_idx].append(
                            // self.gpa(),
                            // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                            // @as(u64, @intCast(_token)),
                        // ) catch {
                            // @panic("Failed to append bigram.");
                        // };
                    // }
                // }
            // }
        // }
// 
        // if (empty_query) {
            // std.debug.print("Empty query\n", .{});
            // var iterator = queries.iterator();
            // std.debug.print("\n", .{});
            // std.debug.print("-----------------------------------------", .{});
            // while (iterator.next()) |item| {
                // std.debug.print("{s}: {s}\n", .{item.key_ptr.*, item.value_ptr.*});
            // }
            // std.debug.print("-----------------------------------------", .{});
            // std.debug.print("\n", .{});
            // return;
        // }
// 
        // var sorted_scores = SortedScoreMultiArray(u32).init(
            // self.gpa(), 
            // query_results.capacity,
            // ) catch {
                // @panic("Failed to init sorted scores");
            // };
        // defer sorted_scores.deinit();
// 
        // if (iterators.items.len == 1) {
            // const score: f32 = @floatFromInt(iterators.items[0].score);
// 
            // var score_sum: f32 = 0.0;
            // var res = iterators.items[0].nextSlice();
// 
            // while (res.doc_id != std.math.maxInt(u32)) {
                // score_sum = 0.0;
                // for (res.term_pos) |tp| {
// 
                    // // TODO: Assess how we want to do term pos scoring.
                    // score_sum += score + 25.0 * @as(f32, @floatFromInt(@intFromBool(tp == 0)));
                // }
                // sorted_scores.insert(res.doc_id, score_sum);
                // res = iterators.items[0].nextSlice();
            // }
// 
            // // std.debug.print("\nTOTAL DOCS SCORED: {d}\n", .{iterators.items[0].len});
// 
            // for (0..sorted_scores.count) |idx| {
// 
                // const result = QueryResult{
                    // .doc_id = sorted_scores.items[idx],
                    // .partition_idx = @intCast(partition_idx),
                // };
                // query_results.insert(result, sorted_scores.scores[idx]);
            // }
// 
            // return;
        // }
// 
        // var doc_term_positions = self.gpa().alloc(
            // SortedIntMultiArray(u32, false), 
            // num_search_cols,
            // ) catch {
            // @panic("Failed to alloc doc_term_positions.");
        // };
        // for (doc_term_positions) |*arr| {
            // arr.* = SortedIntMultiArray(u32, false).init(
                // self.gpa(),
                // 64,
            // ) catch {
                // @panic("Failed to init doc_term_positions.");
            // };
        // }
        // defer {
            // for (doc_term_positions) |*pos| {
                // pos.deinit();
            // }
            // self.gpa().free(doc_term_positions);
        // }
// 
        // var total_docs_scored: usize = 0;
// 
        // var consumed_mask:  @Vector(8, bool) = @splat(false);
        // for (iterators.items.len..8) |idx| {
            // consumed_mask[idx] = true;
        // }
        // var doc_id_vec: @Vector(8, u32) = @splat(0);
        // var score_vec:  @Vector(8, u16) = @splat(0);
        // std.debug.assert(iterators.items.len <= 8);
        // for (0.., iterators.items) |idx, *it| {
            // // score_vec[idx] = @truncate(it.score);
// 
            // // multiply by 1.25 to help account for potential term_pos boosts.
            // score_vec[idx] = @truncate(@divFloor(it.score * 5, 4));
        // }
// 
        // // PROCESS
        // // 1. Do (doc_id_vec < doc_id) for each iterator, to identify iterators which could potentially
        // //    contain `doc_id`, and could impact the score.
        // // 2. Use mask from 1 on score_vec, then take sum to get the approx upper bound if we chose that
        // //    iterator's doc_id as the pivot_doc_id.
        // // 3. Choose the largest pivot_doc_id whose upper_bound is below min_score.
// 
        // // --- MAIN WAND LOOP ---
        // var current_doc_id: u32 = 0;
        // while (std.simd.countTrues(consumed_mask) < 8) {
// 
            // inline for (0..8) |idx| {
                // if (!consumed_mask[idx]) {
                    // doc_id_vec[idx] = iterators.items[idx].currentDocId();
                // }
            // }
// 
            // var max_doc_id: u32 = 0;
            // var min_doc_id: u32 = std.math.maxInt(u32);
// 
            // for (0.., iterators.items) |idx, *it| {
                // if (consumed_mask[idx]) continue;
// 
                // const doc_id: @Vector(8, u32) = @splat(it.currentDocId());
                // const upper_bound = @reduce(
                    // .Add,
                    // score_vec * @as(@Vector(8, u16), @intFromBool(doc_id_vec < doc_id)),
                    // );
// 
                // min_doc_id = @min(it.currentDocId(), min_doc_id);
// 
                // if (@as(f32, @floatFromInt(upper_bound)) > sorted_scores.lastScore()) continue;
// 
                // if (it.currentDocId() > max_doc_id) {
                    // max_doc_id = it.currentDocId();
                // }
            // }
// 
            // if (max_doc_id > min_doc_id) {
                // current_doc_id = std.math.maxInt(u32);
                // for (0..iterators.items.len) |idx| {
                    // if (consumed_mask[idx]) continue;
// 
                    // var iterator = &iterators.items[idx];
                    // // const res = iterator.advanceTo(max_doc_id + 1);
                    // const res = iterator.advanceToSlice(max_doc_id + 1);
// 
                    // if (res.doc_id == std.math.maxInt(u32)) {
                        // consumed_mask[idx] = true;
                    // } else {
                        // current_doc_id = @min(res.doc_id, current_doc_id);
                    // }
                // }
            // } else {
                // std.debug.assert(current_doc_id != std.math.maxInt(u32));
                // current_doc_id = min_doc_id;
            // }
            // if (current_doc_id == std.math.maxInt(u32)) {
                // std.debug.assert(std.simd.countTrues(consumed_mask) == 8);
                // continue;
            // }
// 
            // total_docs_scored += 1;
// 
            // for (doc_term_positions) |*pos| {
                // pos.clear();
            // }
// 
            // var base_score: f32 = 0.0;
            // for (0.., iterators.items) |idx, *it| {
                // if (consumed_mask[idx]) continue;
                // if (it.currentDocId() > current_doc_id) continue;
// 
                // std.debug.assert(it.currentDocId() == current_doc_id);
// 
                // const res = it.nextSlice();
                // for (0.., res.term_pos) |tp_idx, tp| {
                    // if (tp_idx == 64) {
                        // @branchHint(.cold);
                        // break;
                    // }
                    // base_score += @as(f32, @floatFromInt(it.score)) +
                                  // 25.0 * @as(f32, @floatFromInt(@intFromBool(tp == 0)));
                    // doc_term_positions[it.col_idx].insert(
                        // it.term_id,
                        // tp & 0b01111111_11111111,
                    // );
                // }
// 
                // if (res.doc_id == std.math.maxInt(u32)) {
                    // consumed_mask[idx] = true;
                // }
            // }
// 
            // // 3c. Apply Phrase Scoring Boost
            // var final_score = base_score;
            // for (0.., doc_term_positions) |col_idx, *pos| {
                // if (pos.count <= 1) continue;
// 
                // std.debug.assert(pos.count < 1024);
// 
                // // TODO: Double check this logic with multiple vals.
                // for (0..(pos.count - 1)) |idx| {
                    // const diff = pos.values[idx + 1] - pos.values[idx];
                    // if (diff != 1) continue;
// 
                    // // Form the bigram from the term IDs.
                    // const new_bigram = @as(u64, @intCast(pos.items[idx])) << 32 |
                                       // @as(u64, @intCast(pos.items[idx + 1]));
// 
                    // // Check against the query's required bigrams.
                    // for (bigrams[col_idx].items) |required_bigram| {
                        // if (required_bigram == new_bigram) {
                            // final_score *= 1.4;
                        // }
                    // }
                // }
            // }
// 
            // sorted_scores.insert(
                // current_doc_id,
                // final_score,
            // );
// 
            // // std.debug.print("Total docs scored: {d} | Current doc ID: {d} | Final score: {d}\n",
                // // .{total_docs_scored, max_doc_id, final_score},
                // // );
        // }
// 
        // // std.debug.print("\nTOTAL DOCS SCORED: {d}\n", .{total_docs_scored});
// 
        // for (0..sorted_scores.count) |idx| {
            // const result = QueryResult{
                // .doc_id = sorted_scores.items[idx],
                // .partition_idx = @intCast(partition_idx),
            // };
            // query_results.insert(result, sorted_scores.scores[idx]);
        // }
    // }

    pub fn queryPartitionDAATIntersection(
        _: *IndexManager,
        _: SHM,
        _: std.ArrayList(f32),
        _: usize,
        _: *SortedScoreMultiArray(QueryResult),
    ) void {}

    // pub fn queryPartitionDAATIntersection(
        // self: *IndexManager,
        // queries: SHM,
        // boost_factors: std.ArrayList(f32),
        // partition_idx: usize,
        // query_results: *SortedScoreMultiArray(QueryResult),
    // ) void {
        // const num_search_cols = self.search_col_idxs.items.len;
        // std.debug.assert(num_search_cols > 0);
// 
        // var term_buffer: [MAX_TERM_LENGTH]u8 = undefined;
// 
        // var search_col_counts = self.gpa().alloc(usize, num_search_cols) catch {
            // @panic("Failed to alloc search_col_counts.");
        // };
        // defer self.gpa().free(search_col_counts);
// 
        // @memset(search_col_counts, 0);
// 
        // const bigrams = self.gpa().alloc(
            // std.ArrayListUnmanaged(u64),
            // num_search_cols,
        // ) catch {
            // @panic("Failed to alloc bigrams.");
        // };
        // defer {
            // for (bigrams) |*inner_arr| {
                // inner_arr.deinit(self.gpa());
            // }
            // self.gpa().free(bigrams);
        // }
        // for (bigrams) |*bg| {
            // bg.* = std.ArrayListUnmanaged(u64){};
        // }
// 
        // var empty_query = true; 
// 
        // // TODO: Figure out meta typing to just map doc_id and term_pos types.
        // var iterators = std.ArrayListUnmanaged(
            // PostingsIterator,
            // // PostingsIteratorV2,
        // ){};
        // defer iterators.deinit(self.gpa());
// 
        // var idf_remaining: f32 = 0.0;
// 
        // var query_it = queries.iterator();
        // while (query_it.next()) |entry| {
            // const col_idx = self.columns.find(entry.key_ptr.*) catch {
                // std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                // continue;
            // };
            // const II_idx = findSorted(
                // u32,
                // self.search_col_idxs.items,
                // col_idx,
            // ) catch {
                // std.debug.print("Column {s} not found!\n", .{entry.key_ptr.*});
                // @panic("Column not found. Check findSorted function and self.columns map.");
            // };
// 
            // std.debug.assert(II_idx <= self.search_col_idxs.items.len);
// 
            // var term_len: usize = 0;
// 
            // var term_pos: u16 = 0;
            // for (entry.value_ptr.*) |c| {
                // switch (c) {
                    // 0...33, 35...47, 58...64, 91...96, 123...126 => {
                        // if (term_len == 0) continue;
// 
                        // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                        // const token = II.vocab.get(
                            // term_buffer[0..term_len],
                            // II.vocab.getAdapter(),
                            // );
                        // if (token) |_token| {
                            // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                               // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                            // const boost_weighted_idf: f32 = (
                                // 1.0 + std.math.log2(inner_term)
                                // ) * boost_factors.items[II_idx];
// 
                            // std.debug.assert(II_idx <= self.search_col_idxs.items.len);
// 
                            // const iterator_ptr = iterators.addOne(
                                // self.gpa(),
                            // ) catch @panic("Error adding iterator");
                            // // iterator_ptr.* = PostingsIteratorV2.init(
                            // iterator_ptr.* = PostingsIterator.init(
                                // II.postings,
                                // _token,
                                // @truncate(II_idx),
                                // @intFromFloat(boost_weighted_idf),
                                // // if (II.doc_freqs.items.len == 1) II.postings.doc_id_ptrs[_token] else null,
                                // // if (II.doc_freqs.items.len == 1) II.postings.term_pos_ptrs[_token] else null,
                            // );
// 
                            // term_pos += 1;
                            // empty_query = false;
                            // idf_remaining += boost_weighted_idf;
// 
                            // search_col_counts[II_idx] += 1;
                            // if (search_col_counts[II_idx] > 1) {
                                // bigrams[II_idx].append(
                                    // self.gpa(),
                                    // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                                    // @as(u64, @intCast(_token)),
                                // ) catch {
                                    // @panic("Failed to append bigram.");
                                // };
                            // }
                        // }
                        // term_len = 0;
                        // continue;
                    // },
                    // else => {
                        // term_buffer[term_len] = std.ascii.toUpper(c);
                        // term_len += 1;
// 
                        // if (term_len == MAX_TERM_LENGTH) {
                            // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                            // const token = II.vocab.get(
                                // term_buffer[0..term_len],
                                // II.vocab.getAdapter(),
                                // );
// 
                            // if (token) |_token| {
                                // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                                   // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                                // const boost_weighted_idf: f32 = (
                                    // 1.0 + std.math.log2(inner_term)
                                    // ) * boost_factors.items[II_idx];
// 
// 
                                // const iterator_ptr = iterators.addOne(
                                    // self.gpa(),
                                // ) catch @panic("Error adding iterator");
                                // // iterator_ptr.* = PostingsIteratorV2.init(
                                // iterator_ptr.* = PostingsIterator.init(
                                    // II.postings,
                                    // _token,
                                    // @truncate(II_idx),
                                    // @intFromFloat(boost_weighted_idf),
                                    // // if (II.doc_freqs.items.len == 1) II.postings.doc_id_ptrs[_token] else null,
                                    // // if (II.doc_freqs.items.len == 1) II.postings.term_pos_ptrs[_token] else null,
                                // );
// 
                                // term_pos += 1;
                                // empty_query = false;
                                // idf_remaining += boost_weighted_idf;
// 
                                // search_col_counts[II_idx] += 1;
                                // if (search_col_counts[II_idx] > 1) {
                                    // bigrams[II_idx].append(
                                        // self.gpa(),
                                        // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                                        // @as(u64, @intCast(_token)),
                                    // ) catch {
                                        // @panic("Failed to append bigram.");
                                    // };
                                // }
                            // }
                            // term_len = 0;
                        // }
                    // },
                // }
            // }
// 
            // if (term_len > 0) {
                // const II = &self.partitions.index_partitions[partition_idx].II[II_idx];
// 
                // const token = II.vocab.get(
                    // term_buffer[0..term_len],
                    // II.vocab.getAdapter(),
                    // );
// 
                // if (token) |_token| {
                    // const inner_term = @as(f32, @floatFromInt(II.num_docs)) / 
                                       // @as(f32, @floatFromInt(II.doc_freqs.items[_token]));
                    // const boost_weighted_idf: f32 = (
                        // 1.0 + std.math.log2(inner_term)
                        // ) * boost_factors.items[II_idx];
// 
                    // const iterator_ptr = iterators.addOne(
                        // self.gpa(),
                    // ) catch @panic("Error adding iterator");
                    // // iterator_ptr.* = PostingsIteratorV2.init(
                    // iterator_ptr.* = PostingsIterator.init(
                        // II.postings,
                        // _token,
                        // @truncate(II_idx),
                        // @intFromFloat(boost_weighted_idf),
                        // // if (II.doc_freqs.items.len == 1) II.doc_freqs.items[_token] else null,
                    // );
// 
                    // term_pos += 1;
                    // empty_query = false;
                    // idf_remaining += boost_weighted_idf;
// 
                    // search_col_counts[II_idx] += 1;
                    // if (search_col_counts[II_idx] > 1) {
                        // bigrams[II_idx].append(
                            // self.gpa(),
                            // (@as(u64, @intCast(iterators.items[iterators.items.len - 2].term_id)) << 32) | 
                            // @as(u64, @intCast(_token)),
                        // ) catch {
                            // @panic("Failed to append bigram.");
                        // };
                    // }
                // }
            // }
        // }
// 
        // if (empty_query) {
            // std.debug.print("Empty query\n", .{});
            // var iterator = queries.iterator();
            // std.debug.print("\n", .{});
            // std.debug.print("-----------------------------------------", .{});
            // while (iterator.next()) |item| {
                // std.debug.print("{s}: {s}\n", .{item.key_ptr.*, item.value_ptr.*});
            // }
            // std.debug.print("-----------------------------------------", .{});
            // std.debug.print("\n", .{});
            // return;
        // }
// 
        // var sorted_scores = SortedScoreMultiArray(u32).init(
            // self.gpa(), 
            // query_results.capacity,
            // ) catch {
                // @panic("Failed to init sorted scores");
            // };
        // defer sorted_scores.deinit();
// 
        // if (iterators.items.len == 1) {
            // const score: f32 = @floatFromInt(iterators.items[0].score);
// 
            // var score_sum: f32 = 0.0;
            // var res = iterators.items[0].nextSlice();
// 
            // while (res.doc_id != std.math.maxInt(u32)) {
                // score_sum = 0.0;
                // for (res.term_pos) |tp| {
// 
                    // // TODO: Assess how we want to do term pos scoring.
                    // tp &= 0b01111111_11111111;
                    // score_sum += score + 25.0 * @as(f32, @floatFromInt(@intFromBool(tp == 0)));
                // }
                // sorted_scores.insert(res.doc_id, score_sum);
                // res = iterators.items[0].nextSlice();
            // }
// 
            // // std.debug.print("\nTOTAL DOCS SCORED: {d}\n", .{iterators.items[0].len});
// 
            // for (0..sorted_scores.count) |idx| {
// 
                // const result = QueryResult{
                    // .doc_id = sorted_scores.items[idx],
                    // .partition_idx = @intCast(partition_idx),
                // };
                // query_results.insert(result, sorted_scores.scores[idx]);
            // }
// 
            // return;
        // }
// 
        // // Sort iterators by score.
        // sortStruct(
            // PostingsIterator,
            // // PostingsIteratorV2,
            // iterators.items,
            // "score",
            // true,
        // );
        // // Check sorting.
        // for (0..iterators.items.len) |idx| {
            // const iterator = iterators.items[idx];
            // std.debug.print("Score: {d}\n", .{iterator.score});
        // }
// 
        // var doc_term_positions = self.gpa().alloc(
            // SortedIntMultiArray(u32, false), 
            // num_search_cols,
            // ) catch {
            // @panic("Failed to alloc doc_term_positions.");
        // };
        // defer self.gpa().free(doc_term_positions);
        // for (doc_term_positions) |*arr| {
            // arr.* = SortedIntMultiArray(u32, false).init(
                // self.gpa(),
                // 64,
            // ) catch {
                // @panic("Failed to init doc_term_positions.");
            // };
        // }
// 
        // // TODO: Fix rest of this function to work with PostingsV2
        // var lead_iter = &iterators.items[0];
        // var total_docs_scored: usize = 0;
        // while (true) {
            // const result = lead_iter.next();
            // if (result.doc_id == std.math.maxInt(u32)) break;
            // for (doc_term_positions) |*pos| {
                // pos.clear();
            // }
// 
            // doc_term_positions[lead_iter.col_idx].insert(
                // lead_iter.term_id,
                // result.term_pos,
            // );
// 
            // const doc_id = result.doc_id;
// 
            // var score = iterators.items[0].score;
            // for (iterators.items[1..]) |*iterator| {
                // const score_add = iterator.score;
// 
                // const res = iterator.advanceTo(doc_id);
                // if (res.doc_id == doc_id) {
                    // score += score_add;
                // }
// 
                // doc_term_positions[iterator.col_idx].insert(
                    // iterator.term_id,
                    // res.term_pos,
                // );
            // }
// 
            // // Term pos scoring.
            // for (0.., doc_term_positions) |II_idx, *pos| {
                // if (pos.count <= 1) continue;
// 
                // for (0..(pos.count - 1)) |idx| {
                    // const diff = pos.values[idx + 1] - pos.values[idx];
                    // if (diff != 1) continue;
// 
                    // const new_bigram = @as(u64, @intCast(pos.items[idx])) << 32 | 
                                       // @as(u64, @intCast(pos.items[idx + 1]));
                    // for (bigrams[II_idx].items) |bg| {
                        // score *= 2 * @as(u32, @intCast(@intFromBool(bg == new_bigram)));
                    // }
                // }
            // }
// 
            // sorted_scores.insert(
                // doc_id,
                // @floatFromInt(score),
            // );
            // total_docs_scored += 1;
        // }
// 
        // std.debug.print("\nTOTAL DOCS SCORED: {d}\n", .{total_docs_scored});
// 
        // for (0..sorted_scores.count) |idx| {
// 
            // const result = QueryResult{
                // .doc_id = sorted_scores.items[idx],
                // .partition_idx = @intCast(partition_idx),
            // };
            // query_results.insert(result, sorted_scores.scores[idx]);
        // }
    // }

    pub fn query(
        self: *IndexManager,
        queries: SHM,
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
        const num_partitions = self.partitions.index_partitions.len;

        var wg: std.Thread.WaitGroup = .{};

        // const _start = std.time.microTimestamp();
        for (0..num_partitions) |partition_idx| {
            self.query_state.results_arrays[partition_idx].clear();
            self.query_state.results_arrays[partition_idx].resize(k);
            self.query_state.thread_pool.spawnWg(
                &wg,
                // queryPartitionDAATIntersection,
                queryPartitionDAATUnion,
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
        // const _end = std.time.microTimestamp();
        // const time_taken_us_query_only = _end - _start;
        // std.debug.print(
            // "Query took {d} us\n", 
            // .{time_taken_us_query_only},
        // );

        if (self.partitions.index_partitions.len > 1) {
            for (self.query_state.results_arrays[1..]) |*tr| {
                for (0.., tr.items[0..tr.count]) |idx, r| {
                    self.query_state.results_arrays[0].insert(r, tr.scores[idx]);
                }
            }
        }

        if (self.query_state.results_arrays[0].count == 0) return;

        // const start_2 = std.time.microTimestamp();

        var wg2: std.Thread.WaitGroup = .{};
        for (0..self.query_state.results_arrays[0].count) |idx| {
            const result = self.query_state.results_arrays[0].items[idx];

            std.debug.assert(self.query_state.result_strings[idx].capacity > 0);
            self.query_state.thread_pool.spawnWg(
                &wg2,
                fetchRecordsDocStore,
                .{
                    &self.partitions.index_partitions[result.partition_idx],
                    self.query_state.result_positions[idx],
                    result,
                    &self.query_state.result_strings[idx],

                    self.query_state.bit_sizes[idx],
                },
            );
        }

        wg2.wait();

        // const end_2 = std.time.microTimestamp();
        // const time_taken_us_fetch = end_2 - start_2;
        // std.debug.print(
            // "Fetch took {d} us\n", 
            // .{time_taken_us_fetch},
        // );
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
