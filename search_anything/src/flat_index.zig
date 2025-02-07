const std = @import("std");
const string_utils = @import("string_utils.zig");
const file_utils = @import("file_utils.zig");
const progress = @import("progress.zig");
const rt = @import("radix_trie.zig");
const index = @import("index.zig");


pub const FlatFileIndex = struct {
    gpa: *std.heap.GeneralPurposeAllocator(.{}),
    string_arena: std.heap.ArenaAllocator,
    postings: index.PostingsDynamic,

    vocab: std.StringHashMap(u32),
    doc_freqs: std.ArrayList(u32),
    doc_sizes: std.ArrayList(u64),

    avg_doc_size: f32,

    pub fn init() !FlatFileIndex {
        var string_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        var gpa = try string_arena.allocator().create(std.heap.GeneralPurposeAllocator(.{}));
        gpa.* = std.heap.GeneralPurposeAllocator(.{}){};

        const FFI = FlatFileIndex{
            .gpa = gpa,
            .string_arena = string_arena,

            .postings = try index.PostingsDynamic.init(gpa.allocator()),
            .vocab = std.StringHashMap(u32).init(gpa.allocator()),
            .doc_freqs = std.ArrayList(u32).init(gpa.allocator()),
            .doc_sizes = std.ArrayList(u64).init(gpa.allocator()),

            .avg_doc_size = 0.0,
        };
        return FFI;
    }

    pub fn deinit(
        self: *FlatFileIndex,
        ) void {
        self.postings.deinit();
        self.vocab.deinit();

        self.doc_freqs.deinit();
        self.doc_sizes.deinit();

        self.string_arena.deinit();
        _ = self.gpa.deinit();
    }

    inline fn addTerm(
        self: *FlatFileIndex,
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
        self: *FlatFileIndex,
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
        self: *FlatFileIndex,
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
        self: *FlatFileIndex,
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

                try self.postings.doc_ids.items[term_id].append(
                    self.postings.doc_ids.allocator,
                    current_doc_id,
                    );
                try self.postings.term_positions.items[term_id].append(
                    self.postings.term_positions.allocator,
                    @intCast(term_pos)
                    );

                token_count += 1;
            }
        }

        try self.doc_sizes.append(@intCast(token_count));
    }
};


test "index_txt" {
    // Treat as txt flat file.
    const filename: []const u8 = "../data/mb_small.csv";
    // const filename: []const u8 = "../data/mb.csv";

    var index_manager = try FlatFileIndex.init();
    defer index_manager.deinit();

    try index_manager.indexFile(filename);

    std.debug.print("Vocab size: {d}\n", .{index_manager.vocab.count()});
}
