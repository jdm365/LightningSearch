const std = @import("std");
const string_utils = @import("string_utils.zig");
const file_utils = @import("file_utils.zig");
const progress = @import("progress.zig");
const rt = @import("radix_trie.zig");
const index = @import("index.zig");


const FlatFileIndex = struct {
    gpa: std.heap.GeneralPurposeAllocator(.{}),
    string_arena: std.heap.ArenaAllocator,
    postings: index.Postings,
    term_offsets: []usize,

    vocab: std.StringHashMap(u32),
    doc_freqs: std.ArrayList(u32),
    doc_sizes: []u64,

    avg_doc_size: f32,

    pub fn init(
        gpa: std.heap.GeneralPurposeAllocator(.{}),
        string_arena: std.heap.ArenaAllocator,
        num_docs: usize,
        ) !FlatFileIndex {
        var vocab = std.StringHashMap(u32).init(gpa.allocator());

        // Guess capacity
        try vocab.ensureTotalCapacity(@intCast(num_docs / 25));

        const FFI = FlatFileIndex{
            .gpa = gpa,
            .string_arena = string_arena,

            .postings = index.Postings{
                .doc_ids = undefined,
                .term_positions = undefined,
            },
            .vocab = vocab,
            .term_offsets = &[_]usize{},
            .doc_freqs = try std.ArrayList(u32).initCapacity(
                gpa.allocator(), @as(usize, @intFromFloat(@as(f32, @floatFromInt(num_docs)) * 0.1))
                ),
            .doc_sizes = try gpa.allocator().alloc(u16, num_docs),
            .avg_doc_size = 0.0,
        };
        @memset(FFI.doc_sizes, 0);
        return FFI;
    }

    pub fn deinit(
        self: *FlatFileIndex,
        ) void {
        self.gpa.allocator().free(self.postings.term_positions);
        self.gpa.allocator().free(self.postings.doc_ids);

        self.vocab.deinit();

        self.gpa.allocator().free(self.term_offsets);
        self.doc_freqs.deinit();
        self.gpa.allocator().free(self.doc_sizes);

        _ = self.gpa.deinit();
        self.string_arena.deinit();
    }

    pub fn resizePostings(
        self: *FlatFileIndex,
        allocator: std.mem.Allocator,
        ) !void {
        self.term_offsets = try allocator.alloc(usize, self.doc_freqs.items.len + 1);

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
        avg_doc_size /= @floatFromInt(self.doc_sizes.len);
        self.avg_doc_size = @floatCast(avg_doc_size);
    }

    inline fn addTerm(
        self: *FlatFileIndex,
        term: []u8,
        term_len: usize,
        term_pos: u64,
        token_stream: *file_utils.TokenStream(file_utils.token_64t),
        file_terms: std.AutoHashMap(u32, void),
    ) !void {

        const gop = try self.vocab.getOrPut(term[0..term_len]);

        if (!gop.found_existing) {
            const term_copy = try self.string_arena.allocator().dupe(
                u8, 
                term[0..term_len],
                );

            gop.key_ptr.* = term_copy;
            gop.value_ptr.* = self.II[0].num_terms;
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

    fn tokenize(
        self: *FlatFileIndex,
        buffer: []const u8,
        token_stream: *file_utils.TokenStream(file_utils.token_64t),
        file_terms: *std.AutoHashMap(u32, void),
        doc_size: *usize,
        ) usize {
        var buffer_pos: usize = 0;
        while (buffer_pos < buffer.len) {
            const skip_chars = string_utils.simdFindCharInRangesEscapedFull(
                buffer[buffer_pos], 
                string_utils.SEP_RANGES,
                );
            try self.addTerm(
                buffer[buffer_pos],
                skip_chars,
                doc_size.*,
                token_stream,
                file_terms,
                );
            buffer_pos += skip_chars + 1;
            while (buffer[buffer_pos] == ' ') buffer_pos += 1;
        }
        return buffer.len;
    }

    pub fn indexFile(
        self: *FlatFileIndex,
        filename: []const u8,
        ) !void {
        const file_hash = blk: {
            var hash: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(self.input_filename, &hash, .{});
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

        std.fs.cwd().makeDir(self.tmp_dir) catch {
            try std.fs.cwd().deleteTree(self.tmp_dir);
            try std.fs.cwd().makeDir(self.tmp_dir);
        };
        defer {
            std.fs.cwd().deleteTree(self.tmp_dir) catch |err| {
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

        var doc_size: usize = 0;
        const file_size = token_stream.input_file.getEndPos();
        while (file_pos < file_size - 1) {
            const buffer = try token_stream.getBuffer(file_pos);
            file_pos += self.tokenize(
                buffer, 
                &token_stream, 
                &file_terms, 
                &doc_size,
                );
        }
        try token_stream.flushTokenStream(0);

        try self.constructFromTokenStream(&token_stream);
    }

    pub fn constructFromTokenStream(
        self: *FlatFileIndex,
        token_stream: *file_utils.TokenStream(file_utils.token_32t),
        ) !void {

        var term_cntr = try self.gpa.allocator().alloc(usize, self.vocab.count());
        defer self.gpa.allocator().free(term_cntr);

        try self.resizePostings(self.allocator);

        if (self.vocab.count() > term_cntr.len) {
            term_cntr = try self.gpa.allocator().realloc(
                term_cntr, 
                self.vocab.count(),
                );
        }
        @memset(term_cntr, 0);

        // Create index.
        const output_file = &token_stream.output_files[0];
        try output_file.seekTo(0);

        const tokens = token_stream.tokens[0];

        var bytes_read: usize = 0;

        var num_tokens: usize = file_utils.TOKEN_STREAM_CAPACITY;
        var current_doc_id: usize = 0;

        while (num_tokens == file_utils.TOKEN_STREAM_CAPACITY) {
            var _num_tokens: [@sizeOf(file_utils.token_64t)]u8 = undefined;
            _ = try output_file.read(std.mem.asBytes(&_num_tokens));
            num_tokens = std.mem.readInt(u32, &_num_tokens, file_utils.ENDIANESS);

            bytes_read = try output_file.read(
                std.mem.sliceAsBytes(tokens.*[0..num_tokens])
                );

            for (0..num_tokens) |idx| {
                const token = tokens[idx];

                const term_pos = token.term_pos;
                const term_id: usize = @intCast(token.term_id);

                const postings_offset = self.term_offsets[term_id] + term_cntr[term_id];
                std.debug.assert(postings_offset < self.postings.doc_ids.len);

                std.debug.assert(current_doc_id <= self.doc_sizes.len);

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
};
