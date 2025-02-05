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
        doc_id: u32,
        term_pos: u8,
        col_idx: usize,
        token_stream: *file_utils.TokenStream,
        file_terms: std.StringHashMap(void),
        new_doc: *bool,
    ) !void {

        const gop = try self.vocab.getOrPut(term[0..term_len]);

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
            if (!file_terms.checkOrInsert(val)) {
                self.II[col_idx].doc_freqs.items[val] += 1;
                try token_stream.addToken(new_doc.*, term_pos, val, col_idx);
            }
        }

        self.II[col_idx].doc_sizes[doc_id] += 1;
        new_doc.* = false;
    }

    fn tokenize(
        self: *FlatFileIndex,
        buffer: []const u8,
        file_terms: std.StringHashMap(void),
        ) void {
    }

    pub fn indexFile(
        self: *FlatFileIndex,
        filename: []const u8,
        ) !void {
        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();

        const file_size = try file.getEndPos();

        var buffered_reader = try file_utils.DoubleBufferedReader.init(
            self.gpa.allocator(),
            file,
            '\n',
        );
        defer buffered_reader.deinit(self.gpa.allocator());

        var file_pos: usize = 0;
        var file_terms = std.StringHashMap(void).init(self.gpa.allocator());
        defer file_terms.deinit();

        while (file_pos < file_size - 1) {
            const buffer = try buffered_reader.getBuffer(file_pos);
            self.tokenize(buffer);
            file_pos += buffer.len;
        }
    }
};
