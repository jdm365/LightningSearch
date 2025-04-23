const std     = @import("std");
const builtin = @import("builtin");

const HuffmanCompressor = @import("../compression/huffman.zig").HuffmanCompressor;

pub const DocStore = struct {
    huffman_compressor: HuffmanCompressor,
    literal_byte_sizes: *std.ArrayListUnmanaged(usize),
    literal_byte_size_sum: usize,
    literal_col_idxs: *std.ArrayListUnmanaged(usize),
    huffman_col_idxs: *std.ArrayListUnmanaged(usize),

    huffman_rows: std.ArrayListUnmanaged([]u8),
    literal_rows: std.ArrayListUnmanaged([]u8),

    pub fn init(
        literal_byte_sizes: *std.ArrayListUnmanaged(usize),
        literal_col_idxs: *std.ArrayListUnmanaged(usize),
        huffman_col_idxs: *std.ArrayListUnmanaged(usize),
    ) !DocStore {
        var sum: usize = 0;
        for (literal_byte_sizes.items) |byte_size| {
            sum += byte_size;
        }

        return DocStore {
            .huffman_compressor = try HuffmanCompressor.init(),
            .literal_byte_sizes = literal_byte_sizes,
            .literal_byte_size_sum = sum,
            .literal_col_idxs = literal_col_idxs,
            .huffman_col_idxs = huffman_col_idxs,

            .huffman_rows = std.ArrayListUnmanaged([]u8){},
            .literal_rows = std.ArrayListUnmanaged([]u8){},
        };
    }

    pub fn deinit(self: *DocStore, allocator: *std.mem.Allocator) void {
        self.huffman_rows.deinit(allocator);
        self.literal_rows.deinit(allocator);
    }

    pub fn addRow(
        self: *DocStore,
        allocator: *std.mem.Allocator,
        byte_offsets: *std.ArrayListUnmanaged(usize),
        row_data: []u8,
    ) !void {
        var huffman_row = std.ArrayListUnmanaged(u8){};
        huffman_row.ensureTotalCapacity(1 << 12);

        var literal_row = allocator.alloc(u8, self.literal_byte_size_sum);

        var literal_row_offset: usize = 0;
        for (self.literal_col_idxs.items) |col_idx| {
            const offset    = byte_offsets.items[col_idx];
            const num_bytes = self.literal_byte_sizes.items[col_idx];

            @memcpy(
                literal_row[literal_row_offset..][0..num_bytes], 
                row_data[offset..offset + num_bytes],
                );
            literal_row_offset += num_bytes;
        }

        for (self.huffman_col_idxs.items) |col_idx| {
            const offset = byte_offsets.items[col_idx];
            const next_offset = byte_offsets.items[col_idx + 1];

            try huffman_row.ensureUnusedCapacity(allocator, next_offset - offset);

            const current_buffer_idx = huffman_row.items.len;
            const compressed_size: u16 = @truncate(try self.huffman_compressor.compress(
                row_data[offset..next_offset],
                self.huffman_buffer.items[(current_buffer_idx + 2)..],
            ));

            self.huffman_buffer.items[current_buffer_idx]     = @truncate(compressed_size >> 8);
            self.huffman_buffer.items[current_buffer_idx + 1] = @truncate(compressed_size);
        }

        self.literal_rows.fromOwnedSlice(literal_row);
        self.huffman_rows.fromOwnedSlice(huffman_row.toOwnedSlice(allocator));
    }
};
