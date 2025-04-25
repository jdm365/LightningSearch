const std     = @import("std");
const builtin = @import("builtin");

const HuffmanCompressor = @import("../compression/huffman.zig").HuffmanCompressor;
const TermPos           = @import("../server/server.zig").TermPos;

pub const DocStore = struct {
    huffman_compressor: HuffmanCompressor,
    literal_byte_sizes: *const std.ArrayListUnmanaged(usize),
    literal_byte_size_sum: usize,
    literal_col_idxs: *const std.ArrayListUnmanaged(usize),
    huffman_col_idxs: *const std.ArrayListUnmanaged(usize),

    huffman_rows: std.ArrayListUnmanaged([]u8),
    literal_rows: std.ArrayListUnmanaged([]u8),

    huffman_field_sizes: std.ArrayListUnmanaged(u16),

    pub fn init(
        literal_byte_sizes: *const std.ArrayListUnmanaged(usize),
        literal_col_idxs: *const std.ArrayListUnmanaged(usize),
        huffman_col_idxs: *const std.ArrayListUnmanaged(usize),
    ) !DocStore {
        var sum: usize = 0;
        for (literal_byte_sizes.items) |byte_size| {
            sum += byte_size;
        }

        return DocStore {
            .huffman_compressor = HuffmanCompressor.init(),
            .literal_byte_sizes = literal_byte_sizes,
            .literal_byte_size_sum = sum,
            .literal_col_idxs = literal_col_idxs,
            .huffman_col_idxs = huffman_col_idxs,

            .huffman_rows = std.ArrayListUnmanaged([]u8){},
            .literal_rows = std.ArrayListUnmanaged([]u8){},

            .huffman_field_sizes = std.ArrayListUnmanaged(u16){},
        };
    }

    pub fn deinit(self: *DocStore, allocator: std.mem.Allocator) void {
        self.huffman_rows.deinit(allocator);
        self.literal_rows.deinit(allocator);
        self.huffman_field_sizes.deinit(allocator);
    }

    pub fn addRow(
        self: *DocStore,
        allocator: std.mem.Allocator,
        byte_positions: []TermPos,
        row_data: []u8,
    ) !void {
        // var huffman_row = std.ArrayListUnmanaged(u8){};
        var huffman_row = std.ArrayListUnmanaged(u32){};
        try huffman_row.ensureUnusedCapacity(allocator, 32);

        if (self.literal_byte_size_sum > 0) {
            var literal_row = try allocator.alloc(u8, self.literal_byte_size_sum);

            var literal_row_offset: usize = 0;
            for (self.literal_col_idxs.items) |col_idx| {
                const start_pos = byte_positions[col_idx].start_pos;
                const field_len = byte_positions[col_idx].field_len;

                @memcpy(
                    literal_row[literal_row_offset..][0..field_len], 
                    row_data[start_pos..(start_pos + field_len)],
                    );
                literal_row_offset += field_len;
            }
            try self.literal_rows.append(allocator, literal_row);
        }

        var row_size: usize = 0;
        for (self.huffman_col_idxs.items) |col_idx| {
            const start_pos = byte_positions[col_idx].start_pos;
            const field_len = byte_positions[col_idx].field_len;

            const start_pos_u32 = try std.math.divCeil(u32, start_pos, 4);
            const field_len_u32 = try std.math.divCeil(u32, field_len, 4);

            try huffman_row.ensureUnusedCapacity(allocator, field_len_u32);
            try huffman_row.resize(allocator, field_len_u32);

            const compressed_size: u16 = @truncate(try self.huffman_compressor.compressU32(
                row_data[start_pos..(start_pos + field_len)],
                huffman_row.items[buffer_size..],
            ));

            try self.huffman_field_sizes.append(allocator, compressed_size);
        }

        try self.huffman_rows.append(
            allocator, 
            (try huffman_row.toOwnedSlice(allocator))[0..huffman_row.items.len],
            );
    }
};
