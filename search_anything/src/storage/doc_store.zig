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

    arena: std.heap.ArenaAllocator,

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

            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    // pub fn deinit(self: *DocStore, allocator: std.mem.Allocator) void {
    pub fn deinit(self: *DocStore) void {
        // self.huffman_rows.deinit(allocator);
        // self.literal_rows.deinit(allocator);
        // self.huffman_field_sizes.deinit(allocator);
        self.arena.deinit();
    }

    pub fn printMemoryUsage(self: *const DocStore) void {
        var total_bytes: usize = 0;
        var huffman_rows_sum: usize = 0;
        var literal_rows_sum: usize = 0;
        var huffman_field_sizes_sum: usize = 0;

        for (self.huffman_rows.items) |row| {
            huffman_rows_sum += row.len;
        }
        for (self.literal_rows.items) |row| {
            literal_rows_sum += row.len;
        }
        huffman_field_sizes_sum += 2 * self.huffman_field_sizes.items.len;
        total_bytes += huffman_rows_sum + total_bytes + huffman_field_sizes_sum;

        if (huffman_rows_sum >= (1 << 20)) {
            std.debug.print("HUFFMAN ROWS SIZE:  {d}MB\n", .{@divFloor(huffman_rows_sum, 1 << 20)});
            std.debug.print("LITERAL ROWS SIZE:  {d}MB\n", .{@divFloor(literal_rows_sum, 1 << 20)});
            std.debug.print("HUFFMAN FIELD SIZE: {d}MB\n", .{@divFloor(huffman_field_sizes_sum, 1 << 20)});
            std.debug.print("-------------------------\n", .{});
            std.debug.print("TOTAL SIZE:         {d}MB\n", .{@divFloor(total_bytes, 1 << 20)});
        } else {
            std.debug.print("HUFFMAN ROWS SIZE:  {d}kB\n", .{@divFloor(huffman_rows_sum, 1 << 10)});
            std.debug.print("LITERAL ROWS SIZE:  {d}kB\n", .{@divFloor(literal_rows_sum, 1 << 10)});
            std.debug.print("HUFFMAN FIELD SIZE: {d}kB\n", .{@divFloor(huffman_field_sizes_sum, 1 << 10)});
            std.debug.print("-------------------------\n", .{});
            std.debug.print("TOTAL SIZE:         {d}kB\n", .{@divFloor(total_bytes, 1 << 10)});
        }

    }


    pub fn addRow(
        self: *DocStore,
        // allocator: std.mem.Allocator,
        byte_positions: []TermPos,
        row_data: []u8,
    ) !void {
        // TODO: Make member to retain allocation.
        var huffman_row = std.ArrayListUnmanaged(u8){};
        try huffman_row.ensureUnusedCapacity(self.arena.allocator(), 128);

        if (self.literal_byte_size_sum > 0) {
            var literal_row = try self.arena.allocator().alloc(u8, self.literal_byte_size_sum);

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
            const val = try self.literal_rows.addOne(self.arena.allocator());
            val.* = literal_row[0..literal_row_offset];
        }

        var row_size: usize = 0;
        for (self.huffman_col_idxs.items) |col_idx| {
            const start_pos = byte_positions[col_idx].start_pos;
            const field_len = byte_positions[col_idx].field_len;

            try huffman_row.ensureUnusedCapacity(self.arena.allocator(), field_len);
            try huffman_row.resize(self.arena.allocator(), 2 * (row_size + field_len + 8));

            const compressed_size: u16 = @truncate(
                try self.huffman_compressor.compress(
                    row_data[start_pos..(start_pos + field_len)],
                    huffman_row.items[row_size..],
                    )
                );
            try self.huffman_field_sizes.append(self.arena.allocator(), compressed_size);
            row_size += compressed_size;
        }
        try huffman_row.resize(self.arena.allocator(), row_size);

        const val = try self.huffman_rows.addOne(self.arena.allocator());
        val.* = (try huffman_row.toOwnedSlice(self.arena.allocator()))[0..row_size];
    }
};
