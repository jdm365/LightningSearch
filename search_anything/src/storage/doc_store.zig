const std     = @import("std");
const builtin = @import("builtin");

const HuffmanCompressor = @import("../compression/huffman.zig").HuffmanCompressor;
const TermPos           = @import("../server/server.zig").TermPos;

pub const DocStore = struct {
    huffman_compressor: HuffmanCompressor,
    literal_byte_sizes: std.ArrayListUnmanaged(usize),
    literal_byte_size_sum: usize,
    literal_col_idxs: std.ArrayListUnmanaged(usize),
    huffman_col_idxs: std.ArrayListUnmanaged(usize),

    huffman_rows: std.ArrayListUnmanaged([]u8),
    literal_rows: std.ArrayListUnmanaged([]u8),

    huffman_field_sizes: std.ArrayListUnmanaged(u16),
    huffman_field_rem_bits: std.ArrayListUnmanaged(u3),

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

        var store = DocStore {
            .huffman_compressor = HuffmanCompressor.init(),
            .literal_byte_sizes = std.ArrayListUnmanaged(usize){},
            .literal_byte_size_sum = sum,
            .literal_col_idxs = std.ArrayListUnmanaged(usize){},
            .huffman_col_idxs = std.ArrayListUnmanaged(usize){},

            .huffman_rows = std.ArrayListUnmanaged([]u8){},
            .literal_rows = std.ArrayListUnmanaged([]u8){},

            .huffman_field_sizes = std.ArrayListUnmanaged(u16){},
            .huffman_field_rem_bits = std.ArrayListUnmanaged(u3){},

            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
        store.literal_byte_sizes = try literal_byte_sizes.clone(store.arena.allocator());
        store.literal_col_idxs   = try literal_col_idxs.clone(store.arena.allocator());
        store.huffman_col_idxs   = try huffman_col_idxs.clone(store.arena.allocator());

        return store;
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
        byte_positions: []TermPos,
        row_data: []u8,
    ) !void {
        // TODO: Make member to retain allocation.
        var huffman_row = std.ArrayListUnmanaged(u8){};
        try huffman_row.ensureUnusedCapacity(self.arena.allocator(), 128);

        if (self.literal_byte_size_sum > 0) {
            var literal_row = try self.arena.allocator().alloc(
                u8, 
                self.literal_byte_size_sum,
                );

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

        try huffman_row.resize(
            self.arena.allocator(), 
            2 * (
                byte_positions[byte_positions.len - 1].start_pos + 
                byte_positions[byte_positions.len - 1].field_len + 
                8
                ),
            );

        var row_size: usize = 0;
        for (self.huffman_col_idxs.items) |col_idx| {
            const start_pos = byte_positions[col_idx].start_pos;
            const field_len = byte_positions[col_idx].field_len;

            if (field_len == 0) {
                try self.huffman_field_sizes.append(
                    self.arena.allocator(), 
                    0,
                    );
                try self.huffman_field_rem_bits.append(
                    self.arena.allocator(), 
                    0,
                    );
                continue;
            }

            const compressed_bit_size = try self.huffman_compressor.compress(
                    row_data[start_pos..][0..field_len],
                    huffman_row.items[row_size..],
                    );
            const compressed_byte_size: u16 = @truncate(
                try std.math.divCeil(u64, compressed_bit_size, 8)
                );
            const rem_bits: u3 = @truncate(compressed_bit_size % 8);
            std.debug.assert(compressed_byte_size > 0);

            try self.huffman_field_sizes.append(
                self.arena.allocator(), 
                compressed_byte_size,
                );
            try self.huffman_field_rem_bits.append(
                self.arena.allocator(), 
                rem_bits,
                );
            row_size += compressed_byte_size;
        }
        try huffman_row.resize(self.arena.allocator(), row_size);

        const val = try self.huffman_rows.addOne(self.arena.allocator());
        val.* = (try huffman_row.toOwnedSlice(self.arena.allocator()))[0..row_size];
    }

    pub inline fn getRow(
        self: *DocStore,
        row_idx: usize,
        row_data: []u8,
        offsets: []TermPos,
    ) !void {
        // Assume all huffman for now.
        std.debug.assert(self.literal_col_idxs.items.len == 0);

        const huffman_row = self.huffman_rows.items[row_idx];

        const huffman_idx_offset  = self.huffman_col_idxs.items.len * row_idx;

        const huffman_field_sizes = self.huffman_field_sizes.items[
            huffman_idx_offset..
        ][0..self.huffman_col_idxs.items.len];
        const huffman_field_rem_bits = self.huffman_field_rem_bits.items[
            huffman_idx_offset..
        ][0..self.huffman_col_idxs.items.len];

        var input_pos:  usize = 0;
        var output_pos: usize = 0;
        for (0..huffman_field_sizes.len) |idx| {
            offsets[idx].start_pos = @truncate(output_pos);

            const compressed_len = huffman_field_sizes[idx];
            if (compressed_len == 0) {
                offsets[idx].field_len = 0;
                continue;
            }

            const rem_bits = huffman_field_rem_bits[idx];
            offsets[idx].field_len = @truncate(
                try self.huffman_compressor.decompress(
                    huffman_row[input_pos..][0..compressed_len],
                    row_data[output_pos..],
                    rem_bits,
                )
            );

            input_pos  += compressed_len;
            output_pos += offsets[idx].field_len;
        }
    }
};
