const std = @import("std");


pub const EFBlocked = struct{
    buffers: std.ArrayList(EFBuffer),
    allocator: std.mem.Allocator,
    block_size: usize,

    pub fn init(allocator: std.mem.Allocator, block_size: usize) EFBlocked {
        return EFBlocked{
            .allocator = allocator,
            .buffers = std.ArrayList(EFBuffer).init(allocator),
            .block_size = block_size,
        };
    }

    pub fn deinit(self: *EFBlocked) void {
        for (self.buffers.items) |*buf| {
            buf.deinit(self.allocator);
        }
        self.buffers.deinit();
    }

    fn compress(
        self: *EFBlocked,
        comptime T: type, 
        sorted_arr: []const T,
        ) !void {
        var done = false;
        var current_idx: usize = 0;

        while (!done) {
            const current_block_size = @min(self.block_size, sorted_arr.len - current_idx);
            if (current_block_size == 0) return;

            const slice = sorted_arr[current_idx..current_idx + current_block_size];
            done = (slice.len != self.block_size);

            var new_buffer = EFBuffer.init();
            try new_buffer.compress(self.allocator, T, slice);
            try self.buffers.append(new_buffer);

            current_idx += self.block_size;
        }
    }

    fn getByteSize(self: *const EFBlocked) usize {
        var total_size: usize = 0;
        for (self.buffers.items) |buf| {
            total_size += buf.high_bits.len + buf.low_bits.len;
        }
        return total_size;
    }
};

const EFBuffer = struct {
    low_bits: []u8,
    high_bits: []u8,
    num_low_bits: usize,

    pub fn init() EFBuffer {
        return EFBuffer{
            .low_bits = undefined,
            .high_bits = undefined,
            .num_low_bits = undefined,
        };
    }

    pub fn deinit(self: *EFBuffer, allocator: std.mem.Allocator) void {
        allocator.free(self.low_bits);
        allocator.free(self.high_bits);
    }

    fn compress(
        self: *EFBuffer,
        allocator: std.mem.Allocator,
        comptime T: type, 
        sorted_arr: []const T,
        ) !void {
        const num_elements = sorted_arr.len;
        const max_value    = sorted_arr[num_elements - 1];

        self.num_low_bits  = std.math.log2(max_value + 1 / num_elements);

        const max_possible_low_bits  = self.num_low_bits * num_elements;
        const max_possible_high_bits = 2 * num_elements;

        self.low_bits  = try allocator.alloc(
            u8, 
            try std.math.divCeil(usize, max_possible_low_bits, 8),
            );
        self.high_bits = try allocator.alloc(
            u8, 
            try std.math.divCeil(usize, max_possible_high_bits, 8),
            );

        const max_low_bytes = try std.math.divCeil(usize, self.num_low_bits, 8);

        @memset(self.low_bits,  0);
        @memset(self.high_bits, 0);

        const low_mask:  u64 = (@as(u64, 1) << @as(u6, @intCast(self.num_low_bits + 1))) - 1;

        var low_bit_idx:  usize = 0;
        var high_bit_idx: usize = 0;

        const low_shift_val: u6 = @as(u6, @intCast((64 - self.num_low_bits) % 64));

        for (sorted_arr) |value| {
            const shift_val: u6 = low_shift_val - @as(u6, @intCast(low_bit_idx % 8));

            const low_bits:  u64 = (value & low_mask) << shift_val;
            const high_bits: u64 = value >> @as(u6, @intCast(self.num_low_bits));

            const low_byte_idx:  usize = @divFloor(low_bit_idx, 8);

            low_bit_idx += self.num_low_bits;
            const bytes = @as([8]u8, @bitCast(low_bits));
            for (0..max_low_bytes) |idx| {
                const byte_idx = 7 - idx;
                self.low_bits[low_byte_idx + idx] |= bytes[byte_idx];
            }

            high_bit_idx += high_bits;
            const high_byte_idx: usize = @divFloor(high_bit_idx, 8);

            self.high_bits[high_byte_idx] |= @as(u8, 1) << @as(u3, @intCast(7 - (high_bit_idx % 8)));
            high_bit_idx += 1;
        }

        self.high_bits = try allocator.realloc(
            self.high_bits,
            try std.math.divCeil(usize, high_bit_idx, 8),
        );
    }
};



test "elias_fano" {
    std.debug.print("\n",  .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    const allocator = gpa.allocator();

    var arr = try allocator.alloc(usize, 1_000_000);
    for (0..arr.len) |idx| {
        arr[idx] = idx;
    }

    var ef_buffer = EFBlocked.init(allocator, 4096);
    defer ef_buffer.deinit();

    try ef_buffer.compress(usize, arr);

    std.debug.print("Num low bits:  {d}\n",  .{ef_buffer.buffers.items[0].num_low_bits});
    std.debug.print("Low_bits:  {b:0>8}\n",  .{ef_buffer.buffers.items[0].low_bits[0..]});
    std.debug.print("High_bits: {b:0>8}\n",  .{ef_buffer.buffers.items[0].high_bits[0..]});

    // Doesn't include overhead.
    std.debug.print(
        "Compression Ratio: {d}\n", 
        .{
            @as(f32, @floatFromInt(arr.len * @sizeOf(usize))) / 
            @as(f32, @floatFromInt(ef_buffer.getByteSize()))
        },
        );
}
