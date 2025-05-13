const std = @import("std");


inline fn asU64(buffer: []const u8) u64 {
    // return std.mem.bytesAsValue(u64, &buffer[0..@min(8, buffer.len)]).*; 
    return std.mem.bytesAsValue(u64, buffer[0..8]); 
}

pub const EFBlocked = struct{
    buffers: std.ArrayList(EFBuffer),
    partition_mins: std.ArrayList(usize),
    allocator: std.mem.Allocator,
    block_size: usize,

    pub fn init(allocator: std.mem.Allocator, block_size: usize) EFBlocked {
        return EFBlocked{
            .allocator = allocator,
            .partition_mins = std.ArrayList(usize).init(allocator),
            .buffers = std.ArrayList(EFBuffer).init(allocator),
            .block_size = block_size,
        };
    }

    pub fn deinit(self: *EFBlocked) void {
        for (self.buffers.items) |*buf| {
            buf.deinit(self.allocator);
        }
        self.buffers.deinit();
        self.partition_mins.deinit();
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
            try self.partition_mins.append(slice[0]);

            var new_buffer = EFBuffer.init();
            try new_buffer.compress(self.allocator, T, slice);
            try self.buffers.append(new_buffer);

            current_idx += self.block_size;
        }
        try self.partition_mins.append(sorted_arr[sorted_arr.len - 1]);
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
    low_bits: []align(8) u8,
    high_bits: []align(8) u8,
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
        std.debug.assert(sorted_arr.len > 1);

        const num_elements = sorted_arr.len;
        const max_value    = sorted_arr[num_elements - 1] - sorted_arr[0];

        self.num_low_bits  = std.math.log2((max_value + 1) / num_elements);

        const max_possible_low_bits  = self.num_low_bits * num_elements;
        const max_possible_high_bits = ((3 + self.num_low_bits) * num_elements) - max_possible_low_bits;

        self.low_bits  = try allocator.alignedAlloc(
            u8, 
            8,
            try std.math.divCeil(usize, max_possible_low_bits, 8),
            );
        self.high_bits = try allocator.alignedAlloc(
            u8, 
            8,
            try std.math.divCeil(usize, max_possible_high_bits, 8),
            );

        const max_low_bytes = switch (self.num_low_bits) {
            0...1 => 1,
            else => 2 + @divFloor(self.num_low_bits - 2, 8),
        };

        @memset(self.low_bits,  0);
        @memset(self.high_bits, 0);
        self.high_bits[0] = 0b10000000;

        const low_mask:  u64 = (@as(u64, 1) << @as(u6, @intCast(self.num_low_bits))) - 1;

        var low_bit_idx:  usize = self.num_low_bits;
        var high_bit_idx: usize = 1;

        const low_shift_val: u6 = @as(u6, @intCast((64 - self.num_low_bits) % 64));

        var prev_value = sorted_arr[0];
        for (sorted_arr[1..]) |_value| {
            std.debug.assert(_value >= prev_value);

            const value = _value - prev_value;

            const shift_val: u6 = low_shift_val - @as(u6, @intCast(low_bit_idx % 8));

            const low_bits:  u64 = (value & low_mask) << shift_val;
            const high_bits: u64 = value >> @as(u6, @intCast(self.num_low_bits));

            const low_byte_idx:  usize = @divFloor(low_bit_idx, 8);

            low_bit_idx += self.num_low_bits;
            const bytes = @as([8]u8, @bitCast(low_bits));

            const bytes_remaining = @min(self.low_bits.len - low_byte_idx, max_low_bytes);
            for (0..bytes_remaining) |idx| {
                const byte_idx = 7 - idx;
                self.low_bits[low_byte_idx + idx] |= bytes[byte_idx];
            }

            high_bit_idx += high_bits + 1;
            const high_byte_idx: usize = @divFloor(high_bit_idx, 8);
            self.high_bits[high_byte_idx] |= @as(u8, 1) << @as(u3, @intCast(7 - (high_bit_idx % 8)));

            prev_value = _value;
        }

        self.high_bits = try allocator.realloc(
            self.high_bits,
            try std.math.divCeil(usize, high_bit_idx, 8),
        );
    }

    pub const Iterator = struct {
        buffer: *EFBuffer,
        low_bit_idx: usize,
        high_bit_idx: usize,
        prev_value: usize,

        pub fn next(self: *Iterator, T: type) ?T {
            // if (low_byte_idx >= self.buffer.low_bits.len) return null;
            if (self.high_bit_idx >= self.buffer.high_bits.len * 8) return null;
            if (self.high_bit_idx == 0) {
                self.low_bit_idx  += self.buffer.num_low_bits;
                self.high_bit_idx += 2;
                return self.prev_value;
            }

            var low_byte = switch (self.buffer.num_low_bits) {
                0 => 0,
                else => blk: {
                    const low_byte_idx: usize = @divFloor(self.low_bit_idx, 8);
                    const low_bit_idx = self.low_bit_idx % 8;
                    const shift_len: u6 = @as(u6, @intCast((64 - self.buffer.num_low_bits) % 64 - low_bit_idx));

                    break :blk @as(u64, @intFromPtr(&self.buffer.low_bits[low_byte_idx..low_byte_idx+8])) >> shift_len;
                },
            };

            const high_byte_idx: usize = @divFloor(self.high_bit_idx, 8);

            // TODO: Change so endian swap isn't necessary.
            const u64_high_byte_idx = @divFloor(high_byte_idx, 8);
            const u64_high_bit_idx  = @as(u6, @intCast(self.high_bit_idx % 64));
            const u64_mask = ~@as(u64, 0) >> u64_high_bit_idx;
            const u64_slice = std.mem.bytesAsSlice(u64, self.buffer.high_bits);
            const u64_high_val = std.mem.nativeToBig(u64, u64_slice[u64_high_byte_idx]) & u64_mask;

            const high_result = switch (u64_high_val) {
                0 => blk: {
                    if (u64_high_byte_idx + 1 == @divFloor(self.buffer.high_bits.len, 8)) return null;

                    const next_byte = std.mem.nativeToBig(u64, u64_slice[u64_high_byte_idx + 1]);
                    break :blk @clz(next_byte) + (64 - @as(u64, @intCast(u64_high_bit_idx)));
                },
                else => @clz(u64_high_val) - u64_high_bit_idx,
            };

            low_byte |= @as(u64, @intCast(high_result)) << @as(u6, @intCast(self.buffer.num_low_bits));
            const result = @as(T, @intCast(low_byte)) + self.prev_value;
            self.prev_value = result;

            self.low_bit_idx  += self.buffer.num_low_bits;
            self.high_bit_idx += high_result + 1;

            return result;
        }
    };

    pub fn iterator(self: *EFBuffer, comptime T: type, min_value: T) Iterator {
        return Iterator{
            .buffer = self,
            .low_bit_idx = 0,
            .high_bit_idx = 0,
            .prev_value = min_value,
        };
    }
};



test "elias_fano" {
    std.debug.print("\n",  .{});

    var gpa = std.heap.DebugAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }
    const allocator = gpa.allocator();

    var arr = try allocator.alloc(usize, 1_000_000);
    defer allocator.free(arr);
    for (0..arr.len) |idx| {
        arr[idx] = idx * 9;
    }

    var ef_buffer = EFBlocked.init(allocator, 4096);
    defer ef_buffer.deinit();

    
    const start_time = std.time.microTimestamp();
    try ef_buffer.compress(usize, arr);
    const end_time = std.time.microTimestamp();

    const elapsed_time = end_time - start_time;
    const mb_s = @as(f32, @floatFromInt(arr.len * @sizeOf(usize))) / @as(f32, @floatFromInt(elapsed_time));
    std.debug.print("Throughput:   {d}MB/s\n",  .{mb_s});

    std.debug.print("Num low bits:  {d}\n",  .{ef_buffer.buffers.items[0].num_low_bits});
    // std.debug.print("Low_bits:  {b:0>8}\n",  .{ef_buffer.buffers.items[0].low_bits[0..]});
    // std.debug.print("High_bits: {b:0>8}\n",  .{ef_buffer.buffers.items[1].high_bits[0..]});

    // Doesn't include overhead.
    std.debug.print(
        "Compression Ratio: {d}\n", 
        .{
            @as(f32, @floatFromInt(arr.len * @sizeOf(usize))) / 
            @as(f32, @floatFromInt(ef_buffer.getByteSize()))
        },
        );
    std.debug.print("Num bytes final: {d}\n", .{ef_buffer.getByteSize()});


    // Test iterator
    var prev_value: usize = ef_buffer.partition_mins.items[0];
    std.debug.print("prev_value: {d}\n", .{prev_value});
    var iter = ef_buffer.buffers.items[0].iterator(usize, prev_value);
    while (iter.next(usize)) |value| {
        std.debug.assert(value >= prev_value);
        prev_value = value;
        std.debug.print("{d} ", .{value});
    }
}
