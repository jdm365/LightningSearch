const std = @import("std");

// Keep small. Don't feel like dealing atm.
const TABLE_TYPE = u1;
// const TABLE_TYPE = u16;
const PAIR_LOOKUP_TABLE = blk: {
    const max_val = std.math.maxInt(TABLE_TYPE);
    @setEvalBranchQuota(max_val * 2 * @bitSizeOf(TABLE_TYPE));

    var table: [max_val]TABLE_TYPE = undefined;
    var bit_idx: usize = 0;

    for (0..max_val) |idx| {
        bit_idx = 0;
        var value = idx;
        var other: TABLE_TYPE = 3;

        while (bit_idx < @bitSizeOf(TABLE_TYPE)) {
            if (@popCount(other & value) == 2) {
                value &= ~other;
                bit_idx += 1;
                other <<= 1;
            }
            bit_idx += 1;
            other <<= 1;
        }
        table[idx] = value;
    }
    break :blk table;
};

// Unrolled once max SIMD lane. Fast than not unrolling in practice.
pub const VEC_SIZE = 2 * (std.simd.suggestVectorLength(u8) orelse 64);
const MASK_TYPE = switch (VEC_SIZE) {
    16 => u16,
    32 => u32,
    64 => u64,
    128 => u128,
    else => unreachable,
};
pub const VEC = @Vector(VEC_SIZE, u8);
pub const VEC128 = @Vector(16, u8);

pub inline fn simdFindCharIdx(
    buffer: []const u8,
    comptime char: u8,
) usize {
    if (buffer.len < VEC_SIZE) {
        for (0.., buffer) |idx, byte| {
            if (byte == char) return idx;
        }
        return buffer.len;
    }

    const char_mask: VEC = comptime @splat(char);

    const vec_buffer = @as(*align(1) const VEC, @alignCast(@ptrCast(buffer[0..VEC_SIZE])));
    const mask: MASK_TYPE = @bitCast(vec_buffer.* == char_mask);

    // REMOVE DOUBLED QUOTES:
    // variants have only decreased performance.
    // In practice (at least for files I've tested) quotes are 
    // rare enough that adding the special case to every loop 
    // instead of the occasional handling in the ugly switch statement
    // isn't worth it.
    return @ctz(mask);
}

pub inline fn simdFindCharIdxEscaped(
    buffer: []const u8,
    comptime char: u8,
) usize {
    if (buffer.len < VEC_SIZE) {
        var idx: usize = 0;
        while (idx < buffer.len) {
            if (buffer[idx] == '\\') {
                idx += 2;
                continue;
            }
            if (buffer[idx] == char) return idx;
            idx += 1;
        }
        return buffer.len;
    }

    const _char_mask:   VEC = comptime @splat(char);
    const _escape_mask: VEC = comptime @splat('\\');

    const vec_buffer = @as(*align(1) const VEC, @alignCast(@ptrCast(buffer[0..VEC_SIZE])));
    var char_mask:   MASK_TYPE = @bitCast(vec_buffer.* == _char_mask);
    const escape_mask: MASK_TYPE = @bitCast(vec_buffer.* == _escape_mask);

    // TODO: Handle escaped escapes.
    char_mask &= ~(escape_mask >> 1);

    return @ctz(char_mask);
}

pub inline fn simdFindCharIdxMasked(
    buffer: []const u8,
    comptime char: u8,
    mask_idx: usize,
) usize {
    // Same as above but operates on aligned byte boundaries and uses
    // a mask_idx to deal with unknown chars.
    if (buffer.len < VEC_SIZE) {
        for (0.., buffer) |idx, byte| {
            if (byte == char) return idx;
        }
        return buffer.len;
    }

    const char_mask: VEC = comptime @splat(char);

    const vec_buffer = @as(*const VEC, @ptrCast(buffer[0..VEC_SIZE]));
    const mask: MASK_TYPE = @as(MASK_TYPE, @bitCast(vec_buffer.* == char_mask)) & ~(std.math.maxInt(MASK_TYPE) << mask_idx);

    // REMOVE DOUBLED QUOTES:
    // variants have only decreased performance.
    // In practice (at least for files I've tested) quotes are 
    // rare enough that adding the special case to every loop 
    // instead of the occasional handling in the ugly switch statement
    // isn't worth it.
    return @ctz(mask);
}

inline fn removeDoubled(comptime T: type, x: *T) void {
    comptime {
        std.debug.assert((T == u64) or (T == u32) or (T == u16));
    }
    const parallelism = comptime @divFloor(@bitSizeOf(T), @bitSizeOf(TABLE_TYPE));
    const x_interp = @as([*]TABLE_TYPE, @ptrCast(@alignCast(x)))[0..parallelism];

    inline for (0..parallelism) |idx| {
        x_interp[idx] &= PAIR_LOOKUP_TABLE[x_interp[idx]];
    }
}


pub inline fn stringToUpper(str: [*]u8, len: usize) void {
    var index: usize = 0;

    while (index + VEC_SIZE <= len) {
        const input = @as(*align(1) VEC, @ptrCast(@alignCast(str[index..index+VEC_SIZE])));

        const ascii_a: VEC = comptime @splat('a');
        const ascii_z: VEC = comptime @splat('z');
        const case_diff: VEC = comptime @splat('a' - 'A');

        const greater_than_a = input.* >= ascii_a;
        const less_equal_z   = input.* <= ascii_z;
        const to_sub = (@intFromBool(greater_than_a) * @intFromBool(less_equal_z)) * case_diff;
        // Doing this without underflow wrapping causes issues on ARM
        // even though underflows never happen.
        input.* -%= to_sub;

        index += VEC_SIZE;
    }

    for (index..len) |idx| {
        str[idx] = std.ascii.toUpper(str[idx]);
    }
}

pub inline fn iterUTF8(read_buffer: []const u8, read_idx: *usize) u8 {
    // Return final byte.

    const byte: u8 = read_buffer[read_idx.*];
    const size: usize = @as(usize, @intFromBool(byte > 127)) 
                        + @as(usize, @intFromBool(byte > 223)) 
                        + @as(usize, @intFromBool(byte > 239));
    read_idx.* += size + @intFromBool(size == 0);

    return read_buffer[read_idx.* - 1];
}

pub inline fn readUTF8(
    read_buffer: []const u8,
    write_buffer: []u8,
    read_idx: *usize,
    write_idx: *usize,
    uppercase: bool,
) u8 {
    // Return final byte.
    // TODO: Add support multilingual delimiters.

    const byte: u8 = read_buffer[read_idx.*];
    const size: usize = @as(usize, @intFromBool(byte > 127)) 
                        + @as(usize, @intFromBool(byte > 223)) 
                        + @as(usize, @intFromBool(byte > 239));
    @memcpy(
        write_buffer[write_idx.*..write_idx.* + size + @intFromBool(size == 0)],
        read_buffer[read_idx.*..read_idx.* + size + @intFromBool(size == 0)],
    );
    read_idx.* += size + @intFromBool(size == 0);
    write_idx.* += size + @intFromBool(size == 0);

    if (uppercase) {
        write_buffer[write_idx.* - 1] = std.ascii.toUpper(write_buffer[write_idx.* - 1]);
    }

    return write_buffer[write_idx.* - 1];
}
