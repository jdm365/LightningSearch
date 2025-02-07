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

// TODO: Lookup table. 8x32 comptime generataed. SIMD shuffle_mask @cntz
pub fn LOOKUP_TABLE(comptime ranges: []const Range) struct{table: [8][32]u8, set_table_ranges: u8} {
    var table: [8][32]u8 = undefined;
    for (0..8) |idx| {
        @memset(&table[idx], 0);
    }
    var set_table_ranges: u8 = 0;
    for (ranges) |range| {
        for (range.low..range.high + 1) |c| {
            const table_idx = c / 32;
            const byte_idx  = c % 32;
            table[table_idx][byte_idx] = 1;
            set_table_ranges |= 1 << table_idx;
        }
    }
    return .{table, set_table_ranges};
}

pub fn SCALAR_LOOKUP_TABLE(comptime ranges: []const Range) [256]u8 {
    var table: [256]u8 = undefined;
    @memset(&table, 0);

    for (ranges) |range| {
        for (range.low..range.high + 1) |c| {
            table[c] = 1;
        }
    }
    return table;
}

pub inline fn simdFindCharInRanges(
    buffer: []const u8,
    comptime ranges: []const Range,
) usize {
    var remaining = buffer;
    var total_offset: usize = 0;

    const table_struct = LOOKUP_TABLE(ranges);
    const table = table_struct.table;
    // const set_table_ranges = table_struct.set_table_ranges;

    // NOT DONE.
    while (remaining.len >= VEC_SIZE) {
        const vec_buffer = @as(*align(1) const VEC, @alignCast(@ptrCast(remaining[0..VEC_SIZE])));
        var range_mask: MASK_TYPE = 0;

        inline for (0..8) |idx| {
            range_mask |= @bitCast(vec_buffer.* & comptime @as(VEC, @splat(table[idx][0])) != 0);
        }

        const index = @ctz(range_mask);
        if (index != VEC_SIZE) {
            return total_offset + index;
        }

        remaining = remaining[VEC_SIZE..];
        total_offset += VEC_SIZE;
    }

    // Process any remaining bytes.
    var idx: usize = 0;
    while (idx < remaining.len) {
        if (table[buffer[idx] / 32][buffer[idx] % 32] != 0) {
            return total_offset + idx;
        }
        idx += 1;
    }

    return buffer.len;
}

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
    escape_first: bool,
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
    var char_mask:     MASK_TYPE = @bitCast(vec_buffer.* == _char_mask);
    const escape_mask: MASK_TYPE = @bitCast(vec_buffer.* == _escape_mask);

    const effective_escape_mask = escape_mask & ~(escape_mask << 1);

    // TODO: Handle escaped escapes.
    char_mask &= ~(effective_escape_mask << 1);
    char_mask &= ~@as(MASK_TYPE, @intFromBool(escape_first));

    return @ctz(char_mask);
}

// 0110
// 0011
// 1100
// 0100
//
// 0010
// 0001
// 1110
// 0010
//
// 0010
// 0100
// 0010
// 1101
// 0000

pub inline fn simdFindCharIdxEscapedFull(
    buffer: []const u8,
    comptime char: u8,
) usize {
    var remaining = buffer;
    var total_offset: usize = 0;
    
    const base_mask: MASK_TYPE = comptime 1 << (@bitSizeOf(MASK_TYPE) - 1);
    var escape_next_first_mask: MASK_TYPE = comptime std.math.maxInt(MASK_TYPE);

    while (remaining.len >= VEC_SIZE) {
        const _char_mask:   VEC = @splat(char);
        const _escape_mask: VEC = @splat('\\');

        const vec_buffer = @as(*align(1) const VEC, @alignCast(@ptrCast(remaining[0..VEC_SIZE])));
        var char_mask:     MASK_TYPE = @bitCast(vec_buffer.* == _char_mask);
        const escape_mask: MASK_TYPE = @bitCast(vec_buffer.* == _escape_mask);

        const effective_escape_mask = escape_mask & ~(escape_mask << 1);

        char_mask &= ~(effective_escape_mask << 1);
        char_mask &= escape_next_first_mask;

        escape_next_first_mask = ~@as(MASK_TYPE, @popCount(base_mask & effective_escape_mask));
        const index = @ctz(char_mask);
        if (index != VEC_SIZE) {
            return total_offset + index;
        }

        remaining = remaining[VEC_SIZE..];
        total_offset += VEC_SIZE;
    }

    // Handle remaining bytes
    var idx: usize = 0;
    while (idx < remaining.len) {
        if (remaining[idx] == '\\') {
            idx += 2;
            continue;
        }
        if (remaining[idx] == char) return total_offset + idx;
        idx += 1;
    }
    return buffer.len;
}


const Range = struct {
    low: u8,
    high: u8,
};

pub const SEP_RANGES: [6]Range = .{
    .{ .low = 0, .high = 9 },
    .{ .low = 11, .high = 43 },
    .{ .low = 45, .high = 47 },
    .{ .low = 58, .high = 64 },
    .{ .low = 91, .high = 96 },
    .{ .low = 123, .high = 126 },
};

pub inline fn simdFindCharInRangesEscapedFull(
    buffer: []const u8,
    comptime ranges: []const Range,
) usize {
    // Not actually SIMD for now.
    for (0..buffer.len) |idx| {
        if (SCALAR_LOOKUP_TABLE(ranges)[buffer[idx]] == 1) {
            return idx;
        }
    }
    return buffer.len;
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



test "escape_test" {
    const buf = "Star Trek\\\" - The T.V. Theme\\\"\",\"length\":\"209000\",\"artist\":\"The Fe";

    const index = simdFindCharIdxEscaped(buf, '"', false);
    std.debug.print("\n\nbuffer: ", .{});
    for (buf) |c| std.debug.print("{c}", .{c});
    std.debug.print("\nIndex:  {d}\n", .{index});
    std.debug.print("\nBUF:  {s}\n", .{buf[index..]});


    const buf2 = "001-Atto quarto: No. 24 Cavatina \\\"L'ho perduta... me meschina!\\\"\",\"length\":\"1m 50sec\",\"artist\":\"Wolfgang Amadeus Mozart\",\"album";

    const index2 = simdFindCharIdxEscapedFull(buf2, '"');
    std.debug.print("\n\nbuffer: ", .{});
    for (buf2) |c| std.debug.print("{c}", .{c});
    std.debug.print("\nIndex2:  {d}\n", .{index2});
    std.debug.print("\nBUF:  {s}\n", .{buf2[index2..]});


    const buf3 = "Surreal (Thunderpuss dub mix) (excerpts from ayu-mi-x Ⅲ CD004)\",\"length\":\"09:07\",\"artist\":\"浜崎あゆみ\",\"album\":\"excerpts";
    const index3 = simdFindCharIdxEscapedFull(buf3, '"');
    // const index3 = simdFindCharIdxEscaped(buf3, '"', false);
    std.debug.print("\n\nbuffer: ", .{});
    for (buf3) |c| std.debug.print("{c}", .{c});
    std.debug.print("\nIndex3:  {d}\n", .{index3});
    std.debug.print("\nBUF:  {s}\n", .{buf3[index3..]});
}
