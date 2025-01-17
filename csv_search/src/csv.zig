const std = @import("std");
const TermPos = @import("server.zig").TermPos;

pub const TOKEN_STREAM_CAPACITY = 1_048_576;


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

pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

// Unrolled once max SIMD lane. Fast than not unrolling in practice.
const VEC_SIZE = 2 * (std.simd.suggestVectorLength(u8) orelse 64);
const MASK_TYPE = switch (VEC_SIZE) {
    16 => u16,
    32 => u32,
    64 => u64,
    128 => u128,
    else => unreachable,
};
const VEC = @Vector(VEC_SIZE, u8);
const VEC128 = @Vector(16, u8);

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
        input.* -= to_sub;

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

pub inline fn _iterFieldCSV(buffer: []const u8, byte_idx: *usize) void {
    // Iterate to next field in compliance with RFC 4180.
    const is_quoted = buffer[byte_idx.*] == '"';
    byte_idx.* += @intFromBool(is_quoted);

    outer_loop: while (true) {
        if (is_quoted) {
            const skip_idx = simdFindCharIdx(buffer[byte_idx.*..], '"');
            byte_idx.* += skip_idx;
            if (skip_idx == VEC_SIZE) continue;
            
            byte_idx.* += 1;

            // Check if escape quote.
            if (buffer[byte_idx.*] == '"') {
                byte_idx.* += 1;
                continue;
            }
            std.debug.assert((buffer[byte_idx.*] == ',') or (buffer[byte_idx.*] == '\n'));
            byte_idx.* += 1;
            break :outer_loop;

        } else {
            const newline_idx = simdFindCharIdx(buffer[byte_idx.*..], '\n');
            const comma_idx   = simdFindCharIdx(buffer[byte_idx.*..], ',');

            const skip_idx = @min(newline_idx, comma_idx);
            byte_idx.* += skip_idx;
            if (skip_idx == VEC_SIZE) continue;
            std.debug.assert((buffer[byte_idx.*] == ',') or (buffer[byte_idx.*] == '\n'));
            byte_idx.* += 1;
            break :outer_loop;
        }
    }
}

pub inline fn iterLineCSV(buffer: []const u8, byte_idx: *usize) !void {
    // Iterate to next line in compliance with RFC 4180.

    var skip_idx: usize = 0;
    var is_newline: bool = false;
    var quote_idx: usize = 0;
    var newline_idx: usize = 0;

    while (true) {
        quote_idx   = simdFindCharIdx(buffer[byte_idx.*..], '"');
        newline_idx = simdFindCharIdx(buffer[byte_idx.*..], '\n');

        if (quote_idx < newline_idx) {
            skip_idx = quote_idx;
            is_newline = false;
        } else {
            skip_idx = newline_idx;
            is_newline = true;
        }
        byte_idx.* += skip_idx;
        if (skip_idx == VEC_SIZE) continue;

        byte_idx.* += 1;
        if (!is_newline) {

            while (true) {
                quote_idx = simdFindCharIdx(buffer[byte_idx.*..], '"');
                byte_idx.* += quote_idx;
                if (quote_idx == VEC_SIZE) continue;

                byte_idx.* += 1;
                if (buffer[byte_idx.*] == '"') {
                    byte_idx.* += 1;
                    continue;
                }
                std.debug.assert((buffer[byte_idx.*] == ',') or (buffer[byte_idx.*] == '\n'));
                break;
            }
            continue;
        }
        return;
    }
}

pub inline fn parseRecordCSV(
    buffer: []const u8,
    result_positions: []TermPos,
) !void {
    // Parse CSV record in compliance with RFC 4180.
    var byte_idx: usize = 0;
    for (0..result_positions.len) |idx| {
        const start_pos = byte_idx;
        _iterFieldCSV(buffer, &byte_idx);
        result_positions[idx] = TermPos{
            .start_pos = @as(u32, @intCast(start_pos)) + @intFromBool(buffer[start_pos] == '"'),
            .field_len = @as(u32, @intCast(byte_idx - start_pos - 1)) - @intFromBool(buffer[start_pos] == '"'),
        };
    }
}

pub const TokenStream = struct {
    tokens: [][]token_t,
    f_data: []align(std.mem.page_size) u8,
    num_terms: []u32,
    allocator: std.mem.Allocator,
    output_files: []std.fs.File,
    input_file: std.fs.File,
    buffer_idx: usize,

    pub fn init(
        filename: []const u8,
        output_filename: []const u8,
        allocator: std.mem.Allocator,
        num_search_cols: usize,
    ) !TokenStream {

        const input_file = try std.fs.cwd().openFile(filename, .{});
        // const file_size = try file.getEndPos();

        var output_files = try allocator.alloc(std.fs.File, num_search_cols);
        for (0..num_search_cols) |idx| {
            const _output_filename = try std.fmt.allocPrint(
                allocator,
                "{s}_{d}.bin",
                .{output_filename, idx},
            );
            defer allocator.free(_output_filename);

            output_files[idx] = try std.fs.cwd().createFile(
                _output_filename, 
                .{ .read = true },
                );
        }

        const num_terms = try allocator.alloc(u32, num_search_cols);
        @memset(num_terms, 0);

        const token_buffers = try allocator.alloc([]token_t, num_search_cols);
        for (0..token_buffers.len) |idx| {
            token_buffers[idx] = try allocator.alloc(token_t, TOKEN_STREAM_CAPACITY);
        }

        const token_stream = TokenStream{
            .tokens = token_buffers,
            .f_data = try allocator.alignedAlloc(u8, std.mem.page_size, TOKEN_STREAM_CAPACITY),
            .num_terms = num_terms,
            .allocator = allocator,
            .output_files = output_files,
            .input_file = input_file,
            .buffer_idx = 0,
        };

        return token_stream;
    }

    pub fn deinit(self: *TokenStream) void {
        self.allocator.free(self.f_data);
        for (0.., self.output_files) |col_idx, *file| {
            self.allocator.free(self.tokens[col_idx]);
            file.close();
        }
        self.allocator.free(self.output_files);
        self.allocator.free(self.num_terms);
        self.allocator.free(self.tokens);
    }
    
    pub fn addToken(
        self: *TokenStream,
        new_doc: bool,
        term_pos: u8,
        doc_id: u32,
        search_col_idx: usize,
    ) !void {
        self.tokens[search_col_idx][self.num_terms[search_col_idx]] = token_t{
            .new_doc = @intFromBool(new_doc),
            .term_pos = @truncate(term_pos),
            .doc_id = @truncate(doc_id),
        };
        self.num_terms[search_col_idx] += 1;

        if (self.num_terms[search_col_idx] == TOKEN_STREAM_CAPACITY) {
            try self.flushTokenStream(search_col_idx);
        }
    }

    pub inline fn flushTokenStream(self: *TokenStream, search_col_idx: usize) !void {
        const bytes_to_write = @sizeOf(u32) * self.num_terms[search_col_idx];
        _ = try self.output_files[search_col_idx].write(
            std.mem.asBytes(&self.num_terms[search_col_idx]),
            );
        const bytes_written = try self.output_files[search_col_idx].write(
            std.mem.sliceAsBytes(
                self.tokens[search_col_idx][0..self.num_terms[search_col_idx]]
                )
            );
        
        std.debug.assert(bytes_written == bytes_to_write);

        self.num_terms[search_col_idx] = 0;
    }

    pub inline fn incBufferIdx(self: *TokenStream) !void {
        const offset_length = TOKEN_STREAM_CAPACITY - self.buffer_idx;
        if (offset_length <= 16384) {
            @memcpy(
                self.f_data[0..offset_length],
                self.f_data[self.buffer_idx..],
            );
            const bytes_read = try self.input_file.read(self.f_data[offset_length..]);
            if (bytes_read < self.f_data.len - offset_length) {
                // Add newline charachter to end of file to ensure last line is parsed correctly.
                self.f_data[bytes_read + offset_length] = '\n';
            }

            const start_pos = offset_length - (offset_length % 16);
            stringToUpper(self.f_data[start_pos..].ptr, self.f_data.len - start_pos);
            self.buffer_idx = 0;
        }
    }

    pub inline fn iterFieldCSV(self: *TokenStream, byte_idx: *usize) void {
        // Iterate to next field in compliance with RFC 4180.
        _iterFieldCSV(self.f_data, byte_idx);
    }
};
