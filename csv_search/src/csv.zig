const std = @import("std");
const TermPos = @import("server.zig").TermPos;

pub const TOKEN_STREAM_CAPACITY = 1_048_576;


pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

const Vec64 = @Vector(8, u8);
const Vec128 = @Vector(16, u8);
pub inline fn stringToUpper(str: [*]u8, len: usize) void {
    var index: usize = 0;

    while (index + 16 <= len) {
        const input = @as(*Vec128, @ptrCast(@alignCast(str[index..index+16])));

        const ascii_a: Vec128 = @splat('a');
        const ascii_z: Vec128 = @splat('z');
        const case_diff: Vec128 = @splat('a' - 'A');
        const two: Vec128       = @splat(2);

        const greater_than_a = input.* >= ascii_a;
        const less_equal_z   = input.* <= ascii_z;
        const to_sub = @as(
            Vec128, 
            @intFromBool(@as(Vec128, @intFromBool(greater_than_a)) + @as(Vec128, @intFromBool(less_equal_z)) == two)
            ) * case_diff;
        input.* -= to_sub;

        index += 16;
    }

    for (index..len) |idx| {
        str[idx] = std.ascii.toUpper(str[idx]);
    }
}

// const U64_QUOTE_MASK: Vec64     = @splat('"');
// const U64_NEWLINE_MASK: Vec64   = @splat('\n');
// const U64_COMMA_MASK: Vec64     = @splat(',');
const U64_QUOTE_MASK: u64     = 0x2222222222222222;
const U64_NEWLINE_MASK: u64   = 0x0a0a0a0a0a0a0a0a;
const U64_COMMA_MASK: u64     = 0x2c2c2c2c2c2c2c2c;

const SIMD_QUOTE_MASK: Vec128 align(16)   = @splat('"');
const SIMD_NEWLINE_MASK: Vec128 align(16) = @splat('\n');
const SIMD_COMMA_MASK: Vec128 align(16)   = @splat(',');

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

    // var vec128: *const Vec128 = undefined;
    // var vec64: *const u64 = undefined;

    outer_loop: while (true) {
        // vec128 = @as(*const Vec128, @ptrCast(@alignCast(buffer[byte_idx.*..])));
        // vec64 = @as(*const Vec64, @ptrCast(@alignCast(buffer[byte_idx.*..])));
        // vec64 = @as(*const u64, @alignCast(@ptrCast(buffer[byte_idx.*..])));
        // const matches = (vec64.* ^ U64_QUOTE_MASK) &
                        // (vec64.* ^ U64_NEWLINE_MASK) &
                        // (vec64.* ^ U64_COMMA_MASK);
        // const mask = ((matches -% 0x0101010101010101) & ~matches & 0x8080808080808080);
        // if (mask == 0) {
            // byte_idx.* += 8;
            // continue;
        // }
        // 
        // byte_idx.* += @ctz(mask) >> 3;

        var match_idx: u16 = undefined;
        asm volatile (
            \\ movdqu %[vec], %%xmm0       // Load data
            \\ movdqu %[quote], %%xmm1     // Load quote mask
            \\ movdqa %%xmm0, %%xmm2       // Copy data
            \\ pcmpeqb %%xmm1, %%xmm2      // Compare with quotes, result in xmm2
            \\ movdqu %[newline], %%xmm1   // Load newline mask
            \\ movdqa %%xmm0, %%xmm3       // Copy data again
            \\ pcmpeqb %%xmm1, %%xmm3      // Compare with newlines
            \\ por %%xmm3, %%xmm2          // OR results together
            \\ movdqu %[comma], %%xmm1     // Load comma mask
            \\ movdqa %%xmm0, %%xmm3       // Copy data once more
            \\ pcmpeqb %%xmm1, %%xmm3      // Compare with commas
            \\ por %%xmm3, %%xmm2          // OR final results
            \\ pmovmskb %%xmm2, %%edx      // Extract mask to GP register
            \\ tzcnt %%dx, %[out]          // Count trailing zeros to get first match
            : [out] "=r" (match_idx)        // Using general register constraint
            : [vec] "m" (buffer[byte_idx.*..][0..16].*),
              [quote] "m" (SIMD_QUOTE_MASK),
              [newline] "m" (SIMD_NEWLINE_MASK),
              [comma] "m" (SIMD_COMMA_MASK)
            : "xmm0", "xmm1", "xmm2", "edx"
        );
        // byte_idx.* += @intCast(result);
        std.debug.print("{s}\n", .{buffer[byte_idx.*..][0..16].*});
        byte_idx.* += @intCast(match_idx);
        std.debug.print("result: {}\n\n", .{match_idx});

        if (is_quoted) {

            switch (buffer[byte_idx.*]) {
                '"' => {
                    // Iter over delimeter or escape quote.
                    byte_idx.* += 1;

                    // Check escape quote.
                    if (buffer[byte_idx.*] == '"') {
                        byte_idx.* += 1;
                        continue;
                    }
                    byte_idx.* += 1;
                    break :outer_loop;
                },
                else => byte_idx.* += 1,
            }
        } else {
            switch (buffer[byte_idx.*]) {
                ',', '\n' => {
                    byte_idx.* += 1;
                    break: outer_loop;
                },
                else => byte_idx.* += 1,
            }
        }
    }
}

pub inline fn iterLineCSV(buffer: []const u8, byte_idx: *usize) void {
    // Iterate to next line in compliance with RFC 4180.
    while (true) {
        // switch (iterUTF8(buffer, byte_idx)) {
        byte_idx.* += 1;

        switch (buffer[byte_idx.* - 1]) {
            '"' => {
                while (true) {
                    // if (iterUTF8(buffer, byte_idx) == '"') {
                    byte_idx.* += 1;
                    if (buffer[byte_idx.* - 1] == '"') {
                        if (buffer[byte_idx.*] == '"') {
                            byte_idx.* += 1;
                            continue;
                        }
                        break;
                    }
                }
            },
            '\n' => {
                return;
            },
            else => {},
        }
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
            .doc_id = @intCast(doc_id),
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
            std.mem.sliceAsBytes(self.tokens[search_col_idx][0..self.num_terms[search_col_idx]])
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
            _ = try self.input_file.read(self.f_data[offset_length..]);

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
