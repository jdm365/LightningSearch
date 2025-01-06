const std = @import("std");
const TermPos = @import("server.zig").TermPos;

pub const TOKEN_STREAM_CAPACITY = 1_048_576;

pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

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
}

const SIMD_QUOTE_MASK   = @as(@Vector(32, u8), @splat('"'));
const SIMD_NEWLINE_MASK = @as(@Vector(32, u8), @splat('\n'));
const SIMD_COMMA_MASK   = @as(@Vector(32, u8), @splat(','));

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

    while (true) {
        byte_idx.* += 1;

        if (is_quoted) {
            // switch (iterUTF8(buffer, byte_idx)) {
            switch (buffer[byte_idx.* - 1]) {
                '"' => {
                    // Iter over delimeter or escape quote.
                    byte_idx.* += 1;

                    // Check escape quote.
                    if (buffer[byte_idx.* - 1] == '"') continue;
                    return;
                },
                else => {},
            }
        } else {
            // switch (iterUTF8(buffer, byte_idx)) {
            switch (buffer[byte_idx.* - 1]) {
                ',', '\n' => return,
                else => {},
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
            std.debug.print("REFRESH\n", .{});
            @memcpy(
                self.f_data[0..offset_length],
                self.f_data[self.buffer_idx..],
            );
            _ = try self.input_file.read(self.f_data[offset_length..]);

            const start_pos = offset_length - (offset_length % 16);
            stringToUpper(self.f_data[start_pos..].ptr, self.f_data.len);
            self.buffer_idx = 0;
        }
    }

    pub inline fn iterFieldCSV(self: *TokenStream, byte_idx: *usize) void {
        // Iterate to next field in compliance with RFC 4180.
        _iterFieldCSV(self.f_data, byte_idx);
    }
};
