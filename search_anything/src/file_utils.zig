const std = @import("std");
const csv = @import("csv.zig");
const json = @import("json.zig");
const string_utils= @import("string_utils.zig");

pub const FileType = enum {
    CSV,
    JSON,
};

pub const token_t = packed struct(u32) {
    new_doc: u1,
    term_pos: u7,
    doc_id: u24
};

pub const TOKEN_STREAM_CAPACITY = 1_048_576;

pub const SingleThreadedDoubleBufferedReader = struct {
    file: std.fs.File,
    buffers: []u8,
    overflow_buffer: []u8,
    single_buffer_size: usize,
    current_buffer: usize,
    
    pub fn init(
        allocator: std.mem.Allocator, 
        file: std.fs.File,
        ) !SingleThreadedDoubleBufferedReader {
        const buffer_size = 1 << 22;
        const overflow_size = 16384;

        // Make buffers larger to accommodate overlap
        const buffers = try allocator.alloc(u8, 2 * buffer_size);
        const overflow_buffer = try allocator.alloc(u8, 2 * overflow_size);

        _ = try file.read(buffers);
        @memcpy(
            overflow_buffer[0..overflow_size], 
            buffers[(2 * buffer_size) - overflow_size..],
            );
        
        return SingleThreadedDoubleBufferedReader{
            .file = file,
            .buffers = buffers,
            .overflow_buffer = overflow_buffer,
            .single_buffer_size = buffer_size,
            .current_buffer = 0,
        };
    }

    pub fn deinit(
        self: *SingleThreadedDoubleBufferedReader, 
        allocator: std.mem.Allocator,
        ) void {
        allocator.free(self.buffers);
        allocator.free(self.overflow_buffer);
    }

    pub inline fn getBuffer(
        self: *SingleThreadedDoubleBufferedReader, 
        file_pos: usize,
        ) ![]u8 {
        const index = file_pos % self.buffers.len;

        if (index >= self.single_buffer_size) {
            if (self.current_buffer == 0) {
                _ = try self.file.read(self.buffers[0..self.single_buffer_size]);
                self.current_buffer = 1;

                const overflow_size = @divFloor(self.overflow_buffer.len, 2);
                @memcpy(
                    self.overflow_buffer[overflow_size..], 
                    self.buffers[0..overflow_size],
                    );
            }
        } else {
            if (self.current_buffer == 1) {
                _ = try self.file.read(self.buffers[self.single_buffer_size..]);
                self.current_buffer = 0;

                const overflow_size = @divFloor(self.overflow_buffer.len, 2);
                @memcpy(
                    self.overflow_buffer[0..overflow_size], 
                    self.buffers[self.buffers.len - overflow_size..],
                    );
            }
        }
        const bytes_from_end = self.buffers.len - index;
        if (bytes_from_end <= 16384) {
            return self.overflow_buffer[16384 - bytes_from_end..];
        }
        return self.buffers[index..];
    }
};

pub const DoubleBufferedReader = struct {
    file: std.fs.File,
    buffers: []u8,
    overflow_buffer: []u8,
    single_buffer_size: usize,
    current_buffer: usize,
    thread: ?std.Thread = null,
    end_token: u8,

    semaphore: std.Thread.Semaphore,

    active_read: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    
    pub fn init(
        allocator: std.mem.Allocator, 
        file: std.fs.File,
        comptime end_token: u8,
        ) !DoubleBufferedReader {

        const buffer_size = 1 << 22;
        const overflow_size = 16384;
        const buffers = try allocator.alloc(u8, 2 * buffer_size);
        const overflow_buffer = try allocator.alloc(u8, 2 * overflow_size);
        _ = try file.read(buffers);
        @memcpy(
            overflow_buffer[0..overflow_size], 
            buffers[(2 * buffer_size) - overflow_size..],
        );
        
        return DoubleBufferedReader{
            .file = file,
            .buffers = buffers,
            .overflow_buffer = overflow_buffer,
            .single_buffer_size = buffer_size,
            .current_buffer = 0,
            .semaphore = .{},
            .end_token = end_token,
        };
    }

    pub fn deinit(self: *DoubleBufferedReader, allocator: std.mem.Allocator) void {
        if (self.thread) |thread| {
            thread.join();
        }
        allocator.free(self.buffers);
        allocator.free(self.overflow_buffer);
    }

    fn readBufferThread(
        reader: *DoubleBufferedReader,
        buffer: []u8,
        overflow_dest: []u8,
        overflow_size: usize,
        current_buffer: usize,
    ) void {
        const bytes_read = reader.file.read(buffer) catch {
            std.debug.print("Error reading file\n", .{});
            return;
        };
        if (bytes_read != buffer.len) buffer[bytes_read] = reader.end_token;

        const start_idx = (buffer.len - overflow_size) * (1 - current_buffer);
        const end_idx   = start_idx + overflow_size;
        @memcpy(overflow_dest, buffer[start_idx..end_idx]);

        reader.active_read.store(false, .release);
        reader.semaphore.post();
    }

    pub fn getBuffer(
        self: *DoubleBufferedReader, 
        file_pos: usize,
    ) ![]u8 {
        const index = file_pos % self.buffers.len;
        const new_buffer = @intFromBool(index >= self.single_buffer_size);

        const bytes_from_end = self.buffers.len - index;
        if (bytes_from_end <= 16384) {
            if (self.thread) |thread| {
                self.semaphore.wait();
                thread.join();
                self.thread = null;
            }
            return self.overflow_buffer[16384 - bytes_from_end..];
        }

        if (new_buffer == self.current_buffer) {
            return self.buffers[index..];
        }

        const overflow_size = @divFloor(self.overflow_buffer.len, 2);
        
        const overflow_start_idx = overflow_size * new_buffer;
        const overflow_end_idx   = overflow_start_idx + overflow_size;

        const buffer_start_idx = self.single_buffer_size * (1 - new_buffer);
        const buffer_end_idx   = buffer_start_idx + self.single_buffer_size;

        if (self.thread) |thread| {
            self.semaphore.wait();
            thread.join();
            self.thread = null;
        }
        self.current_buffer = new_buffer;

        self.active_read.store(true, .release);
        self.thread = try std.Thread.spawn(
            .{},
            readBufferThread,
            .{
                self,
                self.buffers[buffer_start_idx..buffer_end_idx],
                self.overflow_buffer[overflow_start_idx..overflow_end_idx],
                overflow_size,
                self.current_buffer,
            },
        );

        return self.buffers[index..];
    }
};



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
            token_buffers[idx] = try allocator.alloc(
                token_t, 
                TOKEN_STREAM_CAPACITY,
                );
        }

        const token_stream = TokenStream{
            .tokens = token_buffers,
            .f_data = try allocator.alignedAlloc(
                u8, 
                std.mem.page_size, 
                TOKEN_STREAM_CAPACITY,
                ),
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
                // Add newline charachter to end of file 
                // to ensure last line is parsed correctly.
                self.f_data[bytes_read + offset_length] = '\n';
            }

            const start_pos = offset_length - (offset_length % 16);
            string_utils.stringToUpper(
                self.f_data[start_pos..].ptr, 
                self.f_data.len - start_pos,
                );
            self.buffer_idx = 0;
        }
    }

    pub inline fn iterFieldCSV(self: *TokenStream, byte_idx: *usize) void {
        // Iterate to next field in compliance with RFC 4180.
        csv._iterFieldCSV(self.f_data, byte_idx);
    }

    pub inline fn iterFieldJSON(self: *TokenStream, byte_idx: *usize) void {
        // Iterate to next field in compliance with RFC 4180.
        json._iterFieldJSON(self.f_data, byte_idx);
    }
};
