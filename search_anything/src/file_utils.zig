const std = @import("std");

pub const FileType = enum {
    CSV,
    JSON,
};

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
