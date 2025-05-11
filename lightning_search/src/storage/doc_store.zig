const std     = @import("std");
const builtin = @import("builtin");

const HuffmanCompressor = @import("../compression/huffman.zig").HuffmanCompressor;
const TermPos           = @import("../server/server.zig").TermPos;

const pq = @import("../parsing/parquet.zig");


const MMAP_MAX_SIZE_HUFFMAN_BUFFER: u64 = 1 << 36;
const MMAP_MAX_SIZE_HUFFMAN_ROW_OFFSETS: u64 = 1 << 24;


inline fn readValFromFile(
    comptime T: type,
    file: *std.fs.File,
) !T {
    var _val: [@sizeOf(T)]u8 = undefined;
    _ = try file.read(std.mem.asBytes(&_val));
    return std.mem.readInt(T, &_val, std.builtin.Endian.little);
}

pub const DocStore = struct {
    dir: []const u8,

    huffman_compressor: HuffmanCompressor,
    literal_byte_sizes: std.ArrayListUnmanaged(usize),
    literal_byte_size_sum: usize,
    literal_col_idxs: std.ArrayListUnmanaged(usize),
    huffman_col_idxs: std.ArrayListUnmanaged(usize),

    literal_rows: std.ArrayListUnmanaged([]u8),

    huffman_col_bit_sizes: []u64,

    huffman_buffer_pos: u64,
    huffman_prev_buffer_offset: u64,

    file_handles: FileHandles,

    arena: std.heap.ArenaAllocator,
    gpa: *std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }),

    row_idx: usize,
    zeroed_range: u64,

    const FileHandles = struct {
        huffman_row_data_file: std.fs.File,
        huffman_row_offsets_file: std.fs.File,
        literal_rows_file: std.fs.File,

        huffman_row_data_mmap_buffer: []align(4096) u8,
        huffman_row_offsets_mmap_buffer: []align(8) u8,

        pub fn init(doc_store: *DocStore, partition_idx: usize) !FileHandles {

            const huffman_row_data_filename = try std.fmt.allocPrint(
                doc_store.gpa.allocator(), 
                "{s}/huffman_row_data_{d}.bin", .{doc_store.dir, partition_idx}
                );
            defer doc_store.gpa.allocator().free(huffman_row_data_filename);

            const huffman_row_data_file = try std.fs.cwd().createFile(
                huffman_row_data_filename,
                .{ .read = true }
                );
            try huffman_row_data_file.setEndPos(MMAP_MAX_SIZE_HUFFMAN_BUFFER);
            const huffman_row_data_mmap_buffer = try std.posix.mmap(
                null,
                MMAP_MAX_SIZE_HUFFMAN_BUFFER,
                std.posix.PROT.WRITE | std.posix.PROT.READ,
                .{ .TYPE = .SHARED },
                huffman_row_data_file.handle,
                0,
            );
            try std.posix.madvise(
                huffman_row_data_mmap_buffer.ptr,
                MMAP_MAX_SIZE_HUFFMAN_BUFFER,
                std.posix.MADV.SEQUENTIAL,
            );

            const huffman_row_offsets_filename = try std.fmt.allocPrint(
                doc_store.gpa.allocator(), 
                "{s}/huffman_row_offsets_{d}.bin", .{doc_store.dir, partition_idx}
                );
            defer doc_store.gpa.allocator().free(huffman_row_offsets_filename);

            const huffman_row_offsets_file = try std.fs.cwd().createFile(
                huffman_row_offsets_filename,
                .{ .read = true }
                );
            try huffman_row_offsets_file.setEndPos(MMAP_MAX_SIZE_HUFFMAN_BUFFER);
            const huffman_row_offsets_mmap_buffer = try std.posix.mmap(
                null,
                MMAP_MAX_SIZE_HUFFMAN_BUFFER,
                std.posix.PROT.WRITE | std.posix.PROT.READ,
                .{ .TYPE = .SHARED },
                huffman_row_data_file.handle,
                0,
            );

            const literal_rows_filename = try std.fmt.allocPrint(
                doc_store.gpa.allocator(), 
                "{s}/literal_rows_{d}.bin", .{doc_store.dir, partition_idx}
                );
            const literal_rows_file = try std.fs.cwd().createFile(
                literal_rows_filename,
                .{ .read = true }
                );
            defer doc_store.gpa.allocator().free(literal_rows_filename);

            return FileHandles{
                .huffman_row_data_file = huffman_row_data_file,
                .huffman_row_offsets_file = huffman_row_offsets_file,
                .literal_rows_file = literal_rows_file,

                .huffman_row_data_mmap_buffer = huffman_row_data_mmap_buffer,
                .huffman_row_offsets_mmap_buffer = huffman_row_offsets_mmap_buffer,
            };
        }

        pub fn deinit(self: *FileHandles) void {
            self.huffman_row_data_file.close();
            self.huffman_row_offsets_file.close();
            self.literal_rows_file.close();
            self.huffman_field_sizes_file.close();

            std.posix.munmap(self.huffman_row_data_mmap_buffer);
            std.posix.munmap(self.huffman_row_offsets_mmap_buffer);

        }

    };

    pub fn init(
        gpa: *std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }),
        literal_byte_sizes: *const std.ArrayListUnmanaged(usize),
        literal_col_idxs: *const std.ArrayListUnmanaged(usize),
        huffman_col_idxs: *const std.ArrayListUnmanaged(usize),
        dir: []const u8,
        partition_idx: usize,
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

            .literal_rows = std.ArrayListUnmanaged([]u8){},

            .huffman_col_bit_sizes = try gpa.allocator().alloc(
                u64, 
                huffman_col_idxs.items.len,
                ),

            .huffman_prev_buffer_offset = 0,
            .huffman_buffer_pos = 0,

            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .gpa = gpa,

            .row_idx = 1,
            .zeroed_range = 0,

            .dir = dir,

            .file_handles = FileHandles{
                .huffman_row_data_file = undefined,
                .huffman_row_offsets_file = undefined,
                .literal_rows_file = undefined,

                .huffman_row_data_mmap_buffer = undefined,
                .huffman_row_offsets_mmap_buffer = undefined,
            },
        };
        store.literal_byte_sizes = try literal_byte_sizes.clone(store.gpa.allocator());
        store.literal_col_idxs   = try literal_col_idxs.clone(store.gpa.allocator());
        store.huffman_col_idxs   = try huffman_col_idxs.clone(store.gpa.allocator());

        store.file_handles = try FileHandles.init(&store, partition_idx);

        @memset(
            store.file_handles.huffman_row_data_mmap_buffer[0..(1 << 20)],
            0,
        );
        store.zeroed_range = 1 << 20;

        store.file_handles.huffman_row_offsets_mmap_buffer[0] = 0;

        return store;
    }

    pub fn deinit(self: *DocStore) !void {
        try self.flush();

        self.arena.deinit();
        self.file_handles.deinit();
        self.literal_rows.deinit(self.gpa.allocator());
        self.literal_byte_sizes.deinit(self.gpa.allocator());
        self.literal_col_idxs.deinit(self.gpa.allocator());
        self.huffman_col_idxs.deinit(self.gpa.allocator());
        self.gpa.allocator().free(self.huffman_col_bit_sizes);
    }

    pub fn printMemoryUsage(self: *const DocStore) void {
        var total_bytes: usize = 0;
        const huffman_data_size: usize = self.huffman_buffer_pos + (8 * self.row_idx);
        var literal_rows_sum: usize = 0;

        for (self.literal_rows.items) |row| {
            literal_rows_sum += row.len;
        }
        total_bytes += huffman_data_size + total_bytes;

        if (huffman_data_size >= (1 << 20)) {
            std.debug.print("HUFFMAN DATA SIZE:  {d}MB\n", .{@divFloor(huffman_data_size, 1 << 20)});
            std.debug.print("LITERAL ROWS SIZE:  {d}MB\n", .{@divFloor(literal_rows_sum, 1 << 20)});
            std.debug.print("-------------------------\n", .{});
            std.debug.print("TOTAL SIZE:         {d}MB\n", .{@divFloor(total_bytes, 1 << 20)});
        } else {
            std.debug.print("LITERAL ROWS SIZE:  {d}kB\n", .{@divFloor(literal_rows_sum, 1 << 10)});
            std.debug.print("-------------------------\n", .{});
            std.debug.print("TOTAL SIZE:         {d}kB\n", .{@divFloor(total_bytes, 1 << 10)});
        }

    }


    pub fn addRow(
        self: *DocStore,
        byte_positions: []TermPos,
        row_data: []u8,
    ) !void {
        if (self.literal_byte_size_sum > 0) {
            var literal_row = try self.gpa.allocator().alloc(
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
            const val = try self.literal_rows.addOne(self.gpa.allocator());
            val.* = literal_row[0..literal_row_offset];
        }

        const prev_offset = self.huffman_buffer_pos;
        const buf_rem = self.zeroed_range - prev_offset;
        if (buf_rem < 65536) {
            @branchHint(.unlikely);
            @memset(
                self.file_handles.huffman_row_data_mmap_buffer[
                    self.zeroed_range..
                ][0..(comptime 1 << 20)],
                0,
            );
            try std.posix.madvise(
                @alignCast(self.file_handles.huffman_row_data_mmap_buffer[self.zeroed_range..].ptr),
                (comptime 1 << 20),
                std.posix.MADV.DONTNEED,
            );

            self.zeroed_range += (comptime 1 << 20);
        }

        var row_bit_size: usize = 0;
        for (self.huffman_col_idxs.items) |col_idx| {
            const start_pos = byte_positions[col_idx].start_pos;
            const field_len = byte_positions[col_idx].field_len;

            if (field_len == 0) {
                self.huffman_col_bit_sizes[col_idx] = 0;
                continue;
            }

            const compressed_bit_size = try self.huffman_compressor.compressOffset(
                    row_data[start_pos..][0..field_len],
                    self.file_handles.huffman_row_data_mmap_buffer[prev_offset..][@divFloor(row_bit_size, 8)..],
                    row_bit_size % 8,
                    );
            self.huffman_col_bit_sizes[col_idx] = compressed_bit_size;
            row_bit_size += compressed_bit_size;
        }
        var row_byte_size = try std.math.divCeil(usize, row_bit_size, 8);

        var RO_u64_mmap_buf: [*]u64 = @ptrCast(self.file_handles.huffman_row_offsets_mmap_buffer.ptr);
        RO_u64_mmap_buf[self.row_idx] = prev_offset + row_byte_size;
        self.row_idx += 1;

        for (self.huffman_col_bit_sizes) |nbits| {
            pq.encodeVbyte(
                self.file_handles.huffman_row_data_mmap_buffer[prev_offset..].ptr,
                &row_byte_size,
                nbits,
            );
        }
        self.huffman_buffer_pos += row_byte_size;
    }
    
    pub fn flush(self: *DocStore) !void {
        try self.file_handles.huffman_row_data_file.setEndPos(
            self.huffman_buffer_pos
            );
        try self.file_handles.huffman_row_offsets_file.setEndPos(
            self.row_idx * 8
            );
    }

    // pub fn flush(self: *DocStore) !void {
        // const start_time = std.time.milliTimestamp();
// 
        // // TODO: Needed
        // // 1. DONE - Offsets need to be true file byte offsets.
        // // 2. Assess what field sizes/field rem bits actually need to be stored.
        // // 3. Build direct IO seeking getRow.
        // // 4. Look at mmapping for both writes and reads.
        // // 5. Consider using bit field lengths, allowing variable byte fields
        // //    and better huffman compression. Then in flush, store all field
        // //    bit lengths as vbyte prefix values. Would need to have row
        // //    offsets account for this. Could do with field size tracking,
        // //    accumulating eventual vbyte field size values and adding to
        // //    row_size in addRow. (I think this would remove the need
        // //    for rem_bits array too).
// 
        // const buffer_size = self.huffman_buffer_pos;
        // self.huffman_buffer_pos = 0;
// 
        // _ = try self.file_handles.huffman_row_data_file.write(
            // self.huffman_row_data.items[0..buffer_size],
            // );
        // std.debug.print("Buffer: {any}\n", .{self.huffman_row_data.items[0..128]});
        // std.debug.print("Buffer: {any}\n", .{self.huffman_row_data.items[buffer_size - 128..][0..128]});
// 
        // for (0..self.huffman_row_offsets.items.len) |idx| {
            // self.huffman_row_offsets.items[idx] += self.huffman_prev_buffer_offset;
        // }
        // _ = try self.file_handles.huffman_row_offsets_file.write(
            // std.mem.sliceAsBytes(self.huffman_row_offsets.items),
            // );
// 
        // for (self.literal_rows.items) |row| {
            // _ = try self.file_handles.literal_rows_file.write(row);
        // }
// 
        // self.huffman_prev_buffer_offset += buffer_size;
// 
// 
        // self.huffman_row_data.items.len = 1 << 20;
        // @memset(self.huffman_row_data.items[0..(1 << 20)], 0);
        // self.huffman_row_offsets.items.len = 1;
        // self.huffman_row_offsets.items[0] = 0;
        // self.literal_rows.clearRetainingCapacity();
// 
        // const end_time = std.time.milliTimestamp();
        // const execution_time_ms: usize = @intCast(end_time - start_time);
        // std.debug.print(
            // "FLUSH TIME: {d}ms\nBuffer size: {d}MB\n\n", 
            // .{execution_time_ms, buffer_size / (1 << 20)}
            // );
    // }

    pub inline fn getRow(
        _: *DocStore,
        _: usize,
        _: []u8,
        _: []TermPos,
    ) !void {
        @breakpoint();
    }


    // pub inline fn getRow(
        // self: *DocStore,
        // row_idx: usize,
        // row_data: []u8,
        // offsets: []TermPos,
    // ) !void {
        // // Assume all huffman for now.
        // std.debug.assert(self.literal_col_idxs.items.len == 0);
// 
        // try self.file_handles.huffman_row_offsets_file.seekTo(row_idx * 8);
        // const huffman_buffer_byte_offset = try readValFromFile(
            // u64,
            // &self.file_handles.huffman_row_offsets_file,
        // );
// 
        // var col_idx: usize = 0;
        // while (col_idx < self.huffman_col_idxs.items.len) {
            // self.huffman_col_bit_sizes[col_idx] = try readValFromFile(
                // u64,
                // &self.file_handles.huffman_row_data_file,
            // );
            // col_idx += 1;
        // }
// 
        // const start_byte  = self.huffman_row_offsets.items[row_idx];
        // const end_byte    = self.huffman_row_offsets.items[row_idx + 1];
// 
        // const huffman_row = self.huffman_row_data.items[start_byte..end_byte];
        // const huffman_idx_offset  = self.huffman_col_idxs.items.len * row_idx;
// 
        // var input_pos:  usize = 0;
        // var output_pos: usize = 0;
        // for (0..huffman_field_sizes.len) |idx| {
            // offsets[idx].start_pos = @truncate(output_pos);
// 
            // const compressed_len = huffman_field_sizes[idx];
            // if (compressed_len == 0) {
                // offsets[idx].field_len = 0;
                // continue;
            // }
// 
            // const rem_bits = huffman_field_rem_bits[idx];
            // offsets[idx].field_len = @truncate(
                // try self.huffman_compressor.decompress(
                    // huffman_row[input_pos..][0..compressed_len],
                    // row_data[output_pos..],
                    // rem_bits,
                // )
            // );
// 
            // input_pos  += compressed_len;
            // output_pos += offsets[idx].field_len;
        // }
    // }
};
