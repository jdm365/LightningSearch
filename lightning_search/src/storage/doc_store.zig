const std     = @import("std");
const builtin = @import("builtin");

const HuffmanCompressor = @import("../compression/huffman.zig").HuffmanCompressor;
const TermPos           = @import("../server/server.zig").TermPos;

const fu = @import("../storage/file_utils.zig");
const pq = @import("../parsing/parquet.zig");


const MMAP_MAX_SIZE_HUFFMAN_BUFFER: u64 = 1 << 36;
const MMAP_MAX_SIZE_HUFFMAN_ROW_OFFSETS: u64 = 1 << 27;
// Num rows (1 << 27) bytes / 8 bytes per row = (1 << 24) rows = ~16 million rows.


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
    huffman_col_bit_sizes: []u64,

    huffman_buffer_pos: u64,
    huffman_prev_buffer_offset: u64,

    file_handles: FileHandles,

    arena: std.heap.ArenaAllocator,
    gpa: *std.heap.DebugAllocator(.{ .thread_safe = true }),

    row_idx: usize,
    zeroed_range: u64,

    prev_buf_idx: u64,

    const FileHandles = struct {
        huffman_row_data_file: std.fs.File,
        huffman_row_offsets_file: std.fs.File,

        huffman_row_data_writer: fu.SingleThreadedDoubleBufferedWriter(u8),
        huffman_row_offsets_writer: fu.SingleThreadedDoubleBufferedWriter(u64),

        huffman_row_data_mmap_buffer: []align(4096) u8,
        huffman_row_offsets_mmap_buffer: []align(4096) u8,

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
            const huffman_row_data_writer = try fu.SingleThreadedDoubleBufferedWriter(u8).init(
                doc_store.gpa.allocator(),
                huffman_row_data_file,
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
            const huffman_row_offsets_writer = try fu.SingleThreadedDoubleBufferedWriter(u64).init(
                doc_store.gpa.allocator(),
                huffman_row_offsets_file,
            );

            return FileHandles{
                .huffman_row_data_file = huffman_row_data_file,
                .huffman_row_offsets_file = huffman_row_offsets_file,

                .huffman_row_data_writer = huffman_row_data_writer,
                .huffman_row_offsets_writer = huffman_row_offsets_writer,

                .huffman_row_data_mmap_buffer = undefined,
                .huffman_row_offsets_mmap_buffer = undefined,
            };
        }

        pub fn initFromDisk(doc_store: *DocStore, partition_idx: usize) !FileHandles {

            const huffman_row_data_filename = try std.fmt.allocPrint(
                doc_store.gpa.allocator(), 
                "{s}/huffman_row_data_{d}.bin", .{doc_store.dir, partition_idx}
                );
            defer doc_store.gpa.allocator().free(huffman_row_data_filename);

            const huffman_row_data_file = try std.fs.cwd().openFile(
                huffman_row_data_filename,
                std.fs.File.OpenFlags{
                    .mode = .read_write,
                }
                );
            const huffman_row_data_writer = try fu.SingleThreadedDoubleBufferedWriter(u8).init(
                doc_store.gpa.allocator(),
                huffman_row_data_file,
            );

            const huffman_row_offsets_filename = try std.fmt.allocPrint(
                doc_store.gpa.allocator(), 
                "{s}/huffman_row_offsets_{d}.bin", .{doc_store.dir, partition_idx}
                );
            defer doc_store.gpa.allocator().free(huffman_row_offsets_filename);

            const huffman_row_offsets_file = try std.fs.cwd().openFile(
                huffman_row_offsets_filename,
                std.fs.File.OpenFlags{
                    .mode = .read_write,
                }
                );
            const huffman_row_offsets_writer = try fu.SingleThreadedDoubleBufferedWriter(u64).init(
                doc_store.gpa.allocator(),
                huffman_row_offsets_file,
            );

            const h_row_data_mmap_buf = try std.posix.mmap(
                null,
                MMAP_MAX_SIZE_HUFFMAN_BUFFER,
                std.posix.PROT.READ,
                .{ 
                    .TYPE = .PRIVATE, 
                },
                huffman_row_data_file.handle,
                0,
            );
            try std.posix.madvise(
                h_row_data_mmap_buf.ptr,
                MMAP_MAX_SIZE_HUFFMAN_BUFFER,
                std.posix.MADV.RANDOM,
            );

            const h_row_offsets_mmap_buf = try std.posix.mmap(
                null,
                MMAP_MAX_SIZE_HUFFMAN_ROW_OFFSETS,
                std.posix.PROT.READ,
                .{ 
                    .TYPE = .PRIVATE, 
                },
                huffman_row_offsets_file.handle,
                0,
            );
            try std.posix.madvise(
                h_row_offsets_mmap_buf.ptr,
                MMAP_MAX_SIZE_HUFFMAN_ROW_OFFSETS,
                std.posix.MADV.RANDOM,
            );

            return FileHandles{
                .huffman_row_data_file = huffman_row_data_file,
                .huffman_row_offsets_file = huffman_row_offsets_file,

                .huffman_row_data_writer = huffman_row_data_writer,
                .huffman_row_offsets_writer = huffman_row_offsets_writer,

                .huffman_row_data_mmap_buffer = h_row_data_mmap_buf,
                .huffman_row_offsets_mmap_buffer = h_row_offsets_mmap_buf,
            };
        }

        pub fn deinit(self: *FileHandles, allocator: std.mem.Allocator) void {
            self.huffman_row_data_file.close();
            self.huffman_row_offsets_file.close();

            self.huffman_row_offsets_writer.deinit(allocator);
            self.huffman_row_data_writer.deinit(allocator);

            std.posix.munmap(self.huffman_row_data_mmap_buffer);
            std.posix.munmap(self.huffman_row_offsets_mmap_buffer);
        }

    };

    pub fn init(
        gpa: *std.heap.DebugAllocator(.{ .thread_safe = true }),
        dir: []const u8,
        partition_idx: usize,
        num_cols: usize,
    ) !DocStore {
        var store = DocStore {
            .huffman_compressor = HuffmanCompressor.init(),
            .huffman_col_bit_sizes = try gpa.allocator().alloc(
                u64, 
                num_cols,
                ),

            .huffman_prev_buffer_offset = 0,
            .huffman_buffer_pos = 0,

            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .gpa = gpa,

            .row_idx = 0,
            .zeroed_range = 0,

            .dir = dir,

            .file_handles = FileHandles{
                .huffman_row_data_file = undefined,
                .huffman_row_offsets_file = undefined,

                .huffman_row_data_writer = undefined,
                .huffman_row_offsets_writer = undefined,

                .huffman_row_data_mmap_buffer = undefined,
                .huffman_row_offsets_mmap_buffer = undefined,
            },

            .prev_buf_idx = 0,
        };
        store.file_handles = try FileHandles.init(&store, partition_idx);

        return store;
    }

    pub fn deinit(self: *DocStore) !void {
        try self.flush();

        self.arena.deinit();
        self.file_handles.deinit(self.gpa.allocator());
        self.gpa.allocator().free(self.huffman_col_bit_sizes);
    }

    pub fn initFromDisk(
        store: *DocStore,
        gpa: *std.heap.DebugAllocator(.{ .thread_safe = true }),
        dir: []const u8, 
        partition_idx: usize,
        num_cols: usize,
        ) !void {
        store.* = DocStore {
            .huffman_compressor = HuffmanCompressor.init(),
            .huffman_col_bit_sizes = undefined,

            .huffman_prev_buffer_offset = 0,
            .huffman_buffer_pos = 0,

            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .gpa = gpa,

            .row_idx = 0,
            .zeroed_range = 0,

            .dir = dir,

            .file_handles = FileHandles{
                .huffman_row_data_file = undefined,
                .huffman_row_offsets_file = undefined,

                .huffman_row_data_writer = undefined,
                .huffman_row_offsets_writer = undefined,

                .huffman_row_data_mmap_buffer = undefined,
                .huffman_row_offsets_mmap_buffer = undefined,
            },

            .prev_buf_idx = 0,
        };
        store.huffman_col_bit_sizes = try store.gpa.allocator().alloc(u64, num_cols);
        store.file_handles = try FileHandles.initFromDisk(store, partition_idx);

        store.huffman_buffer_pos = try store.file_handles.huffman_row_data_file.getEndPos();
        store.row_idx = try store.file_handles.huffman_row_offsets_file.getEndPos() >> 3;
    }

    pub fn printMemoryUsage(self: *const DocStore) void {
        const total_bytes: usize = self.huffman_buffer_pos + (8 * self.row_idx);

        if (total_bytes >= (1 << 20)) {
            std.debug.print("TOTAL SIZE: {d}MB\n", .{@divFloor(total_bytes, 1 << 20)});
        } else {
            std.debug.print("TOTAL SIZE: {d}kB\n", .{@divFloor(total_bytes, 1 << 10)});
        }

    }


    pub fn addRow(
        self: *DocStore,
        byte_positions: []TermPos,
        row_data: []u8,
    ) !void {
        // | -- data -- |<offset>| -- bit sizes -- |.| -- data -- | ...
        var row_data_buf    = try self.file_handles.huffman_row_data_writer.getBuffer(self.huffman_buffer_pos);
        var row_offsets_buf = try self.file_handles.huffman_row_offsets_writer.getBuffer(self.row_idx);

        var row_bit_size: usize = 0;
        for (0..self.huffman_col_bit_sizes.len) |col_idx| {
            const start_pos = byte_positions[col_idx].start_pos;
            const field_len = byte_positions[col_idx].field_len;

            if (field_len == 0) {
                self.huffman_col_bit_sizes[col_idx] = 0;
                continue;
            }

            const compressed_bit_size = try self.huffman_compressor.compressOffset(
                    row_data[start_pos..][0..field_len],
                    row_data_buf[@divFloor(row_bit_size, 8)..],
                    row_bit_size % 8,
                    );
            self.huffman_col_bit_sizes[col_idx] = compressed_bit_size;
            row_bit_size += compressed_bit_size;
        }
        var byte_pos = try std.math.divCeil(usize, row_bit_size, 8);

        row_offsets_buf[0] = self.huffman_buffer_pos + byte_pos;
        self.row_idx += 1;

        for (self.huffman_col_bit_sizes) |nbits| {
            pq.encodeVbyte(
                row_data_buf.ptr,
                &byte_pos,
                nbits,
            );
        }
        self.huffman_buffer_pos += byte_pos;
    }
    
    pub fn flush(self: *DocStore) !void {
        self.file_handles.huffman_row_data_writer.flush();
        self.file_handles.huffman_row_offsets_writer.flush();

        try self.file_handles.huffman_row_data_file.setEndPos(
            self.huffman_buffer_pos
            );
        try self.file_handles.huffman_row_offsets_file.setEndPos(
            self.row_idx * 8
            );

        // Set up mmap buffers.
        self.file_handles.huffman_row_data_mmap_buffer = try std.posix.mmap(
            null,
            self.huffman_buffer_pos,
            std.posix.PROT.READ,
            .{ 
                .TYPE = .PRIVATE, 
            },
            self.file_handles.huffman_row_data_file.handle,
            0,
        );
        try std.posix.madvise(
            self.file_handles.huffman_row_data_mmap_buffer.ptr,
            self.huffman_buffer_pos,
            std.posix.MADV.RANDOM,
        );
        self.file_handles.huffman_row_offsets_mmap_buffer = try std.posix.mmap(
            null,
            self.row_idx * 8,
            std.posix.PROT.READ,
            .{ 
                .TYPE = .PRIVATE, 
            },
            self.file_handles.huffman_row_offsets_file.handle,
            0,
        );
        try std.posix.madvise(
            self.file_handles.huffman_row_offsets_mmap_buffer.ptr,
            self.row_idx * 8,
            std.posix.MADV.RANDOM,
        );
    }

    pub inline fn getRow(
        self: *DocStore,
        row_idx: usize,
        row_data: *std.ArrayListUnmanaged(u8),
        allocator: std.mem.Allocator,
        offsets: []TermPos,

        bit_sizes: []u32,
    ) !void {
        const RO_u64_mmap_buf: [*]const u64 = @ptrCast(
            self.file_handles.huffman_row_offsets_mmap_buffer.ptr
            );
        const init_byte_idx = RO_u64_mmap_buf[row_idx];
        var current_byte_idx = init_byte_idx;

        var bits_total: usize = 0;
        const data_buf = self.file_handles.huffman_row_data_mmap_buffer;
        for (0..self.huffman_col_bit_sizes.len) |col_idx| {
            bit_sizes[col_idx] = @truncate(pq.decodeVbyte(
                data_buf.ptr,
                &current_byte_idx,
            ));
            bits_total += bit_sizes[col_idx];
        }
        const bytes_total = try std.math.divCeil(usize, bits_total, 8);
        if (bytes_total * 2 > row_data.items.len) {
            try row_data.resize(allocator, bytes_total * 2);
        }

        current_byte_idx = init_byte_idx - bytes_total;

        var compressed_row_bit_pos:    usize = 0;
        var decompressed_row_byte_idx: usize = 0;
        for (0.., bit_sizes) |col_idx, nbits| {

            const start_byte = current_byte_idx + @divFloor(compressed_row_bit_pos, 8);
            const start_bit  = compressed_row_bit_pos % 8;
            compressed_row_bit_pos += nbits;

            offsets[col_idx].start_pos = @truncate(decompressed_row_byte_idx);
            const field_len = try self.huffman_compressor.decompressOffset(
                data_buf[start_byte..],
                row_data.items[decompressed_row_byte_idx..],
                start_bit,
                nbits,
            );
            offsets[col_idx].field_len = @truncate(field_len);

            decompressed_row_byte_idx += field_len;
        }
    }
};
