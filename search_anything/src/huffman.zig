const std = @import("std");
const builtin = @import("builtin");


const BUFFER_SIZE: usize = (1 << 20);
var SCRATCH_BUFFER: [1 << 14]u8 = undefined;
const endianness = builtin.cpu.arch.endian();
const big_endian = std.builtin.Endian.big;

inline fn readValFromFile(
    comptime T: type,
    file: *std.fs.File,
) !T {
    var _val: [@sizeOf(T)]u8 = undefined;
    _ = try file.read(std.mem.asBytes(&_val));
    return std.mem.readInt(T, &_val, endianness);
}

pub fn printBits(comptime T: type, value: T) void {
    var num_bits: T = @sizeOf(T) * 8;
    num_bits += @divFloor(num_bits, 8) - 1;
    var cntr: usize = 0;

    for (0..num_bits) |_idx| {
        const idx = num_bits - _idx - 1;

        if (cntr == 8) {
            SCRATCH_BUFFER[idx] = ' ';
            cntr = 0;
            continue;
        }

        if ((value & (@as(T, @intCast(1)) << @truncate(idx))) != 0) {
            SCRATCH_BUFFER[idx] = '1';
        } else {
            SCRATCH_BUFFER[idx] = '0';
        }
        cntr += 1;
    }
    std.debug.print("{s}\n", .{SCRATCH_BUFFER[0..num_bits]});
}


pub inline fn sum(comptime T: type, x: []T) usize {
    var _sum: usize = 0;
    for (x) |val| {
        _sum += @intCast(val);
    }
    return _sum;
}

const HuffmanNode = packed struct(u64) {
    freq: u32,
    value: u8,
    left_idx: u12,
    right_idx: u12,


    pub fn printHuffmanTree(
        self: *HuffmanNode, 
        current_idx: usize,
        huffman_nodes: [512]HuffmanNode,
        ) void {
        var null_count: usize = 0;

        if (self.left_idx != 0) {
            const left_node  = huffman_nodes[self.left_idx];

            SCRATCH_BUFFER[current_idx] = '0';
            &left_node.printHuffmanTree(current_idx + 1, huffman_nodes);
            null_count += 1;
        }
        if (self.right_idx != 0) {
            const right_node = huffman_nodes[self.right_idx];

            SCRATCH_BUFFER[current_idx] = '1';
            &right_node.printHuffmanTree(current_idx + 1, huffman_nodes);
            null_count += 1;
        }

        if (null_count == 0) {
            std.debug.print("{c}: {s}\n", .{self.value, SCRATCH_BUFFER[0..current_idx]});
        }
    }
};

fn lessThan(_: void, a: HuffmanNode, b: HuffmanNode) std.math.Order {
    return std.math.order(a.freq, b.freq);
}

inline fn writeBits(
    output_buffer: [*]u32,
    bit_offset: usize,
    code: u32,
    code_length: u8,
) !void {
    const shifted_code_0 = code >> @truncate(bit_offset);

    output_buffer[0] |= shifted_code_0;

    if (@as(u8, @truncate(code_length)) + bit_offset > 32) {
        const shift_val: u5 = @truncate((32 - bit_offset) % 32);
        output_buffer[1] |= code << shift_val;
    }
}


pub const HuffmanCompressor = struct {
    huffman_nodes: [512]HuffmanNode,
    codes: [256]u32,
    code_lengths: [256]u8,
    root_node_idx: u12,

    lookup_table: [4096]u8,
    lookup_table_lengths: [4096]u8,

    pub fn init() HuffmanCompressor {
        return HuffmanCompressor{
            .huffman_nodes = undefined,
            .codes = [_]u32{0} ** 256,
            .code_lengths = [_]u8{0} ** 256,
            .root_node_idx = 0,

            .lookup_table = [_]u8{0} ** 4096,
            .lookup_table_lengths = [_]u8{0} ** 4096,
        };
    }

    fn buildHuffmanTree(
        self: *HuffmanCompressor,
        allocator: std.mem.Allocator,
        buffer: []u8,
    ) !void {
        var freqs: [256]usize = undefined;
        @memset(freqs[0..256], 0);

        for (buffer) |byte| {
            freqs[byte] += 1;
        }

        var pq = std.PriorityQueue(HuffmanNode, void, lessThan).init(allocator, {});
        defer pq.deinit();

        var node_idx: usize = 1;

        for (0.., freqs[0..256]) |i, freq| {
            if (freq > 0) {
                const new_node = HuffmanNode{
                    .freq = @truncate(freq),
                    .value = @truncate(i),
                    .left_idx = 0,
                    .right_idx = 0,
                };
                try pq.add(new_node);
            }
        }

        while (pq.count() > 1) {
            const left  = pq.remove();
            const right = pq.remove();

            self.huffman_nodes[node_idx] = left; node_idx += 1;
            self.huffman_nodes[node_idx] = right; node_idx += 1;

            const parent = HuffmanNode{
                .value = 0,
                .freq = left.freq + right.freq,
                .left_idx = @truncate(node_idx - 2),
                .right_idx = @truncate(node_idx - 1),
            };

            try pq.add(parent);
        }

        self.huffman_nodes[node_idx] = pq.remove(); node_idx += 1;
        self.root_node_idx = @truncate(node_idx - 1);
        self.gatherCodes(self.root_node_idx, 0, 0);
        self.buildLookupTable();
    }

    fn buildHuffmanTreeGivenFreqs(
        self: *HuffmanCompressor,
        allocator: std.mem.Allocator,
        freqs: [256]usize,
    ) !void {
        var pq = std.PriorityQueue(HuffmanNode, void, lessThan).init(allocator, {});
        defer pq.deinit();

        var node_idx: usize = 1;

        for (0.., freqs[0..256]) |i, freq| {
            if (freq > 0) {
                const new_node = HuffmanNode{
                    .freq = @truncate(freq),
                    .value = @truncate(i),
                    .left_idx = 0,
                    .right_idx = 0,
                };
                try pq.add(new_node);
            }
        }

        while (pq.count() > 1) {
            const left  = pq.remove();
            const right = pq.remove();

            self.huffman_nodes[node_idx] = left; node_idx += 1;
            self.huffman_nodes[node_idx] = right; node_idx += 1;

            const parent = HuffmanNode{
                .value = 0,
                .freq = left.freq + right.freq,
                .left_idx = @truncate(node_idx - 2),
                .right_idx = @truncate(node_idx - 1),
            };

            try pq.add(parent);
        }

        self.huffman_nodes[node_idx] = pq.remove(); node_idx += 1;
        self.root_node_idx = @truncate(node_idx - 1);
        self.gatherCodes(self.root_node_idx, 0, 0);
        self.buildLookupTable();
    }


    fn gatherCodes(
        self: *HuffmanCompressor,
        current_node_idx: u12,
        current_code: u32,
        current_code_length: u8,
    ) void {

        if (current_node_idx != 0) {
            const node = self.huffman_nodes[current_node_idx];

            const left_null  = (node.left_idx == 0);
            const right_null = (node.right_idx == 0);


            if (left_null and right_null) {
                const value = @as(usize, @intCast(node.value));

                const shift_val: u5 = @truncate((32 - current_code_length) % 32);
                self.codes[value] = current_code << shift_val;
                self.code_lengths[value] = current_code_length;

                std.debug.assert(self.code_lengths[value] <= 32);
                return;
            }

            if (!left_null) {
                self.gatherCodes(
                    node.left_idx,
                    current_code << 1,
                    current_code_length + 1,
                );
            }
            if (!right_null) {
                self.gatherCodes(
                    node.right_idx,
                    (current_code << 1) | 1,
                    current_code_length + 1,
                );
            }

        } else {
            @panic("Error while gathering codes. Null nodes passed to function.");
        }
    }

    fn buildLookupTable(self: *HuffmanCompressor) void {
        @memset(self.lookup_table[0..], 0);
        @memset(self.lookup_table_lengths[0..], 0);

        for (0.., self.codes) |idx, code| {
            if (self.code_lengths[idx] == 0) continue;
            if (self.code_lengths[idx] > 12) continue;

            const mask = (@as(u64, 1) << @truncate(@as(u8, 12) - self.code_lengths[idx])) - 1;

            const min_code = code >> 20;
            const max_code = min_code | mask;

            for (min_code..max_code + 1) |_code| {
                self.lookup_table[_code] = @truncate(idx);
                self.lookup_table_lengths[_code] = self.code_lengths[idx];
            }
        }
    }

    fn compress(
        self: *HuffmanCompressor,
        input_buffer: []u8,
        output_buffer: []u8,
    ) !usize {
        if (self.root_node_idx == 0) {
            @branchHint(.cold);
            return error.HuffmanTreeNotBuilt;
        }

        @memset(output_buffer, 0);

        var output_buffer_bit_idx: usize = 0;

        const output_buffer_u32 = @as(
            [*]u32,
            @ptrCast(@alignCast(output_buffer.ptr)),
        );

        for (input_buffer) |byte| {
            const nbits = self.code_lengths[byte];

            try writeBits(
                output_buffer_u32[@divFloor(output_buffer_bit_idx, 32)..],
                output_buffer_bit_idx % 32,
                self.codes[byte],
                nbits,
            );

            output_buffer_bit_idx += nbits;
        }

        const compressed_size = try std.math.divCeil(usize, output_buffer_bit_idx, 8);
        const last_word_idx   = try std.math.divCeil(usize, output_buffer_bit_idx, 32);

        for (0.., output_buffer_u32[0..last_word_idx]) |idx, word| {
            output_buffer_u32[idx] = @byteSwap(word);
        }

        return compressed_size;
    }

    fn decompressBase(
        self: *HuffmanCompressor,
        compressed_buffer: []u8,
        decompressed_buffer: []u8,
    ) !usize {
        if (self.root_node_idx == 0) {
            @branchHint(.cold);
            return error.HuffmanTreeNotBuilt;
        }

        var decompressed_buffer_idx:   usize = 0;
        var compressed_buffer_bit_idx: usize = 0;

        var current_node_idx = self.root_node_idx;

        while (decompressed_buffer_idx < decompressed_buffer.len) {
            const node = self.huffman_nodes[current_node_idx];

            if (node.left_idx == 0 and node.right_idx == 0) {
                decompressed_buffer[decompressed_buffer_idx] = node.value;
                decompressed_buffer_idx += 1;
                current_node_idx = self.root_node_idx;
                continue;
            }

            const byte_idx    = compressed_buffer_bit_idx / 8;
            const bit_in_byte = compressed_buffer_bit_idx % 8;

            if (byte_idx >= compressed_buffer.len) {
                if (decompressed_buffer_idx < decompressed_buffer.len) {
                    return error.InsufficientInput;
                }
                break;
            }

            const byte = compressed_buffer[byte_idx];
            const bit = (byte >> @truncate(7 - bit_in_byte)) & 1;

            compressed_buffer_bit_idx += 1;

            if (bit == 0) {
                current_node_idx = node.left_idx;
                if (current_node_idx == 0) {
                    return error.InvalidHuffmanCode;
                }
            } else {
                current_node_idx = node.right_idx;
                if (current_node_idx == 0) {
                    return error.InvalidHuffmanCode;
                }
            }
        }

        const final_node = self.huffman_nodes[current_node_idx];
        if (
            (decompressed_buffer_idx < decompressed_buffer.len) and 
            (final_node.left_idx != 0 or final_node.right_idx != 0)
            ) {
             return error.InvalidHuffmanCode;
        }

        return decompressed_buffer_idx;
    }

    fn decompress(
        self: *HuffmanCompressor,
        compressed_buffer: []u8,
        decompressed_buffer: []u8,
    ) !usize {
        if (self.root_node_idx == 0) {
            @branchHint(.cold);
            return error.HuffmanTreeNotBuilt;
        }

        var decompressed_buffer_idx:   usize = 0;
        var compressed_buffer_bit_idx: usize = 0;

        while (decompressed_buffer_idx < decompressed_buffer.len) {
            const byte_idx = @divFloor(compressed_buffer_bit_idx, 8);
            const bit_idx  = compressed_buffer_bit_idx % 8;

            const slice = compressed_buffer[byte_idx..@min(byte_idx + 4, compressed_buffer.len)];
            var current_word = std.mem.readInt(u32, slice[0..4], big_endian);
            current_word = (current_word >> @truncate(20 - bit_idx)) & 0xFFF;

            const length = self.lookup_table_lengths[current_word];
            const symbol = self.lookup_table[current_word];

            if (length > 0) {
                @branchHint(.likely);

                decompressed_buffer[decompressed_buffer_idx] = symbol;
                decompressed_buffer_idx += 1;

                compressed_buffer_bit_idx += length;
                continue;
            }
            std.debug.assert(length == 0);

            var current_node_idx = self.root_node_idx;

            while (true) {
                const node = self.huffman_nodes[current_node_idx];

                if ((node.left_idx == 0) and (node.right_idx == 0)) {
                    decompressed_buffer[decompressed_buffer_idx] = node.value;
                    decompressed_buffer_idx += 1;
                    break;
                }

                const _byte_idx = @divFloor(compressed_buffer_bit_idx, 8);
                const _bit_idx  = compressed_buffer_bit_idx % 8;

                const byte = compressed_buffer[_byte_idx];
                const bit = (byte >> @truncate(7 - _bit_idx)) & 1;

                compressed_buffer_bit_idx += 1;

                if (bit == 0) {
                    current_node_idx = node.left_idx;
                    if (current_node_idx == 0) return error.InvalidHuffmanCode;
                } else {
                    current_node_idx = node.right_idx;
                    if (current_node_idx == 0) return error.InvalidHuffmanCode;
                }
            }
        }
        return decompressed_buffer_idx;
    }

};


test "compression" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const filename: []const u8 = "../data/enwik8";
    const input_file = try std.fs.cwd().openFile(filename, .{});
    defer input_file.close();

    const file_size = try input_file.getEndPos();

    const input_buffer = try arena.allocator().alloc(u8, file_size);

    _ = try input_file.readAll(input_buffer);

    var compressor = HuffmanCompressor.init();
    try compressor.buildHuffmanTree(arena.allocator(), input_buffer);

    const output_buffer = try arena.allocator().alloc(u8, file_size);

    const init = std.time.microTimestamp();
    const compressed_size = try compressor.compress(input_buffer, output_buffer);
    const final = std.time.microTimestamp();
    const elapsed = @as(u64, @intCast(final - init));
    const mb_s = file_size / elapsed;

    std.debug.print("Original size:   {d}\n", .{file_size});
    std.debug.print("Compressed size: {d}\n", .{compressed_size});
    std.debug.print("MB/s:            {d}\n", .{mb_s});

    const decompressed_buffer = try arena.allocator().alloc(u8, file_size);

    const init2 = std.time.microTimestamp();
    // const decompressed_size = try compressor.decompressBase(
    const decompressed_size = try compressor.decompress(
        output_buffer, 
        decompressed_buffer,
        );
    const final2 = std.time.microTimestamp();
    const elapsed2 = @as(u64, @intCast(final2 - init2));
    const mb_s2 = file_size / elapsed2;

    std.debug.assert(decompressed_size == file_size);

    std.debug.print("MB/s:            {d}\n", .{mb_s2});
    std.debug.print("Decompressed size: {d}\n", .{decompressed_size});

    for (0..file_size) |i| {
        if (input_buffer[i] != decompressed_buffer[i]) {
            std.debug.print("Mismatch at index {d}\n", .{i});
            std.debug.print("{c} != {c}\n", .{input_buffer[i], decompressed_buffer[i]});

            std.debug.print("INPUT:        {s}", .{input_buffer[0..i]}); std.debug.print(" | {s}\n", .{input_buffer[i..i+64]});
            std.debug.print("DECOMPRESSED: {s}", .{decompressed_buffer[0..i]}); std.debug.print(" | {s}\n", .{decompressed_buffer[i..i+64]});
            return error.InvalidHuffmanCode;
        }
    }
}
