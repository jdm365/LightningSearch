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
    output_buffer: []u8,
    bit_offset: usize,
    code: u32,
    code_length: u8,
) !void {
    const bytes_required = try std.math.divCeil(usize, @as(usize, @intCast(code_length)) + bit_offset, 8);
    const shifted_code_0 = code >> @as(u5, @truncate(bit_offset));

    const output_buffer_u32 = @as(
        [*]u32,
        @ptrCast(@alignCast(output_buffer.ptr)),
    );
    output_buffer_u32[0] |= shifted_code_0;

    if (bytes_required >= 4) {
        @branchHint(.unlikely);

        const shift_val: u5 = @as(u5, @truncate((32 - bit_offset) % 32));
        const shifted_code_1: u32 = @truncate((
            @as(u64, @intCast(code)) << shift_val
        ) & 0x00000000FFFFFFFF);
        output_buffer_u32[1] |= shifted_code_1;
    }
}


pub const HuffmanCompressor = struct {
    huffman_nodes: [512]HuffmanNode,
    codes: [256]u32,
    code_lengths: [256]u8,

    pub fn buildHuffmanTree(
        self: *HuffmanCompressor,
        allocator: std.mem.Allocator,
        buffer: []u8,
    ) !void {
        // To start, do this on a chunk by chunk level.
        var freqs: [256]usize = undefined;
        @memset(freqs[0..256], 0);

        for (buffer) |byte| {
            freqs[byte] += 1;
        }

        var pq = std.PriorityQueue(HuffmanNode, void, lessThan).init(allocator, {});
        defer pq.deinit();

        var num_nodes: usize = 0;

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

            self.huffman_nodes[num_nodes] = left; num_nodes += 1;
            self.huffman_nodes[num_nodes] = right; num_nodes += 1;

            const parent = HuffmanNode{
                .value = 0,
                .freq = left.freq + right.freq,
                .left_idx = @truncate(num_nodes - 1),
                .right_idx = @truncate(num_nodes),
            };

            try pq.add(parent);
        }

        self.huffman_nodes[num_nodes] = pq.remove(); num_nodes += 1;
        self.gatherCodes(@truncate(num_nodes - 1), 0, 0);
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

            std.debug.print(
                "Node: {d} left: {d} right: {d} code: {b} len: {d}\n",
                .{
                    current_node_idx,
                    node.left_idx,
                    node.right_idx,
                    current_code,
                    current_code_length,
                },
            );

            if (left_null and right_null) {
                const value = @as(usize, @intCast(node.value));

                const shift_val: u5 = @as(u5, @truncate((32 - current_code_length) % 32));
                self.codes[value] = current_code << shift_val;
                self.code_lengths[value] = current_code_length;
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

    fn huffmanCompress(
        self: *HuffmanCompressor,
        input_buffer: []u8,
        output_buffer: []u8,
    ) !usize {
        var input_buffer_bit_idx: usize = 0;

        std.debug.print("Codes:        {any}\n", .{self.codes});
        std.debug.print("Code lengths: {any}\n", .{self.code_lengths});
        @breakpoint();

        // TODO: Try doing 4 elements at a time. Maybe go from u32 -> u64 or larger.
        var ubyte: usize = 0;
        for (input_buffer) |byte| {
            ubyte = @intCast(byte);
            const nbits = self.code_lengths[ubyte];

            const bit_idx = input_buffer_bit_idx;
            const code = self.codes[ubyte] >> @intCast(bit_idx);

            try writeBits(
                output_buffer,
                input_buffer_bit_idx,
                code,
                nbits,
            );

            input_buffer_bit_idx += nbits;
        }

        return try std.math.divCeil(usize, input_buffer_bit_idx, 8);
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

    var compressor = HuffmanCompressor{
        .huffman_nodes = undefined,
        .codes = [_]u32{0} ** 256,
        .code_lengths = [_]u8{0} ** 256,
    };
    try compressor.buildHuffmanTree(arena.allocator(), input_buffer);

    const output_buffer = try arena.allocator().alloc(u8, file_size);

    const compressed_size = try compressor.huffmanCompress(input_buffer, output_buffer);

    std.debug.print("Original size:   {d}\n", .{file_size});
    std.debug.print("Compressed size: {d}\n", .{compressed_size});
}
