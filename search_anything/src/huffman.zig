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

const HuffmanNode = struct {
    value: u8,
    freq: u32,
    left:  ?*HuffmanNode,
    right: ?*HuffmanNode,


    pub fn printHuffmanTree(self: *const HuffmanNode, current_idx: usize) void {
        var null_count: usize = 0;

        if (self.left) |l| {
            SCRATCH_BUFFER[current_idx] = '0';
            l.printHuffmanTree(current_idx + 1);
            null_count += 1;
        }
        if (self.right) |r| {
            SCRATCH_BUFFER[current_idx] = '1';
            r.printHuffmanTree(current_idx + 1);
            null_count += 1;
        }

        if (null_count == 0) {
            std.debug.print("{c}: {s}\n", .{self.value, SCRATCH_BUFFER[0..current_idx]});
        }
    }
};

fn lessThan(_: void, a: *HuffmanNode, b: *HuffmanNode) std.math.Order {
    return std.math.order(a.freq, b.freq);
}

inline fn writeBits(
    output_buffer: []u8,
    bit_offset: usize,
    code: u32,
    code_length: u8,
) void {
    const bytes_required = try std.math.divCeil(code_length + bit_offset, 8);
    const shifted_code_0 = code >> bit_offset;

    const output_buffer_u32 = @as(
        [*]u32,
        @ptrCast(@alignCast(&output_buffer)),
    );
    output_buffer_u32[0] |= shifted_code_0;

    if (bytes_required >= 4) {
        @branchHint(.unlikely);

        const shifted_code_1: u32 = @truncate((
            @as(u64, @intCast(code)) << (32 - bit_offset)
        ) & 0x00000000FFFFFFFF);
        output_buffer_u32[1] |= shifted_code_1;
    }
}

fn huffmanDecompress(
    root: *HuffmanNode,
    input_buffer: []u8,
    output_buffer: []u8,
    allocator: std.mem.Allocator,
) !bool {
    var byte_idx: usize = 0;
    var bit_idx:  usize = 0;

    if (root.*) |_root| {
        while (byte_idx < @as(usize, @intCast(compressed_size.value))) {
            var node = _root;
            while ((node.left != null) and (node.right != null)) {
                if ((stream.input_buffer[byte_idx] & (@as(u8, 1) << @intCast(7 - bit_idx))) != 0) {
                    if (node.right) |right| {
                        node = right;
                    } else {
                        return error.InvalidHuffmanTree;
                    }
                } else {
                    if (node.left) |left| {
                        node = left;
                    } else {
                        return error.InvalidHuffmanTree;
                    }
                }
                bit_idx += 1;
                byte_idx += @divFloor(bit_idx, 8);
                bit_idx %= 8;
            }

            stream.compression_buffer[stream.compression_buffer_idx] = node.value;
            stream.compression_buffer_idx += 1;
        }
    } else {
        return error.InvalidHuffmanTree;
    }

    try stream.flushChunk(false);

    return done;
}

pub const HuffmanCompressor = struct {
    root: ?*HuffmanNode,
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

        var pq = std.PriorityQueue(*HuffmanNode, void, lessThan).init(allocator, {});
        defer pq.deinit();

        for (0.., freqs[0..256]) |i, freq| {
            if (freq > 0) {
                const new_node: *HuffmanNode = try allocator.create(HuffmanNode);
                new_node.* = HuffmanNode{
                    .value = @truncate(i),
                    .freq = @truncate(freq),
                    .left = null,
                    .right = null,
                };
                try pq.add(new_node);
            }
        }

        while (pq.count() > 1) {
            const left  = pq.remove();
            const right = pq.remove();

            const parent: *HuffmanNode = try allocator.create(HuffmanNode);
            parent.* = HuffmanNode{
                .value = 0,
                .freq = left.freq + right.freq,
                .left = left,
                .right = right,
            };

            try pq.add(parent);
        }

        self.root.?.* = pq.remove();
        self.gatherCodes(self.root, 0, 0);
    }


    fn gatherCodes(
        self: *HuffmanCompressor,
        current_node: ?*HuffmanNode,
        current_code: u32,
        current_code_length: u8,
    ) void {

        if (current_node) |root| {
            const left_null  = (root.left == null);
            const right_null = (root.right == null);

            if (left_null and right_null) {
                const value = @as(usize, @intCast(root.value));

                self.codes[value] = current_code << @intCast(32 - current_code_length);
                self.code_lengths[value] = current_code_length;
                return;
            }

            if (!left_null) {
                self.gatherCodes(
                    root.left,
                    current_code << 1,
                    current_code_length + 1,
                );
            }
            if (!right_null) {
                self.gatherCodes(
                    root.right,
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
    ) !void {
        std.debug.assert(self.root != null);

        const input_buffer_bit_idx: usize = 0;

        // TODO: Try doing 4 elements at a time. Maybe go from u32 -> u64 or larger.
        var ubyte: usize = 0;
        for (input_buffer) |byte| {
            ubyte = @intCast(byte);
            const nbits = self.code_lengths[ubyte];

            const bit_idx = input_buffer_bit_idx;
            var code = self.codes[ubyte] >> @intCast(bit_idx);

            code |= std.mem.readInt(
                u32,
                output_buffer[compression_buffer_idx..stream.compression_buffer_idx+4][0..4],
                big_endian,
            );

            code = @byteSwap(code);
            @memcpy(
                stream.compression_buffer[stream.compression_buffer_idx..stream.compression_buffer_idx+4], 
                std.mem.asBytes(&code),
                );

            stream.compression_buffer_bit_idx += nbits;
            stream.compression_buffer_idx += @divFloor(stream.compression_buffer_bit_idx, 8);
            stream.compression_buffer_bit_idx %= 8;
        }

        try stream.flushChunk(true);
    }

};


test "compression" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    // var filename: []const u8 = "../../data/enwik8";

    // TMP
    // _decompress = true;
    // filename = "../../data/declaration_of_independence.txt.fse";
}
