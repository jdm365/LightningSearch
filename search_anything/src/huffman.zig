const std = @import("std");
const builtin = @import("builtin");


const BUFFER_SIZE: usize = (1 << 20);
var SCRATCH_BUFFER: [1 << 14]u8 = undefined;
const endianness = builtin.cpu.arch.endian();
const big_endian = std.builtin.Endian.big;


const CompressedSize = packed struct(u32) {
    last_block: u1,
    value: u31,

    pub fn readFromFile(file: *std.fs.File) !CompressedSize {
        return @as(CompressedSize, @bitCast(try readValFromFile(u32, file)));
    }
};

pub fn sum(comptime T: type, x: []T) usize {
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

pub fn buildHuffmanTree(
    allocator: std.mem.Allocator,
    buffer: []u8,
    root: *?*HuffmanNode,
) !void {
    // To start, do this on a chunk by chunk level.
    var freqs: [256]usize = undefined;
    @memset(freqs[0..256], 0);

    var idx: usize = 0;
    var idx_0: usize = 0;
    var idx_1: usize = 0;
    var idx_2: usize = 0;
    var idx_3: usize = 0;

    while (idx < BUFFER_SIZE) : (idx += 4) {
        idx_0 = @intCast(buffer[idx]);
        idx_1 = @intCast(buffer[idx + 1]);
        idx_2 = @intCast(buffer[idx + 2]);
        idx_3 = @intCast(buffer[idx + 3]);

        freqs[idx_0] += 1;
        freqs[idx_1] += 1;
        freqs[idx_2] += 1;
        freqs[idx_3] += 1;
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

    root.* = pq.remove();
}

pub fn serializeHuffmanTree(
    root: ?*HuffmanNode,
    stream: *BitStream,
) !void {
    // TODO: Make this buffered. Use SCRATCH_BUFFER.
    if (root) |_root| {
        _ = try stream.output_file.write(
            std.mem.asBytes(&_root.value),
            );
        _ = try stream.output_file.write(
            std.mem.asBytes(&_root.freq),
            );

        try serializeHuffmanTree(_root.left, stream);
        try serializeHuffmanTree(_root.right, stream);
    } else {
        _ = try stream.output_file.write(
            std.mem.asBytes(&@as(i32, @intCast(-1))),
            );
        return;
    }
}

pub fn deserializeHuffmanTree(
    root: *?*HuffmanNode,
    stream: *BitStream,
    allocator: std.mem.Allocator,
) !void {
    const val = try readValFromFile(i32, &stream.input_file);
    if (val == -1) return;
    try stream.input_file.seekBy(-4);

    const new_node = try allocator.create(HuffmanNode);
    const value = try readValFromFile(u8, &stream.input_file);
    const freq  = try readValFromFile(u32, &stream.input_file);
    new_node.* = HuffmanNode{
        .value = value,
        .freq = freq,
        .left = null,
        .right = null,
    };
    root.* = new_node;

    try deserializeHuffmanTree(&new_node.left, stream, allocator);
    try deserializeHuffmanTree(&new_node.right, stream, allocator);
}

fn gatherCodes(
    _root: ?*HuffmanNode,
    codes: *[256]u32,
    code_lengths: *[256]u8,
    current_code: u32,
    current_code_length: u8,
) void {
    if (_root) |root| {
        const left_null  = (root.left == null);
        const right_null = (root.right == null);

        if (left_null and right_null) {
            const value = @as(usize, @intCast(root.value));

            codes[value] = current_code << @intCast(32 - current_code_length);
            code_lengths[value] = current_code_length;
            // std.debug.print("{b:0>32}\n", .{current_code << @intCast(32 - current_code_length)});
            return;
        }

        if (!left_null) {
            gatherCodes(
                root.left,
                codes,
                code_lengths,
                current_code << 1,
                current_code_length + 1,
            );
        }
        if (!right_null) {
            gatherCodes(
                root.right,
                codes,
                code_lengths,
                (current_code << 1) | 1,
                current_code_length + 1,
            );
        }

    } else {
        @panic("Error while gathering codes. Null nodes passed to function.");
    }
}


fn gatherCodesLe(
    _root: ?*HuffmanNode,
    codes: *[256]u64,
    code_lengths: *[256]u8,
    current_code: u32,
    current_code_length: u8,
) void {
    if (_root) |root| {
        const left_null  = (root.left == null);
        const right_null = (root.right == null);

        if (left_null and right_null) {
            const value = @as(usize, @intCast(root.value));

            codes[value] = current_code;
            code_lengths[value] = current_code_length;
            // std.debug.print("{b:0>32}\n", .{current_code << @intCast(32 - current_code_length)});
            return;
        }

        if (!left_null) {
            gatherCodes(
                root.left,
                codes,
                code_lengths,
                current_code << 1,
                current_code_length + 1,
            );
        }
        if (!right_null) {
            gatherCodes(
                root.right,
                codes,
                code_lengths,
                (current_code << 1) | 1,
                current_code_length + 1,
            );
        }

    } else {
        @panic("Error while gathering codes. Null nodes passed to function.");
    }
}

fn huffmanCompress(
    root: *?*HuffmanNode,
    stream: *BitStream,
    allocator: std.mem.Allocator,
) !void {
    try buildHuffmanTree(allocator, stream.input_buffer, root);
    try serializeHuffmanTree(root.*, stream);

    // Build code table.
    var codes: [256]u32 = undefined;
    var code_lengths: [256]u8 = undefined;
    @memset(&codes, 0);
    @memset(&code_lengths, 0);
    @memset(stream.compression_buffer, 0);

    gatherCodes(
        root.*,
        &codes,
        &code_lengths,
        0,
        0,
    );

    // TODO: Try doing 4 elements at a time. Maybe go from u32 -> u64 or larger.
    var ubyte: usize = 0;
    for (stream.input_buffer[0..stream.input_buffer_size]) |byte| {
        ubyte = @intCast(byte);
        const nbits = code_lengths[ubyte];

        const bit_idx = stream.compression_buffer_bit_idx;
        var code = codes[ubyte] >> @intCast(bit_idx);

        code |= std.mem.readInt(
            u32,
            stream.compression_buffer[stream.compression_buffer_idx..stream.compression_buffer_idx+4][0..4],
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


fn huffmanCompressLe(
    root: *?*HuffmanNode,
    stream: *BitStream,
    allocator: std.mem.Allocator,
) !void {
    try buildHuffmanTree(allocator, stream.input_buffer, root);
    try serializeHuffmanTree(root.*, stream);

    // Build code table.
    var codes: [256]u64 = undefined;
    var code_lengths: [256]u8 = undefined;
    @memset(&codes, 0);
    @memset(&code_lengths, 0);
    @memset(stream.compression_buffer, 0);

    gatherCodesLe(
        root.*,
        &codes,
        &code_lengths,
        0,
        0,
    );

    // Try little endian style. Fill buffer from right to left to avoid endian swapping.
    stream.compression_buffer_idx = BUFFER_SIZE - 1;
    var cb_ptr = @as(*u64, @ptrCast(&stream.compression_buffer[stream.compression_buffer_idx]));

    var ubyte: usize = 0;
    for (stream.input_buffer[0..stream.input_buffer_size]) |byte| {
        ubyte = @intCast(byte);
        const nbits = code_lengths[ubyte];

        if (nbits - stream.compression_buffer_bit_idx > 0) {
            cb_ptr.* |= (codes[ubyte] << (nbits - stream.compression_buffer_bit_idx));
        } else {
            cb_ptr.* |= (codes[ubyte] >> -(nbits - stream.compression_buffer_bit_idx));
        }
        // const code = codes[ubyte] << @intCast(stream.compression_buffer_bit_idx);
        // cb_ptr.* |= code;

        stream.compression_buffer_bit_idx += nbits;
        if (stream.compression_buffer_bit_idx > 64) {
            // TODO: need to branch on shift sign.
            cb_ptr = @ptrFromInt(@intFromPtr(cb_ptr) - 8);
            stream.compression_buffer_bit_idx %= 64;

            if (nbits - stream.compression_buffer_bit_idx > 0) {
                cb_ptr.* = (codes[ubyte] << (nbits - stream.compression_buffer_bit_idx));
            } else {
                cb_ptr.* = (codes[ubyte] >> -(nbits - stream.compression_buffer_bit_idx));
            }
        }
    }

    try stream.flushChunk(true);
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


test "compression" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var filename: []const u8 = "../../data/enwik8";

    // TMP
    // _decompress = true;
    // filename = "../../data/declaration_of_independence.txt.fse";

    var stream = try BitStream.init(filename, arena.allocator(), _decompress);
    defer stream.deinit();

    try stream.compress(arena.allocator());
}
