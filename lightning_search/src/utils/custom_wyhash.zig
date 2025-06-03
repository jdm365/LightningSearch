const std = @import("std");

const stringToUpper = @import("string_utils.zig").stringToUpper;

var LOWER_U64: u64 = 0x20_20_20_20_20_20_20_20; // 64-bit lower mask for char_original | 0x20

pub const CaseInsensitiveWyhash = struct {
    const secret = [_]u64{
        0xa0761d6478bd642f,
        0xe7037ed1a0b428db,
        0x8ebc6af09c88c6e3,
        0x589965cc75374cc3,
    };

    a: u64,
    b: u64,
    state: [3]u64,
    total_len: usize,

    buf: [48]u8,
    buf_len: usize,

    pub fn init(seed: u64) CaseInsensitiveWyhash {
        var self = CaseInsensitiveWyhash{
            .a = undefined,
            .b = undefined,
            .state = undefined,
            .total_len = 0,
            .buf = undefined,
            .buf_len = 0,
        };

        self.state[0] = seed ^ mix(seed ^ secret[0], secret[1]);
        self.state[1] = self.state[0];
        self.state[2] = self.state[0];
        return self;
    }

    // This is subtly different from other hash function update calls. CaseInsensitiveWyhash requires the last
    // full 48-byte block to be run through final1 if is exactly aligned to 48-bytes.
    pub fn update(self: *CaseInsensitiveWyhash, input: []const u8) void {
        self.total_len += input.len;

        if (input.len <= 48 - self.buf_len) {
            @memcpy(self.buf[self.buf_len..][0..input.len], input);
            self.buf_len += input.len;
            return;
        }

        var i: usize = 0;

        if (self.buf_len > 0) {
            i = 48 - self.buf_len;
            @memcpy(self.buf[self.buf_len..][0..i], input[0..i]);
            self.round(&self.buf);
            self.buf_len = 0;
        }

        while (i + 48 < input.len) : (i += 48) {
            self.round(input[i..][0..48]);
        }

        const remaining_bytes = input[i..];
        if (remaining_bytes.len < 16 and i >= 48) {
            const rem = 16 - remaining_bytes.len;
            @memcpy(self.buf[self.buf.len - rem ..], input[i - rem .. i]);
        }
        @memcpy(self.buf[0..remaining_bytes.len], remaining_bytes);
        self.buf_len = remaining_bytes.len;
    }

    pub fn final(self: *CaseInsensitiveWyhash) u64 {
        var input: []const u8 = self.buf[0..self.buf_len];
        var newSelf = self.shallowCopy(); // ensure idempotency

        if (self.total_len <= 16) {
            newSelf.smallKey(input);
        } else {
            var offset: usize = 0;
            var scratch: [16]u8 = undefined;
            if (self.buf_len < 16) {
                const rem = 16 - self.buf_len;
                @memcpy(scratch[0..rem], self.buf[self.buf.len - rem ..][0..rem]);
                @memcpy(scratch[rem..][0..self.buf_len], self.buf[0..self.buf_len]);

                // Same as input but with additional bytes preceding start in case of a short buffer
                input = &scratch;
                offset = rem;
            }

            newSelf.final0();
            newSelf.final1(input, offset);
        }

        return newSelf.final2();
    }

    // Copies the core wyhash state but not any internal buffers.
    inline fn shallowCopy(self: *CaseInsensitiveWyhash) CaseInsensitiveWyhash {
        return .{
            .a = self.a,
            .b = self.b,
            .state = self.state,
            .total_len = self.total_len,
            .buf = undefined,
            .buf_len = undefined,
        };
    }

    inline fn smallKey(self: *CaseInsensitiveWyhash, input: []const u8) void {
        std.debug.assert(input.len <= 16);

        if (input.len >= 4) {
            const end = input.len - 4;
            const quarter = (input.len >> 3) << 2;

            self.a = (read(4, input[0..]) << 32) | read(4, input[quarter..]);
            self.b = (read(4, input[end..]) << 32) | read(4, input[end - quarter ..]);
        } else if (input.len > 0) {
            self.a = (@as(u64, input[0]) << 16) | (@as(u64, input[input.len >> 1]) << 8) | input[input.len - 1];
            self.b = 0;
        } else {
            self.a = 0;
            self.b = 0;
        }
        stringToUpper(
            @ptrCast(&self.a),
            8,
        );
        stringToUpper(
            @ptrCast(&self.b),
            8,
        );
    }

    inline fn round(self: *CaseInsensitiveWyhash, input: *const [48]u8) void {
        inline for (0..3) |i| {
            var a = read(8, input[8 * (2 * i) ..]);
            var b = read(8, input[8 * (2 * i + 1) ..]);
            stringToUpper(
                @ptrCast(&a),
                8,
            );
            stringToUpper(
                @ptrCast(&b),
                8,
            );
            self.state[i] = mix(a ^ secret[i + 1], b ^ self.state[i]);
        }
    }

    inline fn read(comptime bytes: usize, data: []const u8) u64 {
        std.debug.assert(bytes <= 8);
        const T = std.meta.Int(.unsigned, 8 * bytes);
        return @as(u64, std.mem.readInt(T, data[0..bytes], .little));
    }

    inline fn mum(a: *u64, b: *u64) void {
        const x = @as(u128, a.*) *% b.*;
        a.* = @as(u64, @truncate(x));
        b.* = @as(u64, @truncate(x >> 64));
    }

    inline fn mix(a_: u64, b_: u64) u64 {
        var a = a_;
        var b = b_;
        mum(&a, &b);
        return a ^ b;
    }

    inline fn final0(self: *CaseInsensitiveWyhash) void {
        self.state[0] ^= self.state[1] ^ self.state[2];
    }

    // input_lb must be at least 16-bytes long (in shorter key cases the smallKey function will be
    // used instead). We use an index into a slice to for comptime processing as opposed to if we
    // used pointers.
    inline fn final1(self: *CaseInsensitiveWyhash, input_lb: []const u8, start_pos: usize) void {
        std.debug.assert(input_lb.len >= 16);
        std.debug.assert(input_lb.len - start_pos <= 48);
        const input = input_lb[start_pos..];

        var i: usize = 0;
        while (i + 16 < input.len) : (i += 16) {
            var _a = read(8, input[i..]);
            var _b = read(8, input[i + 8 ..]);
            stringToUpper(
                @ptrCast(&_a),
                8,
            );
            stringToUpper(
                @ptrCast(&_b),
                8,
            );
            self.state[0] = mix(_a ^ secret[1], _b ^ self.state[0]);
        }

        self.a = read(8, input_lb[input_lb.len - 16 ..][0..8]);
        self.b = read(8, input_lb[input_lb.len - 8 ..][0..8]);
        stringToUpper(
            @ptrCast(&self.a),
            8,
        );
        stringToUpper(
            @ptrCast(&self.b),
            8,
        );
    }

    inline fn final2(self: *CaseInsensitiveWyhash) u64 {
        self.a ^= secret[1];
        self.b ^= self.state[0];
        mum(&self.a, &self.b);
        return mix(self.a ^ secret[0] ^ self.total_len, self.b ^ secret[1]);
    }

    pub inline fn hash(seed: u64, input: []const u8) u64 {
        var self = CaseInsensitiveWyhash.init(seed);

        if (input.len <= 16) {
            self.smallKey(input);
        } else {
            var i: usize = 0;
            if (input.len >= 48) {
                while (i + 48 < input.len) : (i += 48) {
                    self.round(input[i..][0..48]);
                }
                self.final0();
            }
            self.final1(input, i);
        }

        self.total_len = input.len;
        return self.final2();
    }
};

pub const CaseInsensitiveWyhashContext = struct {
    pub inline fn eql(_: CaseInsensitiveWyhashContext, a: []const u8, b: []const u8) bool {
        if (a.len != b.len) return false;
        for (0..a.len) |i| {
            if (std.ascii.toLower(a[i]) != std.ascii.toLower(b[i])) {
                return false;
            }
        }
        return true;
    }

    pub inline fn hash(_: CaseInsensitiveWyhashContext, x: []const u8) u64 {
        return CaseInsensitiveWyhash.hash(42, x);
    }
};
