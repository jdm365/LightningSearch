const std     = @import("std");
const builtin = @import("builtin");

////////////////////////////////////////////////////////
// https://arrow.apache.org/docs/format/Columnar.html //
////////////////////////////////////////////////////////

const DateTimeUnitEnum = enum {
    SECOND,
    MILLISECOND,
    MICROSECOND,
    NANOSECOND,
};

const KeysSortednessEnum = enum {
    SORTED,
    UNSORTED,
};

const DictOrderednessEnum = enum {
    ORDERED,
    UNORDERED,
};

const ArrowTypeEnum = enum {
    NULL,
    BOOLEAN,
    INT,
    FLOAT,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL,
    DURATION,
    FIXED_SIZE_BINARY,
    BINARY,
    UTF8,
    LARGE_BINARY,
    LARGE_UTF8,
    BINARY_VIEW,
    UTF8_VIEW,
    FIXED_SIZE_LIST,
    LIST,
    LARGE_LIST,
    LIST_VIEW,
    LARGE_LIST_VIEW,
    STRUCT,
    MAP,
    UNION,
    DICTIONARY,
    RUN_END_ENCODED,
};

const ArrowType = struct {
    // Consider unioning some of these later.

    type: ArrowTypeEnum,
    bit_width: ?usize = null,
    signedness: ?bool = null,
    precision: ?usize = null,
    scale: ?usize = null,
    datetime_unit: ?DateTimeUnitEnum = null,
    timezone: ?[]const u8 = null,
    byte_width: ?usize = null,
    value_type: ?ArrowType = null,
    // children:  // TODO: For struct,
    keys_sortedness: ?KeysSortednessEnum,
    dense_union: ?bool,
    // union_type_ids: ?[], // TODO: For union
    dict_index_type: ?ArrowTypeEnum,
    dict_orderedness: ?DictOrderednessEnum,
    run_end_type: ?ArrowTypeEnum,
};

const ArrowBuffer = struct {
    validity_bitmap: ?[]align(64) u8,
    data:             []align(64) u8,
    offsets_buffer:  ?[]align(64) u8,
    arrow_type:      ArrowType,

    pub fn init(
        data: []align(64) u8, 
        validity_bitmap: ?[]align(64) u8,
        offsets_buffer: ?[]align(64) u8,
        arrow_type: ArrowType,
        ) ArrowBuffer {
        return .{
            .data            = data,
            .validity_bitmap = validity_bitmap,
            .offsets_buffer  = offsets_buffer,
            .arrow_type      = arrow_type,
        };
    }

    inline fn isValid(self: *const ArrowBuffer, idx: usize) bool {
        if (self.validity_bitmap) |bitmap| {
            const byte = bitmap[idx >> 3];
            const mask: u8 = 1 << @truncate(idx & 7);
            return (byte & mask) != 0;
        }
        return true;
    }

    inline fn readLe32(bytes: []const u8, idx: usize) u32 {
        return std.mem.bytesToValue(u32, bytes[idx * 4 ..][0..4]);
    }

    inline fn readLe64(bytes: []const u8, idx: usize) u64 {
        return std.mem.bytesToValue(u64, bytes[idx * 8 ..][0..8]);
    }

    pub inline fn getValueIdx(self: *const ArrowBuffer, idx: usize) ?[]u8 {
        // Return pointer to slice containing requested data.
        // It is up to the caller to coerce to desired output type.
        if (!self.isValid(idx)) return null;

        switch (self.arrow_type.type) {
            .NULL => return null,
            .BOOLEAN => {
                const byte_idx = idx >> 3;
                return self.data[byte_idx..byte_idx+1];
            },

            .INT,
            .FLOAT,
            .DECIMAL,
            .DATE,
            .TIME,
            .TIMESTAMP,
            .INTERVAL,
            .DURATION,
            => {
                const bit_width   = self.arrow_type.bit_width.?;
                const byte_width  = bit_width >> 3;
                const byte_offset = byte_width * idx;
                return self.data[byte_offset..][0..byte_width];
            },
            .FIXED_SIZE_BINARY => {
                const byte_width  = self.arrow_type.byte_width.?;
                const byte_offset = byte_width * idx;
                return self.data[byte_offset..][0..byte_width];
            },
            .BINARY,
            .UTF8,
            => {
                const offsets_buffer = self.offsets_buffer.?;
                const start          = readLe32(offsets_buffer, idx);
                const end            = readLe32(offsets_buffer, idx + 1);

                return self.data[start..end];
            },
            .LARGE_BINARY,
            .LARGE_UTF8,
            => {
                const offsets_buffer = self.offsets_buffer.?;
                const start          = readLe64(offsets_buffer, idx);
                const end            = readLe64(offsets_buffer, idx + 1);
                return self.data[start..end];
            },
            else => null,
        }
    }
};

const ArrowDictionary = struct {
};

const ArrowArray = struct {
    dtype:           ArrowType,
    buffers:         []align(64) ArrowBuffer,
    length:          i64,
    null_count:      i64,
    dict:            ?ArrowDictionary,
};
