const std = @import("std");
const string_utils = @import("string_utils.zig");


pub inline fn nextPoint(buffer: []const u8, byte_idx: *usize) void {
    while (
        (buffer[byte_idx.*] == ' ') 
            or 
        (buffer[byte_idx.*] == '\n')) {
        byte_idx.* += 1;
    }
}

pub inline fn _iterFieldJSON(buffer: []const u8, byte_idx: *usize) !void {
    // Iter to start of value.
    nextPoint(buffer, byte_idx);

    // Iterate to next field in compliance with json standard.
    // Assume at quote of start of key.
    std.debug.assert(buffer[byte_idx.*] == '"');
    byte_idx.* += 1;

    // Iter over key.
    while (true) {
        const skip_idx = string_utils.simdFindCharIdxEscaped(
            buffer[byte_idx.*..], 
            ':',
            );
        byte_idx.* += skip_idx;
        if (skip_idx == string_utils.VEC_SIZE) continue;
        std.debug.assert(buffer[byte_idx.* - 1] == '"');
        byte_idx.* += 1;
        break;
    }
        
    // Iter to start of value.
    nextPoint(buffer, byte_idx);

    // Iter over value.
    // TODO: handle nested fields.
    switch (buffer[byte_idx.*]) {
        '"' => {
            byte_idx.* += 1;
            while (true) {
                const skip_idx = string_utils.simdFindCharIdxEscaped(
                    buffer[byte_idx.*..], 
                    '"',
                    );
                byte_idx.* += skip_idx;
                if (skip_idx == string_utils.VEC_SIZE) continue;
                byte_idx.* += 1;

                nextPoint(buffer, byte_idx);

                switch (buffer[byte_idx.*]) {
                    ',' => {
                        byte_idx.* += 1;
                        while (true) {
                            const quote_idx = string_utils.simdFindCharIdx(
                                buffer[byte_idx.*..], 
                                '"',
                                );
                            byte_idx.* += quote_idx;
                            if (quote_idx == string_utils.VEC_SIZE) continue;
                            break;
                        }
                    },
                    '}' => {
                        while (buffer[byte_idx.*] != '\n') byte_idx.* += 1;
                        byte_idx.* += 1;
                    },
                    else => return error.InvalidJson,
                }
                break;
            }
        },
        45, 48...57, 116, 102, 110 => {
            // Numeric values (minus, 0-9), null, true, false.
            while (true) {
                const comma_idx = string_utils.simdFindCharIdx(
                    buffer[byte_idx.*..], 
                    ',',
                    );
                const close_bracket_idx = string_utils.simdFindCharIdx(
                    buffer[byte_idx.*..], 
                    '}',
                    );
                const whitespcae_idx = string_utils.simdFindCharIdx(
                    buffer[byte_idx.*..], 
                    ' ',
                    );
                const skip_idx = @min(@min(comma_idx, close_bracket_idx), whitespcae_idx);
                byte_idx.* += skip_idx;
                if (skip_idx == string_utils.VEC_SIZE) continue;

                if (buffer[byte_idx.*] == '}') {
                    while (buffer[byte_idx.*] != '\n') byte_idx.* += 1;
                    byte_idx.* += 1;
                } else {
                    // Find quote of next key.
                    while (true) {
                        const quote_idx = string_utils.simdFindCharIdx(
                            buffer[byte_idx.*..], 
                            '"',
                            );
                        byte_idx.* += quote_idx;
                        if (quote_idx == string_utils.VEC_SIZE) continue;
                        break;
                    }
                }
                break;
            }
        },
        else => return error.InvalidJson,
    }
}

pub inline fn iterLineJSON(buffer: []const u8, byte_idx: *usize) !void {
    // Iterate to next line in compliance with json standard.
    while (true) {
        const skip_idx = string_utils.simdFindCharIdxEscaped(
            buffer[byte_idx.*..], 
            '}',
            );

        byte_idx.* += skip_idx;
        if (skip_idx == string_utils.VEC_SIZE) continue;

        byte_idx.* += 1;
        break;
    }
    if (buffer[byte_idx.*] == '\n') {
        byte_idx.* += 1;
        return;
    }
    return error.InvalidJson;
}




test "iter_line" {
    const json_string = \\{
    \\   "stringValue": "hello",
    \\   "numberValue": 42,
    \\   "booleanValue": true,
    \\   "nullValue": null,
    \\ }
    \\{
    ;
    // std.debug.print("json_string: {s}\n", .{json_string});
    var byte_idx: usize = 0;
    try iterLineJSON(json_string[0..], &byte_idx);
    std.debug.assert(byte_idx == json_string.len - 1);
}

test "iter_field" {
    const json_string = \\{
    \\  "stringValue": "hello",
    \\  "numberValue": 42,
    \\  "booleanValue": true,
    \\  "nullValue": null
    \\}
    \\{
    ;
    const iter_first = \\"numberValue": 42,
    \\  "booleanValue": true,
    \\  "nullValue": null
    \\}
    \\{
    ;
    std.debug.print("Start json_string: {s}\n", .{json_string});

    var byte_idx: usize = 1;
    try _iterFieldJSON(json_string[0..], &byte_idx);
    std.debug.assert(std.mem.eql(u8, json_string[byte_idx..], iter_first[0..]));

    std.debug.print("Second json_string: {s}\n", .{json_string[byte_idx..]});
    try _iterFieldJSON(json_string[0..], &byte_idx);
    std.debug.print("Third json_string: {s}\n", .{json_string[byte_idx..]});
    try _iterFieldJSON(json_string[0..], &byte_idx);
    std.debug.print("Fourth json_string: {s}\n", .{json_string[byte_idx..]});
    try _iterFieldJSON(json_string[0..], &byte_idx);
    std.debug.print("Fifth json_string: {s}\n", .{json_string[byte_idx..]});
}
