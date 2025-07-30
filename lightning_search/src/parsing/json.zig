const std = @import("std");

const string_utils    = @import("../utils/string_utils.zig");
const RadixTrie       = @import("../utils/radix_trie.zig").RadixTrie;
const MAX_TERM_LENGTH = @import("../indexing/index.zig").MAX_TERM_LENGTH;
const TermPos         = @import("../server/server.zig").TermPos;


pub inline fn skipSepChars(buffer: []const u8, byte_idx: *usize) void {
    while (
        (buffer[byte_idx.*] == ' ') 
            or 
        (buffer[byte_idx.*] == '\n')) {
        byte_idx.* += 1;
    }
}

pub inline fn iterValueJSON(buffer: []const u8, byte_idx: *usize) void {
    while (true) {
        switch (buffer[byte_idx.*]) {
            '\\' => byte_idx.* += 2,
            'n', 't', 'f', 'N', 'T', 'F', '-', '0'...'9' => {
                byte_idx.* += 1;
                while (
                    (buffer[byte_idx.*] != '}')
                       and 
                    (buffer[byte_idx.*] != ',')
                       and 
                    (buffer[byte_idx.*] != ' ')
                ) {
                    byte_idx.* += 1;
                }
                return;
            },
            '"' => {
                byte_idx.* += 1;
                const skip_idx = string_utils.simdFindCharIdxEscapedFull(
                    buffer[byte_idx.*..],
                    '"',
                );
                byte_idx.* += skip_idx;
                return;
            },
            else => byte_idx.* += 1, 
        }
    }
}

pub inline fn _iterFieldJSON(buffer: []const u8, byte_idx: *usize) !void {
    // Iter to start of value.
    skipSepChars(buffer, byte_idx);

    // Iterate to next field in compliance with json standard.
    // Assume at quote of start of key.
    std.debug.assert(buffer[byte_idx.*] == '"');
    byte_idx.* += 1;

    // Iter over key.
    while (true) {
        const first_char_escaped: bool = buffer[byte_idx.* - 1] == '\\';
        const skip_idx = string_utils.simdFindCharIdxEscaped(
            buffer[byte_idx.*..], 
            ':',
            first_char_escaped,
            );
        byte_idx.* += skip_idx;
        if (skip_idx == string_utils.VEC_SIZE) continue;
        std.debug.assert(buffer[byte_idx.* - 1] == '"');
        byte_idx.* += 1;
        break;
    }
        
    // Iter to start of value.
    skipSepChars(buffer, byte_idx);

    // Iter over value.
    // TODO: handle nested fields.
    switch (buffer[byte_idx.*]) {
        '"' => {
            byte_idx.* += 1;
            while (true) {
                const skip_idx = string_utils.simdFindCharIdxEscaped(
                    buffer[byte_idx.*..], 
                    '"',
                    false,
                    );
                byte_idx.* += skip_idx;
                if (skip_idx == string_utils.VEC_SIZE) continue;
                byte_idx.* += 1;

                skipSepChars(buffer, byte_idx);

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
        45, 48...57, 't', 'f', 'n', 'T', 'F', 'N' => {
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

pub inline fn matchKey(
    buffer: []const u8, 
    byte_idx: *usize,
    reference_dict: *const RadixTrie(u32),
    uppercase_key: bool,
    ) !u32 {
    const key_len = string_utils.simdFindCharIdxEscaped(
        buffer[byte_idx.*..], 
        '"',
        false,
    );

    const key = switch (uppercase_key) {
        true => blk: {
            var buf: [256]u8 = undefined;
            @memcpy(buf[0..key_len], buffer[byte_idx.*..byte_idx.* + key_len]);
            string_utils.stringToUpper(buf[0..key_len].ptr, key_len);
            break :blk buf[0..key_len];
        },
        false => buffer[byte_idx.*..byte_idx.* + key_len],
    };

    const match_val = try reference_dict.find(key);
    return match_val;
}

pub inline fn nextKeyValuePoints(
    buffer: []const u8,
    byte_idx: *usize,

    start_idx_key: *u64,
    end_idx_key:   *u64,

    start_idx_value: *u64,
    end_idx_value:   *u64,
) void {
    std.debug.assert(buffer[byte_idx.*] == '"');
    byte_idx.* += 1;
    start_idx_key.* = byte_idx.*;

    const key_len = string_utils.simdFindCharIdxEscaped(
        buffer[byte_idx.*..], 
        '"',
        false,
    );
    byte_idx.* += key_len;
    end_idx_key.* = byte_idx.*;

    byte_idx.* += 2;

    skipSepChars(buffer, byte_idx);

    start_idx_value.* = byte_idx.*;
    iterValueJSON(buffer, byte_idx);
    end_idx_value.* = byte_idx.*;

    // Goto next key or '}'
    byte_idx.* += @intFromBool(buffer[byte_idx.*] == '"');
    byte_idx.* += @intFromBool(buffer[byte_idx.*] == ',');
    skipSepChars(buffer, byte_idx);
}

pub inline fn nextKeyFromValue(
    buffer: []const u8, 
    byte_idx: *usize,
    ) !void {
        iterValueJSON(buffer, byte_idx);

        // Check if EOL.
        switch (buffer[byte_idx.*]) {
            '}' => {
                byte_idx.* += 1;

                // Iter to start of next doc and return.
                while (buffer[byte_idx.*] != '{') byte_idx.* += 1;
                return error.EOL;
            },
            ',' => {
                // Iter over comma.
                byte_idx.* += 1;

                // Iter to quote start of next key.
                byte_idx.* += string_utils.simdFindCharIdxEscaped(
                    buffer[byte_idx.*..], 
                    '"',
                    false,
                );
            },
            else => {
                std.debug.print("Found unexpected charachter: {c}\n", .{buffer[byte_idx.*]});
                @panic("JSON decoding error");
            }
        }
}

pub inline fn iterLineJSON(buffer: []const u8, byte_idx: *usize) !void {
    std.debug.assert(buffer[byte_idx.*] == '{');
    byte_idx.* += 1;

    // Iterate to next line in compliance with json standard.

    var skip_idx: usize = 0;
    var quote_idx: usize = 0;
    var close_bracket_idx: usize = 0;
    var is_close_bracket: bool = false;
    var first_char_escaped: bool = false;

    while (true) {
        quote_idx         = string_utils.simdFindCharIdxEscaped(
            buffer[byte_idx.*..], 
            '"',
            first_char_escaped,
            );
        close_bracket_idx = string_utils.simdFindCharIdxEscaped(
            buffer[byte_idx.*..], 
            '}',
            first_char_escaped,
            );

        if (quote_idx < close_bracket_idx) {
            skip_idx = quote_idx;
            is_close_bracket = false;
        } else {
            skip_idx = close_bracket_idx;
            is_close_bracket = true;
        }
        byte_idx.* += skip_idx;
        first_char_escaped = buffer[byte_idx.* - 1] == '\\';
        if (skip_idx == string_utils.VEC_SIZE) continue;

        if (!is_close_bracket) {
            byte_idx.* += 1;
            quote_idx = string_utils.simdFindCharIdxEscapedFull(
                buffer[byte_idx.*..], 
                '"',
                );
            byte_idx.* += quote_idx + 1;
            continue;
        }
        break;
    }

    std.debug.assert(buffer[byte_idx.*] == '}');
    while (buffer[byte_idx.*] != '{') byte_idx.* += 1;
}

pub inline fn iterLineJSONGetUniqueKeys(
    buffer: []const u8, 
    byte_idx: *usize,
    // unique_keys: *std.StringHashMap(u32),
    unique_keys: *RadixTrie(u32),
    comptime uppercase: bool,
    ) !void {
    // Just do charachter by charachter for now.
    // Assume starting from '{'

    std.debug.assert(buffer[byte_idx.*] == '{');
    byte_idx.* += 1;

    var KEY_BUFFER: [MAX_TERM_LENGTH]u8 = undefined;
    var key_idx: usize = 0;

    while (true) {
        switch (buffer[byte_idx.*]) {
            '\\' => byte_idx.* += 2,
            '"' => {
                byte_idx.* += 1;

                while (buffer[byte_idx.*] != '"') {

                    if (buffer[byte_idx.*] == '\\') {
                        byte_idx.* += 1;
                        if (uppercase) {
                            KEY_BUFFER[key_idx] = std.ascii.toUpper(buffer[byte_idx.*]);
                        }

                        byte_idx.* += 1;
                        key_idx    += 1;
                        continue;
                    }

                    KEY_BUFFER[key_idx] = std.ascii.toUpper(buffer[byte_idx.*]);

                    byte_idx.* += 1;
                    key_idx    += 1;
                }

                try unique_keys.insertNoReplace(
                    KEY_BUFFER[0..key_idx], 
                    @truncate(unique_keys.num_keys),
                    );

                key_idx = 0;
                byte_idx.* += 2;

                iterValueJSON(buffer, byte_idx);

                byte_idx.* += 1;
                if (buffer[byte_idx.* - 1] == '}') {
                    while (buffer[byte_idx.*] != '{') byte_idx.* += 1;
                    return;
                }
            },
            'n', 't', 'f', 'N', 'T', 'F', '-', '0'...'9' => {
                byte_idx.* += 1;
                while (
                    (buffer[byte_idx.*] != '}')
                       and 
                    (buffer[byte_idx.*] != ',')
                ) {
                    byte_idx.* += 1;
                }

                byte_idx.* += 1;
                if (buffer[byte_idx.* - 1] == '}') {
                    while (buffer[byte_idx.*] != '{') byte_idx.* += 1;
                    return;
                }
            },
            else => byte_idx.* += 1,
        }
    }

    return error.InvalidJson;
}



test "iter_line" {
    const json_string = \\{
    \\   "stringValue": "hello",
    \\   "numberValue": 42,
    \\   "booleanValue": true,
    \\   "nullValue": null
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


test "get_unique_keys" {
    const json_string = \\{
    \\  "stringValue": "hello",
    \\  "numberValue": 42,
    \\  "booleanValue": true,
    \\  "booleanValue": true,
    \\  "nullValue": null
    \\}
    \\{
    ;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    std.debug.print("Start json_string: {s}\n", .{json_string});
    var unique_keys = try RadixTrie(u32).init(arena.allocator());
    defer unique_keys.deinit();

    var byte_idx: usize = 0;
    try iterLineJSONGetUniqueKeys(
        json_string[0..], 
        &byte_idx,
        &unique_keys,
        true
        );

    var iterator = try unique_keys.iterator();
    defer iterator.deinit();
    while (try iterator.next()) |item| {
        // std.debug.print("{s}\n", .{item.key_ptr.*});
        std.debug.print("{s}\n", .{item.key});
    }
        
    std.debug.print("Start json_string: {s}\n", .{json_string[byte_idx..]});
}
