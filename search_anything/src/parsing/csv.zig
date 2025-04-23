const std = @import("std");
const string_utils = @import("../utils/string_utils.zig");
const TermPos      = @import("../server/server.zig").TermPos;


pub inline fn _iterFieldCSV(buffer: []const u8, byte_idx: *usize) void {
    // Iterate to next field in compliance with RFC 4180.
    const is_quoted = buffer[byte_idx.*] == '"';
    byte_idx.* += @intFromBool(is_quoted);

    outer_loop: while (true) {
        if (is_quoted) {
            const skip_idx = string_utils.simdFindCharIdx(
                buffer[byte_idx.*..], 
                '"',
                );
            byte_idx.* += skip_idx;
            if (skip_idx == string_utils.VEC_SIZE) continue;
            
            byte_idx.* += 1;

            // Check if escape quote.
            if (buffer[byte_idx.*] == '"') {
                byte_idx.* += 1;
                continue;
            }
            std.debug.assert(
                (buffer[byte_idx.*] == ',') 
                    or 
                (buffer[byte_idx.*] == '\n')
                );
            byte_idx.* += 1;
            break :outer_loop;

        } else {
            const newline_idx = string_utils.simdFindCharIdx(buffer[byte_idx.*..], '\n');
            const comma_idx   = string_utils.simdFindCharIdx(buffer[byte_idx.*..], ',');

            const skip_idx = @min(newline_idx, comma_idx);
            byte_idx.* += skip_idx;
            if (skip_idx == string_utils.VEC_SIZE) continue;
            std.debug.assert((buffer[byte_idx.*] == ',') or (buffer[byte_idx.*] == '\n'));
            byte_idx.* += 1;
            break :outer_loop;
        }
    }
}

pub inline fn iterLineCSV(buffer: []const u8, byte_idx: *usize) !void {
    // Iterate to next line in compliance with RFC 4180.

    var skip_idx: usize = 0;
    var is_newline: bool = false;
    var quote_idx: usize = 0;
    var newline_idx: usize = 0;

    while (true) {
        quote_idx   = string_utils.simdFindCharIdx(buffer[byte_idx.*..], '"');
        newline_idx = string_utils.simdFindCharIdx(buffer[byte_idx.*..], '\n');

        if (quote_idx < newline_idx) {
            skip_idx = quote_idx;
            is_newline = false;
        } else {
            skip_idx = newline_idx;
            is_newline = true;
        }
        byte_idx.* += skip_idx;
        if (skip_idx == string_utils.VEC_SIZE) continue;

        byte_idx.* += 1;
        if (!is_newline) {

            while (true) {
                quote_idx = string_utils.simdFindCharIdx(buffer[byte_idx.*..], '"');
                byte_idx.* += quote_idx;
                if (quote_idx == string_utils.VEC_SIZE) continue;

                byte_idx.* += 1;
                if (buffer[byte_idx.*] == '"') {
                    byte_idx.* += 1;
                    continue;
                }
                std.debug.assert((buffer[byte_idx.*] == ',') or (buffer[byte_idx.*] == '\n'));
                break;
            }
            continue;
        }
        return;
    }
}

pub inline fn parseRecordCSV(
    buffer: []const u8,
    result_positions: []TermPos,
) !void {
    // Parse CSV record in compliance with RFC 4180.
    var byte_idx: usize = 0;
    for (0..result_positions.len) |idx| {
        const start_pos = byte_idx;
        _iterFieldCSV(buffer, &byte_idx);
        result_positions[idx] = TermPos{
            .start_pos = @as(u32, @intCast(start_pos)) + 
                         @intFromBool(buffer[start_pos] == '"'),
            .field_len = @as(u32, @intCast(byte_idx - start_pos - 1)) - 
                         2 * @as(u32, @intFromBool(buffer[start_pos] == '"')),
        };
    }
}
