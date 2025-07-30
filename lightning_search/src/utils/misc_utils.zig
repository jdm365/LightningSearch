const std = @import("std");

pub inline fn findSorted(comptime T: type, sorted_arr: []T, key: T) !usize {
    for (0..sorted_arr.len) |idx| {
        const val = sorted_arr[idx];
        if (val == key) return idx;
        if (val > key) return error.KeyNotFound;
    }

    return error.KeyNotFound;
}


pub fn printPercentiles(
    comptime T: type,
    arr: []T,
) void {
    // Sort
    std.mem.sort(T, arr, {}, comptime std.sort.asc(T));

    const chunk_size = @divFloor(arr.len, 100);
    for (0..100) |idx| {
         std.debug.print(
             "{d}%: {any}\n", 
             .{idx, arr[idx * chunk_size]},
             );
    }

    std.debug.print(
        "Min: {any}\n", 
        .{arr[0]},
        );
    std.debug.print(
        "Max: {any}\n", 
        .{arr[arr.len - 1]},
        );
}

pub fn sortStruct(
    comptime T: type,
    arr: []T,
    comptime field_name: []const u8,
    comptime descending: bool,
) void {
    if (descending) {
        std.mem.sort(T, arr, {}, struct {
            fn greaterThan(_: void, a: T, b: T) bool {
                return @field(a, field_name) > @field(b, field_name);
            }
        }.greaterThan);
    } else {
        std.mem.sort(T, arr, {}, struct {
            fn lessThan(_: void, a: T, b: T) bool {
                return @field(a, field_name) < @field(b, field_name);
            }
        }.lessThan);
    }
}

pub inline fn bitIsSet(
    comptime T: type,
    value: T,
    idx: usize,
) bool {
    const bit_mask: T = @as(T, 1) << @truncate(idx);
    return (value & bit_mask) != 0;
}

pub inline fn setBit(
    comptime T: type,
    value: *T,
    idx: usize,
) void {
    const bit_mask: T = @as(T, 1) << @truncate(idx);
    value.* |= bit_mask;
}

pub inline fn unsetBit(
    comptime T: type,
    value: *T,
    idx: usize,
) void {
    const bit_mask: T = @as(T, 1) << @truncate(idx);
    value.* &= ~bit_mask;
}

pub inline fn contains(
    haystack: []const u8, 
    needle: []const u8,
    ) bool {
    return std.mem.indexOf(u8, haystack, needle) != null;
}


test "sortStruct" {
    const misc_utils = @import("misc_utils.zig");

    const TestStruct = struct {
        value: i32,
    };

    var arr: [5]TestStruct = .{
        .{ .value = 5 },
        .{ .value = 3 },
        .{ .value = 1 },
        .{ .value = 4 },
        .{ .value = 2 },
    };

    misc_utils.sortStruct(TestStruct, arr[0..], "value", false);

    try std.testing.expectEqual(
        arr,
        [_]TestStruct{ .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 }, .{ .value = 4 }, .{ .value = 5 } },
    );
}
