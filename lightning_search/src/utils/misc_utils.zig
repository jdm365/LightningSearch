const std = @import("std");

pub inline fn findSorted(
    comptime T: type,
    sorted_arr: []T,
    key: T,
) !usize {
    var val: T = sorted_arr[0];
    var idx: usize = 0;
    while (val <= key) {
        if (val == key) return idx;
        idx += 1;
        val = sorted_arr[idx];
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
