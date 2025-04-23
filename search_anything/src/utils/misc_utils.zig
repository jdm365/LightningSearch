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
