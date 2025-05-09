const std = @import("std");

pub const VEC128 = @Vector(4, u32);

pub fn StaticIntegerSet(comptime n: u32) type {
    return struct {
        const Self = @This();

        values: [n]u32 align(16),
        count: usize,

        pub fn init() Self {
            return Self{
                .values = undefined,
                .count = 0,
            };
        }

        pub fn clear(self: *Self) void {
            self.count = 0;
        }

        pub inline fn checkOrInsert(self: *Self, new_value: u32) bool {
            // Don't allow new insertions if full.
            if (self.count == n) return true;

            // If element already exists return true, else return false.
            // If element doesn't exist also insert.
            for (0..self.count) |idx| {
                if (self.values[idx] == new_value) return true;
            }

            self.values[self.count] = new_value;
            self.count += 1;
            return false;
        }


        pub inline fn checkOrInsertSIMD(self: *Self, new_value: u32) bool {
            // Don't allow new insertions if full.
            if (self.count == n) return true;

            // If element already exists return true, else return false.
            // If element doesn't exist also insert.
            const floor_loop_idx = self.count - (self.count % 4);

            const valueSIMD: VEC128 = @splat(new_value);

            var idx: usize = 0;
            while (idx < floor_loop_idx) {
                if (std.simd.countTrues(@as(VEC128, self.values[idx..idx+4][0..4].*) == valueSIMD) > 0) {
                    return true;
                }
                idx += 4;
            }

            while (floor_loop_idx < self.count) {
                if (self.values[idx] == new_value) return true;
                idx += 1;
            }

            self.values[self.count] = new_value;
            self.count += 1;
            return false;
        }
    };
}


test "bench" {

    const n = 1000;
    var set = StaticIntegerSet(n).init();
    var i: u32 = 0;

    while (i < n) {
        const result = set.checkOrInsert(i);
        std.debug.assert(!result);
        i += 1;
    }

    i = 0;
    while (i < n) {
        const result = set.checkOrInsertSIMD(i);
        std.debug.assert(result);
        i += 1;
    }


    // Bench
    const num_queries = 100_000;
    const num_inserts = 10;
    var start = std.time.nanoTimestamp();
    for (0..num_queries) |idx| {
        _ = set.checkOrInsert(@truncate(idx % num_inserts));
    }

    var end = std.time.nanoTimestamp();
    var execution_time_ms: usize = @intCast(end - start);
    var qps = @divFloor(num_queries * 1000, execution_time_ms);
    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});

    start = std.time.nanoTimestamp();
    for (0..num_queries) |idx| {
        _ = set.checkOrInsertSIMD(@truncate(idx % num_inserts));
    }

    end = std.time.nanoTimestamp();
    execution_time_ms = @intCast(end - start);
    qps = @divFloor(num_queries * 1000, execution_time_ms);
    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});
}
