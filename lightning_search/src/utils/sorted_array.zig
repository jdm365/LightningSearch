const std = @import("std");


pub const MAX_CAPACITY: usize = 1000;

const ScorePair = struct {
    doc_id: u32,
    score:  f32,
};

pub fn topk_score_compare_func(_: void, a: ScorePair, b: ScorePair) std.math.Order {
    if (a.score > b.score) return std.math.Order.gt;
    return std.math.Order.lt;
}

pub fn TopKPQ(
    comptime T: type,
    comptime Context: type,
    comptime compareFn: fn (context: Context, a: T, b: T) std.math.Order,
    ) type {

    return struct{
        const Self = @This();

        pq: std.PriorityQueue(T, Context, compareFn),
        k: usize,

        pub fn init(
            allocator: std.mem.Allocator, 
            context: type,
            k: usize,
            ) Self {
            const pq = std.PriorityQueue(T, context, compareFn).init(allocator, {});
            return Self{
                .pq = pq,
                .k = k,
            };
        }

        pub fn deinit(self: *Self) void {
            self.pq.deinit();
        }

        pub fn add(self: *Self, entry: T) !void {
            try self.pq.add(entry);
            if (self.pq.items.len > self.k) {
                _ = self.pq.removeIndex(self.k);
            }
        }
    };
}

pub fn SortedScoreArray(comptime T: type) type {

    return struct {
        const Self = @This();

        // TODO: Consider MultiArraylist items.
        allocator: std.mem.Allocator,
        items: []T,
        count: usize,
        capacity: usize,

        pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
            if (size > MAX_CAPACITY) {
                std.debug.print("Max Capacity: {d}\n", .{MAX_CAPACITY});
                @panic("Value exceeded max capcity\n");
            }

            return Self{
                .allocator = allocator,
                .items = try allocator.alloc(T, size + 1),
                .count = 0,
                .capacity = size,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
        }

        inline fn cmp(lhs: T, rhs: T) bool {
            // For a max heap, we return true if the lhs.score is less than rhs.score
            return lhs.score > rhs.score;
        }

        pub inline fn clear(self: *Self) void {
            self.count = 0;
        }

        pub inline fn resize(self: *Self, new_size: usize) void {
            if (new_size > MAX_CAPACITY) {
                @panic("Cannot grow the array");
            }
            self.capacity = new_size;
        }

        inline fn binarySearch(self: *Self, item: T) usize {
            // TODO: Allow for common case of very many items and place starting
            // needle closer to the end.
            var low: usize = 0;
            var high: usize = self.count;

            while (low < high) {
                const mid = low + (high - low) / 2;

                if (self.items[mid].score == item.score) return mid;

                if (cmp(self.items[mid], item)) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        inline fn linearSearch(self: *Self, item: T) usize {
            for (0.., self.items[0..self.count]) |idx, val| {
                if (!cmp(val, item)) {
                    return idx;
                }
            }
            return self.count;
        }

        inline fn dualSearch(self: *const Self, item: T) usize {
            const mid = @divFloor(self.count, 2);
            const min = if (cmp(self.items[0], item)) 0 else mid;
            const max = min + mid;

            for (min..max, self.items[0..self.count]) |idx, val| {
                if (!cmp(val, item)) {
                    return idx;
                }
            }
            return self.count;
        }

        inline fn search(self: *Self, item: T) usize {
            // if (self.count <= 32) {
            if (self.count <= 4) {
                return self.linearSearch(item);
            } else {
                return self.binarySearch(item);
            }
        }

        pub inline fn insert(self: *Self, item: T) void {
            const insert_idx = self.search(item);
            if (insert_idx == self.capacity) return;

            self.count = @min(self.count + 1, self.capacity);

            var idx: usize = self.count;
            while (idx > insert_idx) {
                self.items[idx] = self.items[idx - 1];
                idx -= 1;
            }

            self.items[insert_idx] = item;
        }

        pub inline fn insertCheck(self: *Self, item: T) bool {
            // Returns true if inserted item was inserted, false if not.
            const insert_idx = self.search(item);
            if (insert_idx == self.capacity) return false;

            self.count = @min(self.count + 1, self.capacity);

            var idx: usize = self.count;
            while (idx > insert_idx) {
                self.items[idx] = self.items[idx - 1];
                idx -= 1;
            }

            self.items[insert_idx] = item;

            return true;
        }

        pub inline fn getMinScore(self: *Self) f32 {
            if (self.count != self.capacity) return -std.math.floatMax(f32);
            return self.items[self.count - 1].score;
        }

        pub fn check(self: *Self) void {
            var prev_value: f32 = 1000000.0;
            for (0.., self.items[0..self.count]) |idx, item| {
                if (item.score > prev_value) {
                    std.debug.print("IDX: {d}\n", .{idx});
                    std.debug.print("Score: {d}\n", .{item.score});
                    std.debug.print("Prev Score: {d}\n", .{prev_value});
                    @panic("Bad copy\n");
                }
                prev_value = item.score;
            }
        }
    };
}


pub fn SortedScoreMultiArray(comptime T: type) type {

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: []align(32)T,
        scores: []align(32)f32,
        count: usize,
        capacity: usize,

        pub const Result = struct {
            value: T,
            score: f32,
        };

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            if (capacity > MAX_CAPACITY) {
                std.debug.print("Max Capacity: {d}\n", .{MAX_CAPACITY});
                @panic("Value exceeded max capcity\n");
            }

            const alloc_size: usize = std.mem.alignForward(usize, capacity + 1, 8);

            const scores = try allocator.alignedAlloc(
                f32, 
                // .@"32",
                32,
                alloc_size,
                );
            @memset(scores, -std.math.floatMax(f32));

            return Self{
                .allocator = allocator,
                // .items = try allocator.alignedAlloc(T, .@"32", alloc_size),
                .items = try allocator.alignedAlloc(T, 32, alloc_size),
                .scores = scores,
                .count = 0,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
            self.allocator.free(self.scores);
        }

        pub inline fn clear(self: *Self) void {
            self.count = 0;
        }

        pub inline fn resize(self: *Self, new_size: usize) void {
            if (new_size > MAX_CAPACITY) {
                @panic("Cannot grow the array");
            }
            self.capacity = new_size;
        }

        inline fn binarySearch(self: *Self, score: f32) usize {
            // TODO: Allow for common case of very many items and place starting
            // needle closer to the end.
            var low: usize = 0;
            var high: usize = self.count;

            while (low < high) {
                const mid = low + (high - low) / 2;

                if (self.scores[mid] == score) return mid;

                if (self.scores[mid] > score) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        inline fn linearSearch(self: *Self, score: f32) usize {
            for (0.., self.scores[0..self.count]) |idx, _score| {
                if (_score <= score) {
                    return idx;
                }
            }
            return self.count;
        }

        inline fn linearSearchSIMD(self: *Self, score: f32) usize {
            const new_score     = @as(@Vector(8, f32), @splat(score));
            var existing_scores = @as(
                *const @Vector(8, f32), 
                @ptrCast(self.scores.ptr),
                );

            const simd_limit: usize = @divFloor(self.count, 8) + 
                                      8 * @as(usize, @intFromBool(self.count % 8 != 0));
            var idx: usize = 0;
            while (idx < simd_limit) {
                const mask = new_score > existing_scores.*;
                const set_idx = @ctz(@as(u8, @bitCast(mask)));
                if (set_idx != 8) {
                    return @min(idx + set_idx, self.count);
                }

                existing_scores = @ptrFromInt(@intFromPtr(self.scores.ptr) + 32);
                idx += 8;
            }
            return self.count;
        }

        inline fn search(self: *Self, score: f32) usize {
            if (self.count <= 32) {
                return self.linearSearchSIMD(score);
            } else {
                return self.binarySearch(score);
            }
        }

        pub inline fn insert(self: *Self, value: T, score: f32) void {
            const insert_idx = self.search(score);
            if (insert_idx == self.capacity) return;

            self.count = @min(self.count + 1, self.capacity);

            std.mem.copyBackwards(
                T,
                self.items[insert_idx + 1..self.count + 1],
                self.items[insert_idx..self.count],
                );
            std.mem.copyBackwards(
                f32,
                self.scores[insert_idx + 1..self.count + 1],
                self.scores[insert_idx..self.count],
                );

            self.items[insert_idx]  = value;
            self.scores[insert_idx] = score;
        }

        pub fn check(self: *Self) void {
            var prev_value: f32 = 1000000.0;
            for (0.., self.scores[0..self.count]) |idx, score| {
                if (score > prev_value) {
                    std.debug.print("IDX: {d}\n", .{idx});
                    std.debug.print("Score: {d}\n", .{score});
                    std.debug.print("Prev Score: {d}\n", .{prev_value});
                    @panic("Bad copy\n");
                }
                prev_value = score;
            }
        }

        pub inline fn last(self: *Self) T {
            if (self.count == 0) {
                return T.init();
            }
            return self.items[self.count - 1];
        }

        pub inline fn lastScore(self: *Self) f32 {
            if (self.count == 0) {
                return -std.math.floatMax(f32);
            }
            return self.scores[self.count - 1];
        }

        pub inline fn lastScoreCapacity(self: *Self) f32 {
            if (self.capacity == 0) {
                @branchHint(.cold);
                return -std.math.floatMax(f32);
            }
            std.debug.print("Scores: {d}\n", .{self.scores});
            std.debug.print("Final:  {d}\n\n", .{self.scores[self.capacity]});
            return self.scores[self.capacity];
        }
    };
}


pub fn SortedIntMultiArray(comptime T: type, comptime descending: bool) type {

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        items: []align(32)T,
        values: []align(32)u32,
        count: usize,
        capacity: usize,

        pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
            const alloc_size: usize = std.mem.alignForward(usize, capacity + 1, 8);

            const values = try allocator.alignedAlloc(
                u32, 
                .@"32",
                alloc_size,
                );
            @memset(values, comptime if (descending) 0 else std.math.maxInt(u32));

            return Self{
                .allocator = allocator,
                .items = try allocator.alignedAlloc(T, .@"32", alloc_size),
                .values = values,
                .count = 0,
                .capacity = capacity,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.items);
            self.allocator.free(self.values);
        }

        pub inline fn clear(self: *Self) void {
            self.count = 0;
        }

        pub inline fn resize(self: *Self, new_size: usize) void {
            self.capacity = new_size;
        }

        inline fn binarySearch(self: *Self, value: u32) usize {
            // TODO: Allow for common case of very many items and place starting
            // needle closer to the end.
            var low: usize = 0;
            var high: usize = self.count;

            while (low < high) {
                const mid = low + (high - low) / 2;

                if (self.values[mid] == value) return mid;

                if (descending) {
                    if (self.values[mid] > value) {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                } else {
                    if (self.values[mid] < value) {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }
            }
            return low;
        }

        inline fn linearSearch(self: *Self, value: u32) usize {
            for (0.., self.values[0..self.count]) |idx, _values| {
                if (descending) {
                    if (_values <= value) {
                        return idx;
                    }
                } else {
                    if (_values >= value) {
                        return idx;
                    }
                }
            }
            return self.count;
        }

        inline fn linearSearchSIMD(self: *Self, value: u32) usize {
            const new_value     = @as(@Vector(8, u32), @splat(value));
            var existing_values = @as(
                *const @Vector(8, u32), 
                @ptrCast(self.values.ptr),
                );

            const simd_limit: usize = @divFloor(self.count, 8) + 
                                      8 * @as(usize, @intFromBool(self.count % 8 != 0));
            var idx: usize = 0;
            while (idx < simd_limit) {
                const mask = if (descending) 
                    new_value > existing_values.*
                 else 
                    new_value < existing_values.*
                ;

                const set_idx = @ctz(@as(u8, @bitCast(mask)));
                if (set_idx != 8) {
                    return @min(idx + set_idx, self.count);
                }

                existing_values = @ptrFromInt(@intFromPtr(self.values.ptr) + 32);
                idx += 8;
            }
            return self.count;
        }

        pub inline fn search(self: *Self, value: u32) usize {
            if (self.count <= 32) {
                return self.linearSearchSIMD(value);
            } else {
                return self.binarySearch(value);
            }
        }

        pub inline fn insert(self: *Self, item: T, value: u32) void {
            const insert_idx = self.search(value);
            if (insert_idx == self.capacity) return;

            self.count = @min(self.count + 1, self.capacity);

            std.mem.copyBackwards(
                T,
                self.items[insert_idx + 1..self.count + 1],
                self.items[insert_idx..self.count],
                );
            std.mem.copyBackwards(
                u32,
                self.values[insert_idx + 1..self.count + 1],
                self.values[insert_idx..self.count],
                );

            self.items[insert_idx]  = item;
            self.values[insert_idx] = value;
        }

        pub inline fn insertIdx(
            self: *Self, 
            value: u32,
            item: T,
            insert_idx: usize,
            ) void {
            self.count = @min(self.count + 1, self.capacity);

            std.mem.copyBackwards(
                T,
                self.items[insert_idx + 1..self.count + 1],
                self.items[insert_idx..self.count],
                );
            std.mem.copyBackwards(
                u32,
                self.values[insert_idx + 1..self.count + 1],
                self.values[insert_idx..self.count],
                );

            self.items[insert_idx]  = item;
            self.values[insert_idx] = value;
        }

        pub inline fn insertGet(self: *Self, value: u32) *T {
            const insert_idx = self.search(value);
            if (insert_idx == self.capacity) return &self.values[insert_idx];

            self.count = @min(self.count + 1, self.capacity);

            std.mem.copyBackwards(
                T,
                self.items[insert_idx + 1..self.count + 1],
                self.items[insert_idx..self.count],
                );
            std.mem.copyBackwards(
                u32,
                self.values[insert_idx + 1..self.count + 1],
                self.values[insert_idx..self.count],
                );

            self.items[insert_idx]  = T.init();
            self.values[insert_idx] = value;

            return &self.items[insert_idx];
        }

        pub fn check(self: *Self) void {
            var prev_value: f32 = 1000000.0;
            for (0.., self.values[0..self.count]) |idx, value| {
                if (value > prev_value) {
                    std.debug.print("IDX: {d}\n", .{idx});
                    std.debug.print("Score: {d}\n", .{value});
                    std.debug.print("Prev Score: {d}\n", .{prev_value});
                    @panic("Bad copy\n");
                }
                prev_value = value;
            }
        }
    };
}


test "sorted_arr" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var arr = try SortedScoreArray(ScorePair).init(allocator, 10);
    defer arr.deinit();

    for (0..10) |idx| {
        const item = ScorePair{ 
            .doc_id = @intCast(idx + 1), 
            .score = 1.0 - 0.1 * @as(f32, @floatFromInt(idx)) 
        };
        arr.insert(item);
        arr.check();
    }

    try std.testing.expectEqual(arr.items.len - 1, arr.count);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(1, arr.items[0].doc_id);
    try std.testing.expectEqual(2, arr.items[1].doc_id);
    try std.testing.expectEqual(3, arr.items[2].doc_id);

    std.debug.print("MIN SCORE: {d}\n", .{arr.getMinScore()});

    const item = ScorePair{ .doc_id = 42069, .score = 10000.0 };
    arr.insert(item);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(42069, arr.items[0].doc_id);
    try std.testing.expectEqual(10000.0, arr.items[0].score);
    try std.testing.expectEqual(1, arr.items[1].doc_id);
}

test "sorted_multi_arr" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var arr = try SortedScoreMultiArray(u32).init(allocator, 10);
    defer arr.deinit();

    for (0..10) |idx| {
        arr.insert(@intCast(idx + 1), 1.0 - 0.1 * @as(f32, @floatFromInt(idx)));
        arr.check();
    }

    try std.testing.expectEqual(arr.capacity, arr.count);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(1, arr.items[0]);
    try std.testing.expectEqual(2, arr.items[1]);
    try std.testing.expectEqual(3, arr.items[2]);

    // std.debug.print("MIN SCORE: {d}\n", .{arr.getMinScore()});

    arr.insert(42069, 10000.0);
    try std.testing.expectEqual(10, arr.count);
    try std.testing.expectEqual(42069, arr.items[0]);
    try std.testing.expectEqual(10000.0, arr.scores[0]);
    try std.testing.expectEqual(1, arr.items[1]);
}

test "bench" {
    var gpa = std.heap.DebugAllocator(.{}){}; defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const N: usize = 16;
    const M: usize = 1_000_000;

    var arr = try SortedScoreArray(ScorePair).init(allocator, N);
    defer arr.deinit();

    // Generate M random integers and insert them into the array.
    var rand_floats: [M]f32 = undefined;
    for (0..M) |idx| {
        rand_floats[idx] = std.crypto.random.float(f32);
    }

    var start_time = std.time.milliTimestamp();
    for (0..M) |idx| {
        const item = ScorePair{ 
            .doc_id = @intCast(idx + 1), 
            .score = rand_floats[idx],
        };
        arr.insert(item);
    }
    var end_time = std.time.milliTimestamp();
    var elapsed_time = end_time - start_time;

    std.debug.print("Time taken sorted array: {d}ms\n", .{elapsed_time});

    var arr2 = try SortedScoreMultiArray(u32).init(allocator, N);
    defer arr2.deinit();

    // Generate M random integers and insert them into the array.
    start_time = std.time.milliTimestamp();
    for (0..M) |idx| {
        arr2.insert(@intCast(idx + 1), rand_floats[idx]);
    }
    end_time = std.time.milliTimestamp();
    elapsed_time = end_time - start_time;

    std.debug.print("Time taken sorted array: {d}ms\n", .{elapsed_time});


    // Now do min heap.
    var pq = TopKPQ(ScorePair, void, topk_score_compare_func).init(
        allocator, 
        void, 
        N,
        );
    defer pq.deinit();

    start_time = std.time.milliTimestamp();
    for (0..M) |idx| {
        const item = ScorePair{ 
            .doc_id = @intCast(idx + 1), 
            .score = rand_floats[idx],
        };
        try pq.add(item);
    }
    end_time = std.time.milliTimestamp();
    elapsed_time = end_time - start_time;

    std.debug.print("Time taken min heap: {d}ms\n", .{elapsed_time});
}
