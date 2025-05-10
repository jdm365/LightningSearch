const std = @import("std");

pub const RoaringBitmap_u24 = struct {
    occupied_containers: std.ArrayList(u8),
    containers: []RoaringContainer,
    allocator: std.mem.Allocator,

    pub const RoaringContainer = union(enum) {
        array: []u16,
        bitset: [8192]u64,

        pub fn init(allocator: std.mem.Allocator, cardinality: usize) !RoaringContainer {
            if (cardinality <= 4096) {
                return RoaringContainer{ .array = try allocator.alloc(u16, cardinality) };
            } else {
                const container = RoaringContainer{ .bitset = undefined };
                @memset(container.bitset, 0);
                return container;
            }
        }

        pub fn deinit(self: *RoaringContainer, allocator: std.mem.Allocator) void {
            switch (self.*) {
                .array => |arr| allocator.free(arr),
                .bitset => {},
            }
        }
    };


    // pub fn insert(


};


test "init" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // const bitmap = RoaringBitmap_u24{
    _ = RoaringBitmap_u24{
        .occupied_containers = std.ArrayList(u8).init(allocator),
        .containers = undefined,
        .allocator = allocator,
    };
    // bitmap.deinit();
}
