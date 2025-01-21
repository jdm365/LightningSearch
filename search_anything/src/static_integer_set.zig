pub fn StaticIntegerSet(comptime n: u32) type {
    return struct {
        const Self = @This();

        values: [n]u32,
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

        pub fn checkOrInsert(self: *Self, new_value: u32) bool {
            // TODO: Explore SIMD implementation. Expand copy new value into simd register LANE_WIDTH / 32 times. 
            //
            //
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
    };
}

