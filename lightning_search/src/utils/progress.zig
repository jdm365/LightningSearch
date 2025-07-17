const std = @import("std");


const DEFAULT_NUM_UPDATES = 1000;
const BAR_WIDTH = 121;

pub const ProgressBar = struct {
    total_iters:   usize,
    current_iter:  usize,
    current_idx:  usize,
    total_updates: usize,
    update_differential: usize,

    bar_string: [BAR_WIDTH + 2]u8,

    display_units: DisplayUnits,

    const DisplayUnits = enum {
        None,
        K,
        M,
    };

    pub fn init(
        total_iters: usize,
        display_units: DisplayUnits,
        ) ProgressBar {
        const total_updates = @min(BAR_WIDTH - 1, total_iters);

        var bar = ProgressBar{ 
            .total_iters = total_iters,
            .current_iter = 0,
            .current_idx = 1,
            .total_updates = total_updates,
            .update_differential = @max(1, total_iters / total_updates),
            .bar_string = undefined,

            .display_units = display_units,
        };

        for (1..bar.bar_string.len - 1) |i| {
            bar.bar_string[i] = ' ';
        }
        bar.bar_string[0] = '[';
        bar.bar_string[BAR_WIDTH + 1] = ']';

        return bar;
    }

    pub fn tick(self: *ProgressBar) void {
        self.current_iter += 1;

        if (self.current_iter % self.update_differential == 0) {
            self.bar_string[self.current_idx] = '=';
            self.bar_string[self.current_idx + 1] = '>';
            self.current_idx = @min(self.total_updates, self.current_idx + 1);

            self.display();
        }

        if (self.current_iter == self.total_iters - 1) {
            ProgressBar.finish();
        }
    }

    pub fn update(
        self: *ProgressBar, 
        current_iter: usize,
        message: ?[]const u8,
        ) void {
        self.current_iter = current_iter;
        self.current_idx  = 1 + @as(usize, @intFromFloat(BAR_WIDTH * @as(f64, @floatFromInt(self.current_iter)) / @as(f64, @floatFromInt(self.total_iters))));
        self.current_idx = @min(self.total_updates, self.current_idx);

        for (1..self.current_idx) |i| {
            self.bar_string[i] = '=';
        }
        self.bar_string[self.current_idx] = '>';

        self.display(message);

        if (self.current_iter >= self.total_iters - 1) {
            self.finish();
        }
    }
    
    fn display(self: *ProgressBar, message: ?[]const u8) void {
        switch (self.display_units) {
            .None => {
                if (message) |m| {
                    std.debug.print(
                        "{s} | Docs Processed: {d}/{d} {s}\r", 
                        .{m, self.current_iter, self.total_iters, self.bar_string}
                        );
                } else {
                    std.debug.print(
                        "Docs Processed: {d}/{d} {s}\r", 
                        .{self.current_iter, self.total_iters, self.bar_string}
                        );
                }
            },
            .K => {
                if (message) |m| {
                    std.debug.print(
                        "{s} | Docs Processed: {d}/{d}K {s}\r", 
                        .{
                            m, 
                            @divFloor(self.current_iter, 1_000), 
                            @divFloor(self.total_iters, 1_000),
                            self.bar_string,
                        }
                        );
                } else {
                    std.debug.print(
                        "Docs Processed: {d}/{d}K {s}\r", 
                        .{
                            @divFloor(self.current_iter, 1_000), 
                            @divFloor(self.total_iters, 1_000),
                            self.bar_string,
                        }
                    );
                }
            },
            .M => {
                if (message) |m| {
                    std.debug.print(
                        "{s} | Docs Processed: {d}/{d}M {s}\r", 
                        .{
                            m, 
                            @divFloor(self.current_iter, 1_000_000), 
                            @divFloor(self.total_iters, 1_000_000),
                            self.bar_string,
                        }
                        );
                } else {
                    std.debug.print(
                        "Docs Processed: {d}/{d}M {s}\r", 
                        .{
                            @divFloor(self.current_iter, 1_000_000), 
                            @divFloor(self.total_iters, 1_000_000),
                            self.bar_string,
                        }
                    );
                }
            },
        }
    }

    fn finish(self: *ProgressBar) void {
        self.current_iter = self.total_iters;
        self.current_idx = BAR_WIDTH;
        self.bar_string[self.current_idx - 1] = '=';
        self.bar_string[self.current_idx] = '=';
        self.display(null);
        std.debug.print("\n", .{});
    }
};

test "pbar" {
    var pbar = ProgressBar.init(1000, .None);

    try std.testing.expectEqual(1000, pbar.total_iters);
    try std.testing.expectEqual(120, pbar.total_updates);
    try std.testing.expectEqual(8, pbar.update_differential);

    for (0..1000) |i| {
        pbar.update(i, null);
    }
}
