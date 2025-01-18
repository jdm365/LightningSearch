const std = @import("std");
const parseRecordCSV = @import("csv.zig").parseRecordCSV;
const zap = @import("zap");

const IndexManager    = @import("index_manager.zig").IndexManager;
const MAX_NUM_RESULTS = @import("index_manager.zig").MAX_NUM_RESULTS;

var float_buf: [1000][64]u8 = undefined;

pub const TermPos = struct {
    start_pos: u32,
    field_len: u32,
};

pub fn urlDecode(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var output = std.ArrayList(u8).init(allocator);
    defer output.deinit();

    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hex = input[i + 1 .. i + 3];
            const value = try std.fmt.parseInt(u8, hex, 16);
            try output.append(value);
            i += 3;
        } else {
            try output.append(input[i]);
            i += 1;
        }
    }

    return try output.toOwnedSlice();
}

pub fn csvLineToJson(
    allocator: std.mem.Allocator,
    csv_line: []const u8,
    term_positions: []TermPos,
    columns: std.ArrayList([]const u8),
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |idx, entry| {
        const field_value = csv_line[entry.start_pos..entry.start_pos + entry.field_len];
        const column_name = columns.items[idx];

        try json_object.put(
            column_name,
            std.json.Value{
                .string = try allocator.dupe(u8, field_value),
            },
        );
    }

    return std.json.Value{
        .object = json_object,
    };
}

pub fn csvLineToJsonScore(
    allocator: std.mem.Allocator,
    csv_line: []const u8,
    term_positions: []TermPos,
    columns: std.ArrayList([]const u8),
    score: f32,
    idx: usize,
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |i, entry| {
        const column_name = columns.items[i];
        if (std.mem.eql(u8, "SCORE", column_name)) continue;
        const field_value = csv_line[entry.start_pos..entry.start_pos + entry.field_len];

        try json_object.put(
            column_name,
            std.json.Value{
                .string = try allocator.dupe(u8, field_value),
            },
        );
    }
    const score_str = try std.fmt.bufPrint(&float_buf[idx], "{d:.4}", .{score});
    try json_object.put(
        "SCORE",
        std.json.Value{
            .string = score_str,
        },
    );

    return std.json.Value{
        .object = json_object,
    };
}


pub const QueryHandler = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    query_map: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
    json_objects: std.ArrayList(std.json.Value),
    output_buffer: std.ArrayList(u8),

    pub fn init(
        index_manager: *IndexManager,
        boost_factors: std.ArrayList(f32),
        query_map: std.StringHashMap([]const u8),
        allocator: std.mem.Allocator,
    ) !QueryHandler {
        return QueryHandler{
            .index_manager = index_manager,
            .boost_factors = boost_factors,
            .query_map = query_map,
            .allocator = allocator,
            .json_objects = try std.ArrayList(std.json.Value).initCapacity(allocator, MAX_NUM_RESULTS),
            .output_buffer = try std.ArrayList(u8).initCapacity(allocator, 16384),
        };
    }

    pub fn deinit(self: *QueryHandler) void {
        for (self.json_objects.items) |*json| {
            json.object.deinit();
        }
        self.json_objects.deinit();
        self.output_buffer.deinit();
    }

    pub fn on_request(
        self: *QueryHandler,
        r: zap.Request,
        ) void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};

        self.output_buffer.clearRetainingCapacity();
        self.json_objects.clearRetainingCapacity();

        const start = std.time.milliTimestamp();

        if (r.query) |query| {
            // try parse_keys(
                // query,
                // self.query_map,
                // self.index_manager.string_arena.allocator(),
            // );
            parse_keys(
                query,
                self.query_map,
                self.index_manager.string_arena.allocator(),
            ) catch return;

            // Do search.
            self.index_manager.query(
                self.query_map,
                10,
                self.boost_factors,
                ) catch return;

            for (0..self.index_manager.results_arrays[0].count) |idx| {
                self.json_objects.append(csvLineToJsonScore(
                    self.index_manager.string_arena.allocator(),
                    self.index_manager.result_strings[idx].items,
                    self.index_manager.result_positions[idx],
                    self.index_manager.cols,
                    self.index_manager.results_arrays[0].items[idx].score,
                    idx,
                    ) catch return) catch return;
            }
            const end = std.time.milliTimestamp();
            const time_taken_ms = end - start;

            var response = std.json.Value{
                .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
            };
            defer response.object.deinit();

            response.object.put(
                "results",
                std.json.Value{ .array = self.json_objects },
            ) catch return;
            response.object.put(
                "time_taken_ms",
                std.json.Value{ .integer = time_taken_ms },
            ) catch return;

            std.json.stringify(
                response,
                .{},
                self.output_buffer.writer(),
            ) catch unreachable;

            r.sendJson(self.output_buffer.items) catch return;
        }
    }

    pub fn get_columns(
        self: *QueryHandler,
        r: zap.Request,
    ) void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch |err| {
            std.debug.print("Error setting header: {?}\n", .{err});
        };

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
        };

        var json_cols = std.ArrayList(std.json.Value).initCapacity(
            self.allocator, 
            self.index_manager.cols.items.len
            ) catch return;
        defer json_cols.deinit();

        for (self.index_manager.cols.items) |col| {
            json_cols.append(std.json.Value{
                .string = col,
            }) catch return;
        }

        // Swap search_cols to be first.
        var cntr: usize = 0;
        var iterator = self.index_manager.search_cols.iterator();
        while (iterator.next()) |item| {
            const csv_idx = item.value_ptr.*.csv_idx;

            const tmp = json_cols.items[csv_idx];
            json_cols.items[csv_idx] = json_cols.items[cntr];
            json_cols.items[cntr] = tmp;

            cntr += 1;
        }
        json_cols.append(std.json.Value{
            .string = "SCORE",
        }) catch return;
        const csv_idx = json_cols.items.len - 1;
        const tmp = json_cols.items[csv_idx];
        json_cols.items[csv_idx] = json_cols.items[cntr];
        json_cols.items[cntr] = tmp;
        

        response.object.put(
            "columns",
            std.json.Value{ .array = json_cols },
        ) catch return;

        std.json.stringify(
            response,
            .{},
            self.output_buffer.writer(),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch return;
    }

    pub fn get_search_columns(
        self: *QueryHandler,
        r: zap.Request,
    ) void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch |err| {
            std.debug.print("Error setting header: {?}\n", .{err});
        };

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(self.allocator),
        };

        var json_cols = std.ArrayList(std.json.Value).initCapacity(
            self.allocator, 
            self.index_manager.search_cols.count(),
            ) catch unreachable;
        defer json_cols.deinit();

        var iterator = self.index_manager.search_cols.iterator();
        while (iterator.next()) |item| {
            json_cols.append(std.json.Value{
                .string = item.key_ptr.*,
            }) catch @panic("This part failed\n");
        }


        response.object.put(
            "columns",
            std.json.Value{ .array = json_cols },
        ) catch @panic("put failed");

        std.json.stringify(
            response,
            .{},
            self.output_buffer.writer(),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch unreachable;
    }

    pub fn healthcheck(_: *QueryHandler, r: zap.Request) void {
        r.setStatus(zap.StatusCode.ok);
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};
        r.markAsFinished(true);
        r.sendBody("") catch {};
    }

    pub fn parse_keys(
        raw_string: []const u8,
        query_map: std.StringHashMap([]const u8),
        allocator: std.mem.Allocator,
    ) !void {
        // Format key=value&key=value
        var scratch_buffer: [4096]u8 = undefined;
        var count: usize = 0;
        var idx: usize = 0;

        while (idx < raw_string.len) {
            if (raw_string[idx] == '=') {
                idx += 1;

                const result = query_map.getPtr(
                    try urlDecode(allocator, scratch_buffer[0..count])
                    );

                count = 0;
                while ((idx < raw_string.len) and (raw_string[idx] != '&')) {
                    if (raw_string[idx] == '+') {
                        scratch_buffer[count] = ' ';
                        count += 1;
                        idx += 1;
                        continue;
                    }
                    scratch_buffer[count] = std.ascii.toUpper(raw_string[idx]);
                    count += 1;
                    idx   += 1;
                }
                if (result != null) {
                    const value_copy = try allocator.dupe(
                        u8, 
                        try urlDecode(allocator, scratch_buffer[0..count]),
                        );
                    result.?.* = value_copy;
                }
                count = 0;
                idx += 1;
                continue;
            }
            scratch_buffer[count] = std.ascii.toUpper(raw_string[idx]);
            count += 1;
            idx   += 1;
        }
    }
};

pub const QueryHandlerLocal = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    query_map: std.StringHashMap([]const u8),
    search_cols: std.ArrayList([]u8),
    allocator: std.mem.Allocator,

    pub fn init(
        index_manager: *IndexManager,
        boost_factors: std.ArrayList(f32),
        query_map: std.StringHashMap([]const u8),
        search_cols: std.ArrayList([]u8),
        allocator: std.mem.Allocator,
    ) !QueryHandlerLocal {
        return QueryHandlerLocal{
            .index_manager = index_manager,
            .boost_factors = boost_factors,
            .query_map = query_map,
            .search_cols = search_cols,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueryHandlerLocal) void {
        self.index_manager.deinit();
        self.boost_factors.deinit();
        self.query_map.deinit();
        self.search_cols.deinit();
    }

    inline fn parseQueryString(
        self: *QueryHandlerLocal,
        query_string: []const u8,
        ) !void {
        // Format key=value
        var scratch_buffer: [4096]u8 = undefined;
        var count: usize = 0;
        var idx: usize = 0;

        while (idx < query_string.len) {
            if (query_string[idx] == '=') {
                idx += 1;

                const result = self.query_map.getPtr(
                    scratch_buffer[0..count]
                    );

                count = 0;
                while ((idx < query_string.len) and (query_string[idx] != '&')) {
                    scratch_buffer[count] = std.ascii.toUpper(query_string[idx]);
                    count += 1;
                    idx   += 1;
                }
                if (result != null) {
                    const value_copy = try self.allocator.dupe(
                        u8, 
                        scratch_buffer[0..count],
                        );
                    result.?.* = value_copy;
                }
                count = 0;
                idx += 1;
                continue;
            }
            scratch_buffer[count] = std.ascii.toUpper(query_string[idx]);
            count += 1;
            idx   += 1;
        }
    }

    pub fn search(
        self: *QueryHandlerLocal,
        query_string: [*:0]const u8,
        ) !void {
        try self.parseQueryString(std.mem.span(query_string));
        try self.index_manager.query(
            self.query_map,
            25,
            self.boost_factors,
            );
        std.debug.print("Finished query\n", .{});
    }
};


var global_arena: std.heap.ArenaAllocator = undefined;
pub export fn init_allocators() void {
    global_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    std.debug.print("Zig: Arena initialized\n", .{});
}

pub export fn deinit_allocators() void {
    global_arena.deinit();
}

pub export fn get_query_handler_local() *anyopaque {
    // const filename: []const u8 = "../tests/mb.csv";
    const filename: []const u8 = "../tests/mb_small.csv";

    var index_manager = global_arena.allocator().create(IndexManager) catch @panic("BAD\n");
    index_manager.* = IndexManager.init(filename) catch @panic("BAD\n");

    var arena = index_manager.string_arena.allocator();

    var search_cols = std.ArrayList([]u8).init(arena);
    search_cols.append(arena.dupe(u8, "TITLE") catch @panic("BAD\n")) catch @panic("BAD\n");
    search_cols.append(arena.dupe(u8, "ARTIST") catch @panic("BAD\n")) catch @panic("BAD\n");
    search_cols.append(arena.dupe(u8, "ALBUM") catch @panic("BAD\n")) catch @panic("BAD\n");

    index_manager.readHeader(&search_cols) catch @panic("BAD\n");
    index_manager.readFile() catch @panic("BAD\n");

    var query_map = std.StringHashMap([]const u8).init(arena);
    var boost_factors = std.ArrayList(f32).init(arena);

    for (search_cols.items) |col| {
        query_map.put(col, "") catch @panic("BAD\n");
        boost_factors.append(1.0) catch @panic("BAD\n");
    }

    const query_handler = global_arena.allocator().create(QueryHandlerLocal) catch @panic("BAD\n");
    query_handler.* = QueryHandlerLocal.init(
        index_manager,
        boost_factors,
        query_map,
        search_cols,
        arena,
    ) catch @panic("BAD\n");

    return @ptrCast(query_handler);
}

export fn search(
    query_handler: *QueryHandlerLocal, 
    query_string: [*:0]const u8,
    result_count: *u32,
    start_positions: [*]u32,
    lengths: [*]u32,
    result_buffers: [*][*]u8,
    ) void {
    query_handler.search(query_string) catch {
        std.debug.print("Search for {s} failed\n.", .{query_string});
        @panic("Search failed\n.");
    };

    const m = &query_handler.index_manager;

    std.debug.assert(m.*.results_arrays[0].count < MAX_NUM_RESULTS);
    result_count.* = @intCast(m.*.results_arrays[0].count);

    for (0..result_count.*) |doc_idx| {
        const start_idx = doc_idx * m.*.cols.items.len;
        const end_idx   = start_idx + m.*.cols.items.len;

        for (0.., start_idx..end_idx) |col_idx, i| {
            start_positions[i] = m.*.result_positions[doc_idx][col_idx].start_pos;
            lengths[i] = m.*.result_positions[doc_idx][col_idx].field_len;
        }
        result_buffers[doc_idx] = m.*.result_strings[doc_idx].items.ptr;
    }
}

pub export fn get_column_names(
    query_handler: *const QueryHandlerLocal, 
    column_names: [*][*:0]u8,
    num_columns: *u32,
    ) void {
    num_columns.* = @truncate(query_handler.index_manager.cols.items.len);
    for (0.., query_handler.index_manager.cols.items) |idx, item| {
        @memcpy(column_names[idx], item);
        column_names[idx][item.len] = 0;
    }
}


test "csv_parse" {
    const csv_line = "26859,13859,1,1,WoM27813813,006,Under My Skin (You Go To My Head (Set One)),02:44,David McAlmont,You_Go_To_My_Head_(Set_One),2005,,";

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result_positions = try allocator.alloc(TermPos, 12);
    defer allocator.free(result_positions);

    try parseRecordCSV(csv_line, result_positions);

    var columns = std.ArrayList([]const u8).init(allocator);
    defer columns.deinit();

    try columns.appendSlice(&[_][]const u8{
        "id",
        "artist_id",
        "album_id",
        "track_id",
        "track_name",
        "track_number",
        "track_title",
        "track_duration",
        "artist_name",
        "track_slug",
        "release_year",
        "track_genre",
    });

    for (0..12) |col_idx| {
        std.debug.print("start_pos: {d}, field_len: {d}\n", .{result_positions[col_idx].start_pos, result_positions[col_idx].field_len});
        std.debug.print("Term: {s}\n", .{csv_line[result_positions[col_idx].start_pos..result_positions[col_idx].start_pos + result_positions[col_idx].field_len]});
    }

    const json_object = try csvLineToJson(
        allocator,
        csv_line,
        result_positions,
        columns,
    );

    for (0..12) |col_idx| {
        const column_name = columns.items[col_idx];
        const field_value = json_object.object.get(column_name).?.string;
        std.debug.print("Column: {s}, Value: {s}\n", .{column_name, field_value});
    }

    try columns.append("SCORE");

    const json_object_score = try csvLineToJsonScore(
        allocator,
        csv_line,
        result_positions,
        columns,
        3.4,
        0,
    );

    for (0..13) |col_idx| {
        const column_name = columns.items[col_idx];
        const field_value = json_object_score.object.get(column_name).?.string;
        std.debug.print("Column: {s}, Value: {s}\n\n", .{column_name, field_value});
    }
}
