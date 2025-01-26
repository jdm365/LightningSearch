const std = @import("std");
const csv = @import("csv.zig");

const string_utils = @import("string_utils.zig");

const IndexManager    = @import("index_manager.zig").IndexManager;
const MAX_NUM_RESULTS = @import("index_manager.zig").MAX_NUM_RESULTS;
const FileType        = @import("file_utils.zig").FileType;

var float_buf: [1000][64]u8 = undefined;

pub const TermPos = struct {
    start_pos: u32,
    field_len: u32,
};

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

pub const QueryHandlerLocal = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    query_map: std.StringHashMap([]const u8),
    search_cols: std.ArrayList([]u8),
    allocator: std.mem.Allocator,

    pub fn init(
        index_manager: *IndexManager,
        allocator: std.mem.Allocator,
    ) !QueryHandlerLocal {
        return QueryHandlerLocal{
            .index_manager = index_manager,
            .boost_factors = std.ArrayList(f32).init(allocator),
            .query_map = std.StringHashMap([]const u8).init(allocator),
            .search_cols = undefined,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueryHandlerLocal) void {
        self.index_manager.deinit();
        self.boost_factors.deinit();
        self.query_map.deinit();
        self.search_cols.deinit();
    }

    export fn readHeader(
        self: *QueryHandlerLocal,
        filename: [*:0]const u8,
    ) void {
        self.index_manager.readHeader(
            std.mem.span(filename),
            FileType.CSV,
            ) catch {
            @panic("Failed to read CSV header.\n");
        };
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

pub export fn getQueryHandlerLocal() *anyopaque {
    const index_manager = global_arena.allocator().create(IndexManager) catch @panic("BAD\n");
    index_manager.* = IndexManager.init() catch @panic("BAD\n");

    const query_handler = global_arena.allocator().create(QueryHandlerLocal) catch @panic("BAD\n");
    query_handler.* = QueryHandlerLocal.init(
        index_manager,
        global_arena.allocator(),
    ) catch @panic("BAD\n");

    return @ptrCast(query_handler);
}

export fn scanCSVFile(query_handler: *QueryHandlerLocal) void {
    std.debug.assert(query_handler.index_manager.col_map.num_keys > 0);

    query_handler.index_manager.scanCSVFile() catch {
        @panic("Error scanning file.\n");
    };
}

export fn indexFile(query_handler: *QueryHandlerLocal) void {
    std.debug.assert(query_handler.index_manager.search_cols.count() > 0);

    query_handler.index_manager.indexFile() catch {
        @panic("Error indexing file.\n");
    };
}

export fn addSearchCol(
    query_handler: *QueryHandlerLocal,
    col_name: [*:0]const u8,
) void {
    const upper_col = query_handler.index_manager.string_arena.allocator().dupe(
        u8,
        std.mem.span(col_name)
    ) catch @panic("Failed to copy input string.\n");
    string_utils.stringToUpper(upper_col.ptr, upper_col.len);

    query_handler.index_manager.addSearchCol(upper_col) catch {
        @panic("Failed to add search col.\n");
    };
    query_handler.query_map.put(upper_col, "") catch {
        @panic("Failed to add search col to query_map.\n");
    };
    query_handler.boost_factors.append(1.0) catch {
        @panic("Failed to add boost factor.\n");
    };
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
        const num_cols = m.*.col_map.num_keys;
        const start_idx = doc_idx * num_cols;
        const end_idx   = start_idx + num_cols;

        for (0.., start_idx..end_idx) |col_idx, i| {
            start_positions[i] = m.*.result_positions[doc_idx][col_idx].start_pos;
            lengths[i] = m.*.result_positions[doc_idx][col_idx].field_len;
        }
        result_buffers[doc_idx] = m.*.result_strings[doc_idx].items.ptr;
    }
}

pub export fn getColumnNames(
    query_handler: *const QueryHandlerLocal, 
    column_names: [*][*:0]u8,
    num_columns: *u32,
    ) void {
    const num_cols = query_handler.index_manager.col_map.num_keys;
    num_columns.* = @truncate(num_cols);

    // var idx: usize = 0;
    var iterator = query_handler.index_manager.col_map.iterator() catch {
        @panic("Error reading column keys.\n");
    };
    while (iterator.next() catch {@panic("Error reading column keys.\n");}) |*item| {
        const idx = item.value;
        @memcpy(column_names[idx], item.key);
        column_names[idx][item.key.len] = 0;
        // idx += 1;
    }
}

pub export fn getSearchColumns(
    query_handler: *const QueryHandlerLocal, 
    col_mask: [*]u8,
    ) void {
    var iterator = query_handler.index_manager.search_cols.iterator();
    while (iterator.next()) |item| {
        col_mask[item.key_ptr.*] = 1;
    }
}

pub export fn getIndexingProgress(
    query_handler: *const QueryHandlerLocal, 
    ) u64 {
    return @intCast(query_handler.index_manager.last_progress);
}

pub export fn getNumDocs(
    query_handler: *const QueryHandlerLocal, 
    ) u64 {
    var num_docs: usize = 0;
    for (query_handler.index_manager.index_partitions) |*p| {
        num_docs += p.line_offsets.len - 1;
    }
    return @intCast(num_docs);
}


test "csv_parse" {
    const csv_line = "26859,13859,1,1,WoM27813813,006,Under My Skin (You Go To My Head (Set One)),02:44,David McAlmont,You_Go_To_My_Head_(Set_One),2005,,";

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result_positions = try allocator.alloc(TermPos, 12);
    defer allocator.free(result_positions);

    try csv.parseRecordCSV(csv_line, result_positions);

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
