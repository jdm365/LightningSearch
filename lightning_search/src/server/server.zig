const std = @import("std");
const zap = @import("zap");

const csv          = @import("../parsing/csv.zig");
const string_utils = @import("../utils/string_utils.zig");
const SHM = @import("../indexing/index.zig").SHM;

const MAX_NUM_RESULTS = @import("../indexing/index.zig").MAX_NUM_RESULTS;
const IndexManager    = @import("../indexing/index_manager.zig").IndexManager;
const FileType        = @import("../storage/file_utils.zig").FileType;

var FLOAT_BUF: [1000][64]u8 = undefined;
var URL_BUFFER: [4096]u8 = undefined;

const K: usize = 10;

pub const TermPos = struct {
    start_pos: u32,
    field_len: u32,
};


pub fn urlDecode(
    allocator: std.mem.Allocator, 
    input: []const u8,
    ) ![]u8 {
    var output = std.ArrayListUnmanaged(u8){};

    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hex = input[i + 1 .. i + 3];
            const value = try std.fmt.parseInt(u8, hex, 16);
            try output.append(allocator, value);
            i += 3;
        } else {
            try output.append(allocator, input[i]);
            i += 1;
        }
    }

    return try output.toOwnedSlice(allocator);
}

pub fn csvLineToJson(
    allocator: std.mem.Allocator,
    csv_line: std.ArrayListUnmanaged(u8),
    term_positions: []TermPos,
    columns: std.ArrayListUnmanaged([]const u8),
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |idx, entry| {
        const field_value = csv_line.items[
            entry.start_pos..entry.start_pos + entry.field_len
        ];
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
    csv_line: std.ArrayListUnmanaged(u8),
    term_positions: []TermPos,
    columns: std.ArrayListUnmanaged([]const u8),
    score: f32,
    idx: usize,
) !std.json.Value {
    var json_object = std.json.ObjectMap.init(allocator);
    errdefer json_object.deinit();

    for (0.., term_positions) |i, entry| {
        const column_name = columns.items[i];
        if (std.mem.eql(u8, "SCORE", column_name)) continue;
        const field_value = csv_line.items[
            entry.start_pos..entry.start_pos + entry.field_len
        ];

        try json_object.put(
            column_name,
            std.json.Value{
                .string = try allocator.dupe(u8, field_value),
            },
        );
    }
    const score_str = try std.fmt.bufPrint(&FLOAT_BUF[idx], "{d:.4}", .{score});
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

pub const QueryHandlerZap = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    query_map: SHM,

    json_objects: std.ArrayListUnmanaged(std.json.Value),
    output_buffer: std.ArrayListUnmanaged(u8),
    column_names: std.ArrayListUnmanaged([]const u8),

    zap_router: zap.Router,
    zap_listener: zap.HttpListener,

    html_file: [*:0]const u8,
    css_file: [*:0]const u8,
    js_file: [*:0]const u8,

    pub fn init(
        index_manager: *IndexManager,
        boost_factors: std.ArrayList(f32),
    ) !QueryHandlerZap {
        // Use scratch arena for per query requests.

        const handler = QueryHandlerZap{
            .index_manager = index_manager,
            .boost_factors = boost_factors,
            .query_map     = SHM.init(
                index_manager.stringArena(),
                // index_manager.scratchArena(),
                ),
            .json_objects  = std.ArrayListUnmanaged(std.json.Value){},
            .output_buffer = std.ArrayListUnmanaged(u8){},
            .column_names   = std.ArrayListUnmanaged([]const u8){},

            .zap_router   = undefined,
            .zap_listener = undefined,

            .html_file    = @embedFile("../web/index.html"),
            .css_file     = @embedFile("../web/style.css"),
            .js_file      = @embedFile("../web/app.js"),
        };
        return handler;
    }

    pub fn deinit(self: *QueryHandlerZap) void {
        for (self.json_objects.items) |*json| {
            json.object.deinit();
        }
    }

    pub fn initRouter(self: *QueryHandlerZap) !void {
        self.zap_router = zap.Router.init(self.index_manager.gpa(), .{});

        try self.zap_router.handle_func(
            "/", 
            self, 
            &QueryHandlerZap.serveStatic,
            );
        try self.zap_router.handle_func(
            "/index.html", 
            self, 
            &QueryHandlerZap.serveStatic,
            );
        try self.zap_router.handle_func(
            "/app.js", 
            self, 
            &QueryHandlerZap.serveStatic,
            );
        try self.zap_router.handle_func(
            "/style.css", 
            self, 
            &QueryHandlerZap.serveStatic,
            );

        try self.zap_router.handle_func(
            "/search", 
            self, 
            &QueryHandlerZap.on_request,
            );
        try self.zap_router.handle_func(
            "/get_columns", 
            self, 
            &QueryHandlerZap.getColumns,
            );
        try self.zap_router.handle_func(
            "/get_search_columns", 
            self, 
            &QueryHandlerZap.getSearchColumns,
            );
        try self.zap_router.handle_func(
            "/healthcheck", 
            self, 
            &QueryHandlerZap.healthcheck,
            );
    }

    pub fn serve(self: *QueryHandlerZap) !void {
        // self.writeHTMLFiles();

        try self.initRouter();

        self.zap_listener = zap.HttpListener.init(.{
            .port = 5000,
            .on_request = self.zap_router.on_request_handler(),
            .log = true,
        });
        try self.zap_listener.listen();
        std.debug.print(
            "LightningSearch listening on http://localhost:5000/\n",
            .{},
        );

        // self.openIndexHTML();

        zap.start(.{
            .threads = 1,
            .workers = 1,
        });
    }

    pub fn serveStatic(self: *QueryHandlerZap, r: zap.Request) !void {
        const path = r.path orelse "/";

        if (std.mem.eql(u8, path, "/") or std.mem.eql(u8, path, "/index.html")) {
            try r.setHeader("Content-Type", "text/html");
            try r.sendBody(std.mem.span(self.html_file));

        } else if (std.mem.eql(u8, path, "/style.css")) {
            try r.setHeader("Content-Type", "text/css");
            try r.sendBody(std.mem.span(self.css_file));

        } else if (std.mem.eql(u8, path, "/app.js")) {
            try r.setHeader("Content-Type", "application/javascript");
            try r.sendBody(std.mem.span(self.js_file));

        } else {
            r.setStatus(zap.http.StatusCode.not_found);
            try r.sendBody("404 Not Found");
        }
    }

    pub fn openIndexHTML(self: *QueryHandlerZap) void {
        var cmd = std.process.Child.init(
            &[_][]const u8{"xdg-open", "http://localhost:5000/"},
            self.index_manager.scratchArena(),
        );
        cmd.spawn() catch @panic("Failed to spawn process.\n");
        _ = cmd.wait() catch @panic("Failed to open file.\n");
    }

    pub inline fn writeFileToTmpDir(
        self: *QueryHandlerZap,
        filename: []const u8,
        data: []const u8,
    ) void {
        const full_path = std.mem.concat(
            self.index_manager.scratchArena(),
            u8, 
            &[_][]const u8{self.index_manager.file_data.tmp_dir, filename}
            ) catch {
            @panic("Failed to concatenate path.\n");
        };

        var output_file = std.fs.cwd().createFile(
            full_path, 
            .{ .read = false },
            ) catch {
            @panic("Failed to create file.\n");
        };
        _ = output_file.write(data) catch {
            @panic("Failed to write file.\n");
        };
    }

    pub fn writeHTMLFiles(self: *QueryHandlerZap) void {
        self.writeFileToTmpDir(
            "index.html",
            std.mem.span(self.html_file),
        );
        self.writeFileToTmpDir(
            "style.css",
            std.mem.span(self.css_file),
        );
        self.writeFileToTmpDir(
            "app.js",
            std.mem.span(self.js_file),
        );
    }

    pub fn on_request(self: *QueryHandlerZap, r: zap.Request) !void {
        _ = self.index_manager.allocators.scratch_arena.reset(.retain_capacity);

        try r.setHeader("Access-Control-Allow-Origin", "*");

        self.output_buffer.clearRetainingCapacity();
        self.json_objects.clearRetainingCapacity();

        const start = std.time.microTimestamp();

        if (r.query) |query| {
            parseKeys(
                query,
                self.query_map,
                // self.index_manager.scratchArena(),
                self.index_manager.stringArena(),
            );

            // Do search.
            try self.index_manager.query(
                self.query_map,
                K,
                self.boost_factors,
                );

            for (0..self.index_manager.query_state.results_arrays[0].count) |idx| {
                try self.json_objects.append(
                    // self.index_manager.scratchArena(),
                    self.index_manager.stringArena(),
                    try csvLineToJsonScore(
                        // self.index_manager.scratchArena(),
                        self.index_manager.stringArena(),
                        self.index_manager.query_state.result_strings[idx],
                        self.index_manager.query_state.result_positions[idx],
                        self.column_names,
                        self.index_manager.query_state.results_arrays[0].scores[idx],
                        idx,
                    ));
            }
            const end = std.time.microTimestamp();
            const time_taken_us = end - start;

            var response = std.json.Value{
                .object = std.StringArrayHashMap(std.json.Value).init(
                    // self.index_manager.scratchArena()
                    self.index_manager.stringArena()
                    ),
            };

            try response.object.put(
                "results",
                // std.json.Value{ .array = self.json_objects.toManaged(self.index_manager.scratchArena()) },
                std.json.Value{ 
                    .array = self.json_objects.toManaged(
                        self.index_manager.stringArena()
                        ) 
                },
            );
            try response.object.put(
                "time_taken_us",
                std.json.Value{ .integer = time_taken_us },
            );

            try std.json.stringify(
                response,
                .{},
                // self.output_buffer.writer(self.index_manager.scratchArena()),
                self.output_buffer.writer(self.index_manager.stringArena()),
            );

            std.debug.print(
                "{s}\n", 
                .{self.output_buffer.items[self.output_buffer.items.len - 1 ..]},
            );
            r.sendJson(self.output_buffer.items) catch |err| {
                std.debug.print("ERROR SENDING JSON - {any}\n", .{err});
            };
        }
    }

    pub fn getColumns(
        self: *QueryHandlerZap,
        r: zap.Request,
    ) !void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch |err| {
            std.debug.print("Error setting header: {any}\n", .{err});
        };

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(
                // self.index_manager.scratchArena(),
                self.index_manager.stringArena(),
                ),
        };

        var json_cols = std.ArrayListUnmanaged(std.json.Value).initCapacity(
            // self.index_manager.scratchArena(), 
            self.index_manager.stringArena(), 
            self.index_manager.columns.num_keys,
            ) catch {
            std.debug.print("ERROR GETTING COLUMNS\n", .{});
            @panic("Failed to initialize json_cols.\n");
        };

        json_cols.resize(
            self.index_manager.stringArena(), 
            self.index_manager.columns.num_keys,
            ) catch return;

        var it = self.index_manager.columns.iterator() catch {
            std.debug.print("ERROR GETTING COLUMNS\n", .{});
            return;
        };
        while (it.next() catch @panic("Column iterator failed")) |val| {
            json_cols.items[val.value] = std.json.Value{
                .string = val.key,
            };
        }

        json_cols.append(
            self.index_manager.stringArena(),
            std.json.Value{
                .string = "SCORE",
        }) catch return;

        for (json_cols.items) |*json| {
            self.column_names.append(
                self.index_manager.stringArena(),
                json.string,
            ) catch return;
        }

        response.object.put(
            "columns",
            std.json.Value{ 
                .array=json_cols.toManaged(self.index_manager.stringArena()) 
            },
        ) catch return;

        std.json.stringify(
            response,
            .{},
            self.output_buffer.writer(self.index_manager.stringArena()),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch return;
    }

    pub fn getSearchColumns(
        self: *QueryHandlerZap,
        r: zap.Request,
    ) !void {
        r.setHeader("Access-Control-Allow-Origin", "*") catch |err| {
            std.debug.print("Error setting header: {any}\n", .{err});
        };

        self.output_buffer.clearRetainingCapacity();

        var response = std.json.Value{
            .object = std.StringArrayHashMap(std.json.Value).init(
                self.index_manager.stringArena(),
                ),
        };

        var json_cols = std.ArrayListUnmanaged(std.json.Value){};
        json_cols.resize(
            // self.index_manager.scratchArena(), 
            self.index_manager.stringArena(), 
            self.index_manager.search_col_idxs.items.len,
            ) catch unreachable;

        for (0.., self.index_manager.search_col_idxs.items) |idx, col_idx| {
            json_cols.items[idx] = std.json.Value{
                .string = self.column_names.items[col_idx],
            };

            self.query_map.put(
                self.column_names.items[col_idx],
                "",
            ) catch unreachable;
        }

        response.object.put(
            "columns",
            std.json.Value{ 
                .array = json_cols.toManaged(self.index_manager.stringArena()) 
            },
        ) catch @panic("put failed");

        std.json.stringify(
            response,
            .{},
            // self.output_buffer.writer(self.index_manager.scratchArena()),
            self.output_buffer.writer(self.index_manager.stringArena()),
        ) catch unreachable;

        r.sendJson(self.output_buffer.items) catch unreachable;
    }

    pub fn healthcheck(_: *QueryHandlerZap, r: zap.Request) !void {
        r.setStatus(zap.http.StatusCode.ok);
        r.setHeader("Access-Control-Allow-Origin", "*") catch {};
        r.markAsFinished(true);
        r.sendBody("") catch {};
    }

    pub fn parseKeys(
        raw_string: []const u8,
        query_map: SHM,
        allocator: std.mem.Allocator,
    ) void {
        // Format key=value&key=value
        var count: usize = 0;
        var idx: usize = 0;

        while (idx < raw_string.len) {
            if (raw_string[idx] == '=') {
                idx += 1;

                const result = query_map.getPtr(
                    urlDecode(allocator, URL_BUFFER[0..count]) catch {
                        @panic("Failed to copy input string.\n");
                    }
                );

                count = 0;
                while ((idx < raw_string.len) and (raw_string[idx] != '&')) {
                    if (raw_string[idx] == '+') {
                        URL_BUFFER[count] = ' ';
                        count += 1;
                        idx += 1;
                        continue;
                    }
                    URL_BUFFER[count] = std.ascii.toUpper(raw_string[idx]);
                    count += 1;
                    idx   += 1;
                }
                if (result != null) {
                    const value_copy = allocator.dupe(
                        u8, 
                        urlDecode(allocator, URL_BUFFER[0..count]) catch {
                            @panic("Failed to copy input string.\n");
                        }
                    ) catch @panic("Failed to copy input string.\n");
                    result.?.* = value_copy;
                }
                count = 0;
                idx += 1;
                continue;
            }
            URL_BUFFER[count] = std.ascii.toUpper(raw_string[idx]);
            count += 1;
            idx   += 1;
        }
    }
};



pub const QueryHandlerLocal = struct {
    index_manager: *IndexManager,
    boost_factors: std.ArrayList(f32),
    // query_map: std.StringHashMap([]const u8),
    query_map: SHM,
    search_cols: std.ArrayList([]u8),
    allocator: std.mem.Allocator,

    pub fn init(
        index_manager: *IndexManager,
        allocator: std.mem.Allocator,
    ) !QueryHandlerLocal {
        return QueryHandlerLocal{
            .index_manager = index_manager,
            .boost_factors = std.ArrayList(f32).init(allocator),
            .query_map = SHM.init(allocator),
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

    pub export fn readHeader(
        self: *QueryHandlerLocal,
        filename: [*:0]const u8,
    ) void {
        if (std.mem.endsWith(u8, std.mem.span(filename), ".csv")) {
            self.index_manager.readHeader(
                std.mem.span(filename),
                FileType.CSV,
                ) catch {
                @panic("Failed to read CSV header.\n");
            };
        } else if (std.mem.endsWith(u8, std.mem.span(filename), ".json")) {
            self.index_manager.readHeader(
                std.mem.span(filename),
                FileType.JSON,
                ) catch {
                @panic("Failed to read CSV header.\n");
            };
        }
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
            K,
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
    index_manager.* = IndexManager.init(global_arena.allocator()) catch @panic("BAD\n");

    const query_handler = global_arena.allocator().create(QueryHandlerLocal) catch @panic("BAD\n");
    query_handler.* = QueryHandlerLocal.init(
        index_manager,
        global_arena.allocator(),
    ) catch @panic("BAD\n");

    return @ptrCast(query_handler);
}

pub export fn scanFile(query_handler: *QueryHandlerLocal) void {
    std.debug.assert(query_handler.index_manager.columns.num_keys > 0);

    query_handler.index_manager.scanFile() catch {
        @panic("Error scanning file.\n");
    };
}

pub export fn indexFile(query_handler: *QueryHandlerLocal) void {
    std.debug.assert(query_handler.index_manager.search_col_idxs.items.len > 0);

    query_handler.index_manager.indexFile() catch {
        @panic("Error indexing file.\n");
    };
}

pub export fn addSearchCol(
    query_handler: *QueryHandlerLocal,
    col_name: [*:0]const u8,
) void {
    const upper_col = query_handler.index_manager.stringArena().dupe(
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

    std.debug.assert(m.*.query_state.results_arrays[0].count < MAX_NUM_RESULTS);
    result_count.* = @intCast(m.*.query_state.results_arrays[0].count);

    for (0..result_count.*) |doc_idx| {
        const num_cols = m.*.columns.num_keys;
        const start_idx = doc_idx * num_cols;
        const end_idx   = start_idx + num_cols;

        for (0.., start_idx..end_idx) |col_idx, i| {
            start_positions[i] = m.*.query_state.result_positions[doc_idx][col_idx].start_pos;
            lengths[i]         = m.*.query_state.result_positions[doc_idx][col_idx].field_len;
        }
        result_buffers[doc_idx] = m.*.query_state.result_strings[doc_idx].items.ptr;
    }
}

pub export fn getColumnNames(
    query_handler: *const QueryHandlerLocal, 
    column_names: [*][*:0]u8,
    num_columns: *u32,
    ) void {
    const num_cols = query_handler.index_manager.columns.num_keys;
    num_columns.* = @truncate(num_cols);

    var iterator = query_handler.index_manager.columns.iterator() catch {
        @panic("Error reading column keys.\n");
    };
    defer iterator.deinit();

    while (iterator.next() catch {@panic("Error reading column keys.\n");}) |*item| {
        const idx = item.value;
        @memcpy(column_names[idx], item.key);
        column_names[idx][item.key.len] = 0;
    }
}

pub export fn getSearchColumns(
    query_handler: *const QueryHandlerLocal, 
    col_mask: [*]u8,
    ) void {
    for (query_handler.index_manager.search_col_idxs.items) |idx| {
        col_mask[idx] = 1;
    }
}

pub export fn getIndexingProgress(
    query_handler: *const QueryHandlerLocal, 
    ) u64 {
    return @intCast(query_handler.index_manager.indexing_state.last_progress);
}

pub export fn getNumDocs(
    query_handler: *const QueryHandlerLocal, 
    ) u64 {
    return @intCast(query_handler.index_manager.partitions.row_offsets[
        query_handler.index_manager.partitions.row_offsets.len - 1
    ]);
}


test "csv_parse" {
    const _csv_line = "26859,13859,1,1,WoM27813813,006,Under My Skin (You Go To My Head (Set One)),02:44,David McAlmont,You_Go_To_My_Head_(Set_One),2005,,";
    const csv_line = std.ArrayListUnmanaged(u8).fromOwnedSlice(
        std.mem.bytesAsSlice(u8, @constCast(_csv_line))
        );

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const result_positions = try allocator.alloc(TermPos, 12);
    defer allocator.free(result_positions);

    try csv.parseRecordCSV(_csv_line, result_positions);

    var columns = std.ArrayListUnmanaged([]const u8){};
    defer columns.deinit(allocator);

    try columns.appendSlice(allocator, &[_][]const u8{
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
        std.debug.print(
            "Term: {s}\n", 
            .{
                csv_line.items[result_positions[col_idx].start_pos..result_positions[col_idx].start_pos + result_positions[col_idx].field_len]
            },
            );
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

    try columns.append(allocator, "SCORE");

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
