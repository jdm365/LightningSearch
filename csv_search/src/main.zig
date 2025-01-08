const std     = @import("std");
const builtin = @import("builtin");

const zap = @import("zap");

const QueryHandler = @import("server.zig").QueryHandler;

const IndexManager = @import("index_manager.zig").IndexManager;


fn bench(testing: bool) !void {
    // const filename: []const u8 = "../tests/mb_small.csv";
    const filename: []const u8 = "../tests/mb.csv";

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    // const allocator = std.heap.c_allocator;

    var search_cols = std.ArrayList([]u8).init(allocator);
    const title:  []u8 = try allocator.dupe(u8, "Title");
    const artist: []u8 = try allocator.dupe(u8, "ARTIST");
    const album:  []u8 = try allocator.dupe(u8, "ALBUM");

    try search_cols.append(title);
    try search_cols.append(artist);
    try search_cols.append(album);

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    try index_manager.printDebugInfo();

    defer {
        allocator.free(title);
        allocator.free(artist);
        allocator.free(album);

        search_cols.deinit();
        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }

    var query_map = std.StringHashMap([]const u8).init(allocator);
    defer query_map.deinit();

    try query_map.put("TITLE", "UNDER MY SKIN");
    try query_map.put("ARTIST", "FRANK SINATRA");
    try query_map.put("ALBUM", "LIGHTNING");

    var boost_factors = std.ArrayList(f32).init(allocator);
    defer boost_factors.deinit();

    try boost_factors.append(1.0);
    try boost_factors.append(1.0);
    try boost_factors.append(1.0);

    const num_queries: usize = if (testing) 1 else 5_000;

    const start_time = std.time.milliTimestamp();
    for (0..num_queries) |_| {
        try index_manager.query(
            query_map,
            10,
            boost_factors,
            );
    }
    const end_time = std.time.milliTimestamp();
    const execution_time_ms = (end_time - start_time);
    const qps = @as(f64, @floatFromInt(num_queries)) / @as(f64, @floatFromInt(execution_time_ms)) * 1000;

    std.debug.print("\n\n================================================\n", .{});
    std.debug.print("QUERIES PER SECOND: {d}\n", .{qps});
    std.debug.print("================================================\n", .{});
}

fn main_cli_runner() !void {
    // Embed files
    const index_html = @embedFile("index.html");
    const style_css = @embedFile("style.css");
    const table_js = @embedFile("table.js");

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    var filename: []const u8 = undefined;

    var search_cols = std.ArrayList([]u8).init(allocator);

    var idx: usize = 0;
    while (args.next()) |arg| {
        if (idx == 0) {
            // Skip binary name.
        } else if (idx == 1) {
            filename = arg;
        } else {
            try search_cols.append(try allocator.dupe(u8, arg));
        }
        idx += 1;
    }
    if (idx < 3) {
        @panic("Add filename - Usage: ./bm25 <filename> <search_col1> <search_col2> ...");
    }


    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    // Write files to disk
    var output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/index.html", 
        .{index_manager.tmp_dir}
        );
    var output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(index_html);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/style.css",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(style_css);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/table.js",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(table_js);

    var query_map = std.StringHashMap([]const u8).init(allocator);
    var boost_factors = std.ArrayList(f32).init(allocator);

    for (search_cols.items) |col| {
        try query_map.put(col, "");
        try boost_factors.append(1.0);
    }

    var query_handler = try QueryHandler.init(
        &index_manager,
        boost_factors,
        query_map,
        allocator,
    );


    var simple_router = zap.Router.init(
        allocator, 
        .{},
        );

    try simple_router.handle_func("/search", &query_handler, &QueryHandler.on_request);
    try simple_router.handle_func("/get_columns", &query_handler, &QueryHandler.get_columns);
    try simple_router.handle_func("/get_search_columns", &query_handler, &QueryHandler.get_search_columns);
    try simple_router.handle_func("/healthcheck", &query_handler, &QueryHandler.healthcheck);

    var listener = zap.HttpListener.init(.{
        .port = 5000,
        .on_request = simple_router.on_request_handler(),
        .log = true,
    });
    try listener.listen();

    const full_path = try std.mem.concat(
        index_manager.string_arena.allocator(),
        u8, 
        &[_][]const u8{index_manager.tmp_dir, "/index.html"}
        );

    var cmd = std.process.Child.init(&[_][]const u8{"open", full_path}, allocator);
    try cmd.spawn();
    _ = try cmd.wait();

    defer {
        for (search_cols.items) |col| {
            allocator.free(col);
        }

        search_cols.deinit();
        query_map.deinit();
        boost_factors.deinit();

        query_handler.deinit();
        simple_router.deinit();

        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }


    std.debug.print("\n\n\nListening on 0.0.0.0:5000\n", .{});

    // start worker threads
    zap.start(.{
        .threads = 1,
        .workers = 1,
    });
}

fn not_found(req: zap.Request) void {
    std.debug.print("not found handler", .{});
    req.sendBody("Not found") catch return;
}

fn on_request_verbose(r: zap.Request) void {
    if (r.path) |the_path| {
        std.debug.print("PATH: {s}\n", .{the_path});
    }

    if (r.query) |the_query| {
        std.debug.print("QUERY: {s}\n", .{the_query});
    }
    r.sendBody("<html><body><h1>Hello from ZAP!!!</h1></body></html>") catch return;
}

fn server_test() !void {
    // Embed files
    const index_html = @embedFile("index.html");
    const style_css = @embedFile("style.css");
    const table_js = @embedFile("table.js");

    var gpa = std.heap.GeneralPurposeAllocator(.{.thread_safe = true}){};
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    const filename: []const u8 = "../tests/mb.csv";
    // const filename: []const u8 = "../tests/mb_small.csv";

    var search_cols = std.ArrayList([]u8).init(allocator);
    try search_cols.append(try allocator.dupe(u8, "TITLE"));
    try search_cols.append(try allocator.dupe(u8, "ARTIST"));
    try search_cols.append(try allocator.dupe(u8, "ALBUM"));
    defer {
        for (search_cols.items) |col| {
            allocator.free(col);
        }
        search_cols.deinit();
    }

    var index_manager = try IndexManager.init(filename, &search_cols, allocator);
    try index_manager.readFile();

    // Write files to disk
    var output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/index.html", 
        .{index_manager.tmp_dir}
        );
    var output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(index_html);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/style.css",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(style_css);

    output_filename = try std.fmt.allocPrint(
        index_manager.allocator, 
        "{s}/table.js",
        .{index_manager.tmp_dir}
        );
    output_file = try std.fs.cwd().createFile(
        output_filename, 
        .{ .read = true },
        );
    _ = try output_file.write(table_js);

    var query_map = std.StringHashMap([]const u8).init(allocator);
    var boost_factors = std.ArrayList(f32).init(allocator);

    for (search_cols.items) |col| {
        try query_map.put(col, "");
        try boost_factors.append(1.0);
    }

    var query_handler = try QueryHandler.init(
        &index_manager,
        boost_factors,
        query_map,
        allocator,
    );


    var simple_router = zap.Router.init(
        allocator, 
        .{.not_found = not_found},
        );

    try simple_router.handle_func_unbound("/", on_request_verbose);
    try simple_router.handle_func("/search", &query_handler, &QueryHandler.on_request);
    try simple_router.handle_func("/get_columns", &query_handler, &QueryHandler.get_columns);
    try simple_router.handle_func("/get_search_columns", &query_handler, &QueryHandler.get_search_columns);
    try simple_router.handle_func("/healthcheck", &query_handler, &QueryHandler.healthcheck);

    var listener = zap.HttpListener.init(.{
        .port = 5000,
        .on_request = simple_router.on_request_handler(),
        .log = true,
    });
    try listener.listen();

    const full_path = try std.mem.concat(
        index_manager.string_arena.allocator(),
        u8, 
        &[_][]const u8{index_manager.tmp_dir, "/index.html"}
        );

    var cmd = std.process.Child.init(&[_][]const u8{"open", full_path}, allocator);
    try cmd.spawn();
    _ = try cmd.wait();

    defer {
        for (search_cols.items) |col| {
            allocator.free(col);
        }

        search_cols.deinit();
        query_map.deinit();
        boost_factors.deinit();

        query_handler.deinit();
        simple_router.deinit();

        index_manager.deinit() catch {};
        _ = gpa.deinit();
    }


    std.debug.print("\n\n\nListening on 0.0.0.0:5000\n", .{});

    // start worker threads
    zap.start(.{
        .threads = 1,
        .workers = 1,
    });

}

pub fn main() !void {
    // try main_cli_runner();
    try server_test();
    // try bench(true);
}
