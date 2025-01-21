const std = @import("std");
const print = std.debug.print;

const SortedArray = @import("sorted_array.zig").SortedScoreArray;

const MAX_STRING_LEN = 7;


const FreqStruct = struct {
    freq: u64,
    value: u8,
};

pub inline fn LCP(key: []const u8, match: []const u8) u8 {
    const max_chars = @min(key.len, match.len);
    for (0..max_chars) |idx| {
        if (key[idx] != match[idx]) return @intCast(idx);
    }
    return @intCast(max_chars);
}

pub export fn memmove(dest: ?[*]u8, src: ?[*]const u8, n: usize) void {
    @setRuntimeSafety(false);

    if (@intFromPtr(dest) < @intFromPtr(src)) {
        var index: usize = 0;
        while (index != n) : (index += 1) {
            dest.?[index] = src.?[index];
        }
    } else {
        var index = n;
        while (index != 0) {
            index -= 1;
            dest.?[index] = src.?[index];
        }
    }
}

pub fn PruningRadixEdge(comptime _: type) type {
    return extern struct {
        const Self = @This();

        str: [MAX_STRING_LEN]u8,
        len: u8,
        child_idx: usize,

        pub fn init(allocator: std.mem.Allocator) !*Self {
            const edge = try allocator.create(Self);
            edge.len = 0;
            return edge;
        }
    };
}

pub fn PruningRadixNode(comptime T: type) type {

    return extern struct {
        const Self = @This();
        const value_type = if (@sizeOf(T) <= 8) T else *T;

        score: f32,
        max_score_below: f32,
        value: value_type,
        edges: [*]PruningRadixEdge(T),

        comptime {
            std.debug.assert(@sizeOf(Self) <= 24);
        }

        pub fn init() Self {
            return Self{
                .score = -std.math.floatMax(f32),
                .max_score_below = -std.math.floatMax(f32),
                .value = undefined,
                .edges = undefined,
            };
        }

        pub fn printChildren(
            self: *const Self, 
            nodes: *const std.ArrayList(PruningRadixNode(T)),
            depth: u32,
            ) void {
            for (0..self.edge_data.num_edges) |edge_idx| {
                const edge  = self.edges[edge_idx];
                const child = nodes.items[edge.child_idx];
                print("Depth {d} - Edge:      {s}\n",  .{depth, edge.str[0..edge.len]});
                print("Depth {d} - Child:     {d}\n", .{depth, child.value});
                print("Depth {d} - Num Edges: {d}\n\n", .{depth, child.edge_data.num_edges});
                child.printChildren(nodes, depth + 1);
            }
        }

        pub fn printEdges(
            self: *const Self, 
            node_idx: usize,
            trie: *const PruningRadixTrie(T),
            ) void {
            for (0..trie.edge_counts.items[node_idx]) |i| {
                const edge = self.edges[i];
                // print("{d}: {s}: {b}: {d}\n", .{i, edge.str[0..edge.len], trie.is_leaf_bitmask.items[node_idx], trie.nodes.items[edge.child_idx].max_score_below});
                print("{d}: {s}: {b}: {d}\n", .{i, edge.str[0..edge.len], trie.is_leaf_bitmask.items[edge.child_idx], trie.nodes.items[edge.child_idx].max_score_below});
            }
            print("\n", .{});
        }
    };
}


pub fn PruningRadixTrie(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        nodes: std.ArrayList(PruningRadixNode(T)),
        edge_counts: std.ArrayList(u8),
        is_leaf_bitmask: std.ArrayList(u1),
        num_nodes: usize,
        num_edges: usize,
        num_keys: usize,

        sorted_array: SortedArray(PruningEntry),

        pub const PruningEntry = struct {
            key: []const u8,
            value: T,
            score: f32,
        };

        pub fn init(allocator: std.mem.Allocator) !Self {
            var nodes = try std.ArrayList(PruningRadixNode(T)).initCapacity(allocator, 16_384);
            var edge_counts = try std.ArrayList(u8).initCapacity(allocator, 16_384);
            var is_leaf_bitmask = try std.ArrayList(u1).initCapacity(allocator, 16_384);

            const root = PruningRadixNode(T).init();

            try nodes.append(root);
            try edge_counts.append(0);
            try is_leaf_bitmask.append(0);

            return Self{
                .allocator = allocator,
                .nodes = nodes,
                .edge_counts = edge_counts,
                .is_leaf_bitmask = is_leaf_bitmask,
                .num_nodes = 1,
                .num_edges = 0,
                .num_keys = 0,

                .sorted_array = try SortedArray(PruningEntry).init(allocator, 1000),
            };
        }

        pub fn deinit(self: *Self) void {
            self.nodes.deinit();
            self.edge_counts.deinit();
            self.is_leaf_bitmask.deinit();
            self.sorted_array.deinit();
        }

        pub fn getMemoryUsage(self: *Self) usize {
            const bytes = self.nodes.capacity * @sizeOf(PruningRadixNode(T)) + self.num_edges * @sizeOf(PruningRadixEdge(T));
            return bytes;
        }

        pub inline fn addEdge(
            self: *Self, 
            edge: *PruningRadixEdge(T),
            node_idx: usize,
            ) !void {
            self.edge_counts.items[node_idx] += 1;
            const num_edges = self.edge_counts.items[node_idx];

            var new_capacity: usize = std.math.maxInt(usize);
            switch (num_edges) {
                0 => new_capacity = 1,
                1 => new_capacity = 3,
                3 => new_capacity = 5,
                5 => new_capacity = 8,
                8 => new_capacity = 16,
                16 => new_capacity = 32,
                32 => new_capacity = 64,
                64 => new_capacity = 128,
                128 => new_capacity = 192,
                192 => new_capacity = 256,
                else => {},
            }
            var node = &self.nodes.items[node_idx];

            if (new_capacity != std.math.maxInt(usize)) {
                const new_slice: []PruningRadixEdge(T) = try self.allocator.realloc(
                        node.edges[0..num_edges - 1],
                        new_capacity,
                        );
                node.edges = new_slice.ptr;
            }

            // Linear search for now. Do binary search later if large number of edges.
            const child_max_score_below = self.nodes.items[edge.child_idx].max_score_below;
            for (0..num_edges - 1) |idx| {
                if (self.nodes.items[node.edges[idx].child_idx].max_score_below >= child_max_score_below) continue;
                memmove(
                    @ptrCast(&node.edges[idx + 1]),
                    @ptrCast(&node.edges[idx]),
                    (num_edges - idx) * @sizeOf(PruningRadixEdge(T)),
                    );
                node.edges[idx] = edge.*;
                return;
            }
            node.edges[num_edges - 1] = edge.*;
        }

        fn addNode(
            self: *Self, 
            key: []const u8, 
            _node_idx: usize,
            value: T,
            score: f32,
            max_score_below: f32,
        ) !void {
            var node_idx  = _node_idx;
            var node      = &self.nodes.items[node_idx];
            var rem_chars = key.len;

            var current_idx: usize = 0;
            while (true) {
                var num_chars_edge = rem_chars;
                var is_leaf = true;

                if (rem_chars > MAX_STRING_LEN) {
                    num_chars_edge = MAX_STRING_LEN;
                    is_leaf = false;
                }

                const _new_node = PruningRadixNode(T){
                    .score = score,
                    .max_score_below = max_score_below,
                    .value = if (is_leaf) value else undefined,
                    .edges = undefined,
                };
                try self.nodes.append(_new_node);
                try self.edge_counts.append(0);
                try self.is_leaf_bitmask.append(@intCast(@intFromBool(is_leaf)));

                const new_node = &self.nodes.items[self.nodes.items.len - 1];

                const new_edge     = try PruningRadixEdge(T).init(self.allocator);
                new_edge.len       = @truncate(num_chars_edge);
                new_edge.child_idx = self.nodes.items.len - 1;

                @memcpy(
                    new_edge.str[0..num_chars_edge], 
                    key[current_idx..current_idx+num_chars_edge],
                    );

                try self.addEdge(new_edge, node_idx);

                if (is_leaf) break;

                rem_chars   -= MAX_STRING_LEN;
                current_idx += num_chars_edge;
                node         = new_node;
                node_idx     = self.nodes.items.len - 1;
            }
        }

        pub fn insert(
            self: *Self, 
            key: []const u8, 
            value: T,
            score: f32,
            ) !void {
            if (key.len == 0) return;

            try self.nodes.ensureUnusedCapacity(256);

            var prev_node_idx: usize = 0;
            var prev_edge_idx: usize = 0;
            var node_idx: usize = 0;
            var node = &self.nodes.items[node_idx];
            var key_idx: usize = 0;

            while (true) {
                var next_node: *PruningRadixNode(T) = undefined;
                var next_node_idx: usize = undefined;
                var max_edge_idx: usize = 0;
                var max_lcp: u8 = 0;
                var partial: bool = false;

                for (0..self.edge_counts.items[node_idx]) |edge_idx| {
                    const current_edge   = node.edges[edge_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (key[key_idx] == current_prefix[0]) {
                        max_lcp = LCP(key[key_idx..], current_prefix);
                        max_edge_idx = edge_idx;
                        next_node = &self.nodes.items[node.edges[edge_idx].child_idx];
                        next_node_idx = node.edges[edge_idx].child_idx;
                        partial = max_lcp < current_prefix.len;
                        break;
                    }
                }
                node.max_score_below = @max(score, node.max_score_below);

                if (max_lcp == 0) {
                    try self.addNode(key[key_idx..], node_idx, value, score, score);
                    self.num_keys += 1;
                    self.num_nodes += 1;
                    self.num_edges += 1;
                    return;
                }

                // Reposition existing edge.
                if (score > next_node.max_score_below) {
                    next_node.max_score_below = score;

                    const num_edges = self.edge_counts.items[node_idx];
                    const current_edge = node.edges[max_edge_idx];

                    for (0..num_edges) |idx| {
                        if (idx == max_edge_idx) break;

                        const edge = node.edges[idx];
                        const child = &self.nodes.items[edge.child_idx];

                        if (child.max_score_below >= next_node.max_score_below) continue;
                        const old_idx = max_edge_idx;
                        max_edge_idx = idx;

                        memmove(
                            @ptrCast(&node.edges[idx + 1]),
                            @ptrCast(&node.edges[idx]),
                            (old_idx - idx) * @sizeOf(PruningRadixEdge(T)),
                            );
                        node.edges[max_edge_idx] = current_edge;
                        break;
                    }
                }

                key_idx += max_lcp;

                // Matched rest of key. Node already exists. Replace.
                if (!partial and (key_idx == key.len)) {
                    // Make terminal if not.
                    self.num_keys += @intFromBool((self.is_leaf_bitmask.items[next_node_idx] & 1) == 0);
                    self.is_leaf_bitmask.items[next_node_idx] = 1;
                    next_node.value = value;
                    return;
                }


                const rem_chars: usize = key.len - key_idx;
                if (partial) {
                    // Split
                    if (rem_chars == 0) {
                        var existing_edge = &node.edges[max_edge_idx];
                        const _new_node = PruningRadixNode(T){
                            .score = score,
                            .max_score_below = @max(score, next_node.max_score_below),
                            .value = value,
                            .edges = undefined,
                        };
                        try self.nodes.append(_new_node);
                        try self.edge_counts.append(0);
                        try self.is_leaf_bitmask.append(1);

                        const new_node_idx = self.nodes.items.len - 1;
                        const new_node = &self.nodes.items[new_node_idx];
                        const new_edge = try PruningRadixEdge(T).init(self.allocator);

                        self.num_nodes += 1;
                        self.num_edges += 1;

                        /////////////////////////////////
                        //                             //
                        // \ - existing_edge           //
                        //  O - existing_node          //
                        //                             //
                        // str:           'ABC'        //
                        // existing_edge: 'ABCD'       //
                        //                             //
                        // --------------------------- //
                        //      SPLIT NO-CREATE        //
                        // --------------------------- //
                        // \ - existing_edge           // 
                        //  O - new_node               //
                        //   \ - new_edge              //
                        //    O - existing_node        //
                        //                             //
                        // existing_edge: 'ABC'        //
                        // new_edge:      'D'          //
                        //                             //
                        /////////////////////////////////

                        new_edge.len      = existing_edge.len - max_lcp;
                        existing_edge.len = max_lcp;

                        @memcpy(
                            new_edge.str[0..new_edge.len], 
                            existing_edge.str[max_lcp..max_lcp + new_edge.len],
                            );

                        new_edge.child_idx      = existing_edge.child_idx;
                        existing_edge.child_idx = self.nodes.items.len - 1;

                        new_node.value = value;

                        try self.addEdge(
                            new_edge,
                            self.nodes.items.len - 1,
                            );

                    } else {

                        var existing_edge = &node.edges[max_edge_idx];
                        const _new_node_1 = PruningRadixNode(T){
                            .score = score,
                            .max_score_below = @max(score, next_node.max_score_below),
                            .value = value,
                            .edges = undefined,
                        };
                        try self.nodes.append(_new_node_1);
                        try self.edge_counts.append(0);
                        try self.is_leaf_bitmask.append(0);

                        const new_node_1_idx = self.nodes.items.len - 1;

                        const new_edge_2 = try PruningRadixEdge(T).init(self.allocator);

                        self.num_nodes += 2;
                        self.num_edges += 2;

                        /////////////////////////////////////////
                        //                                     //
                        // \ - existing_edge                   //
                        //  O - existing_node                  //
                        //                                     //
                        // str:           'AEF'                //
                        // existing_edge: 'ABCD'               //
                        //                                     //
                        // ----------------------------------- //
                        //             SPLIT CREATE            //
                        // ----------------------------------- //
                        //  \ - existing_edge                  // 
                        //   O - new_node_1                    //
                        //  / \ - (new_edge_1, new_edge_2)     //
                        // O   O - (new_node_2, existing_node) //
                        //                                     //
                        // existing_edge: 'A'                  //
                        // new_edge_1:    'BCD'                //
                        // new_edge_2:    'EF'                 //
                        //                                     //
                        /////////////////////////////////////////

                        new_edge_2.len    = existing_edge.len - max_lcp;
                        existing_edge.len = max_lcp;

                        @memcpy(
                            new_edge_2.str[0..new_edge_2.len], 
                            existing_edge.str[max_lcp..max_lcp + new_edge_2.len],
                            );

                        new_edge_2.child_idx    = existing_edge.child_idx;
                        existing_edge.child_idx = self.nodes.items.len - 1;

                        try self.addEdge(
                            new_edge_2,
                            new_node_1_idx,
                            );
                        try self.addNode(
                            key[key.len-rem_chars..], 
                            new_node_1_idx,
                            value,
                            score,
                            score,
                            );
                    }

                    self.num_keys += 1;
                    return;
                }

                // Traverse
                node = next_node;
                node_idx = next_node_idx;
                prev_node_idx = node_idx;
                prev_edge_idx = max_edge_idx;
            }
            @panic("This should be unreachable.\n");
        }

        fn gatherChildrenBFSPruning(
            self: *const Self, 
            starting_node_idx: usize, 
            current_prefix: []const u8,
            sorted_array: *SortedArray(PruningEntry),
            debug_count: *usize,
            ) !void {
            debug_count.* += 1;

            const starting_node = &self.nodes.items[starting_node_idx];

            for (0..self.edge_counts.items[starting_node_idx]) |edge_idx| {
                const min_score = sorted_array.getMinScore();

                const edge  = starting_node.edges[edge_idx];
                const child = self.nodes.items[edge.child_idx];

                if (child.max_score_below <= min_score) return;

                const new_prefix = try std.mem.concat(
                    self.allocator, 
                    u8, 
                    &[_][]const u8{current_prefix, edge.str[0..edge.len]},
                    );

                if ((child.score > min_score) and ((self.is_leaf_bitmask.items[edge.child_idx] & 1) == 1)) {
                    sorted_array.insert(PruningEntry{
                        .key = new_prefix,
                        .value = child.value,
                        .score = child.score,
                    });
                }

                try self.gatherChildrenBFSPruning(
                    edge.child_idx, 
                    new_prefix, 
                    sorted_array, 
                    debug_count,
                    );
            }
        }

        pub fn getPrefixNodesPruning(
            self: *Self, 
            key: []const u8, 
            matching_nodes: *[]PruningEntry,
            ) !usize {
            self.sorted_array.capacity = matching_nodes.len;
            self.sorted_array.clear();

            var node = self.nodes.items[0];
            var node_idx: usize = 0;
            var key_idx: usize = 0;
            var debug_count: usize = 0;

            var current_score = std.math.floatMin(f32);
            while (key_idx < key.len) {
                if (self.edge_counts.items[node_idx] == 0) {
                    return error.ValueNotFound;
                }

                current_score = node.max_score_below;

                var matched = false;
                for (0..self.edge_counts.items[node_idx]) |edge_idx| {
                    const current_edge   = node.edges[edge_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                        matched = true;
                        node_idx = node.edges[edge_idx].child_idx;
                        node     = self.nodes.items[node_idx];
                        key_idx += current_prefix.len;

                        if (key_idx == key.len) {
                            if ((self.is_leaf_bitmask.items[current_edge.child_idx] & 1) == 1) {
                                self.nodes.items[0].printEdges(0, self);
                                self.sorted_array.insert(PruningEntry{
                                    .key = key,
                                    .value = node.value,
                                    .score = current_score,
                                });
                            }

                            try self.gatherChildrenBFSPruning(
                                node_idx,
                                key,
                                &self.sorted_array,
                                &debug_count,
                                );
                            for (0..self.sorted_array.count) |idx| {
                                matching_nodes.*[idx] = self.sorted_array.items[idx];
                            }
                            return self.sorted_array.count;
                        }
                        break;
                    }
                }
                if (!matched) {
                    if (key_idx > 0) {
                        if ((self.is_leaf_bitmask.items[node_idx] & 1) == 1) {
                            self.sorted_array.insert(PruningEntry{
                                .key = key,
                                .value = node.value,
                                .score = current_score,
                            });
                        }

                        try self.gatherChildrenBFSPruning(
                            node_idx,
                            key,
                            &self.sorted_array,
                            &debug_count,
                            );
                        for (0..self.sorted_array.count) |idx| {
                            matching_nodes.*[idx] = self.sorted_array.items[idx];
                        }
                        return self.sorted_array.count;
                    }
                    return error.ValueNotFound;
                }
            }
            if ((self.is_leaf_bitmask.items[node_idx] & 1) == 1) {
                self.sorted_array.insert(PruningEntry{
                    .key = key,
                    .value = node.value,
                    .score = current_score,
                });

                try self.gatherChildrenBFSPruning(
                    node_idx, 
                    key,
                    &self.sorted_array,
                    &debug_count,
                    );
                for (0..self.sorted_array.count) |idx| {
                    matching_nodes.*[idx] = self.sorted_array.items[idx];
                }
                return self.sorted_array.count;
            }

            if (key_idx > 0) {
                if ((self.is_leaf_bitmask.items[node_idx] & 1) == 1) {
                    self.sorted_array.insert(PruningEntry{
                        .key = key,
                        .value = node.value,
                        .score = current_score,
                    });
                }

                try self.gatherChildrenBFSPruning(
                    node_idx, 
                    key,
                    &self.sorted_array,
                    &debug_count,
                    );
                for (0..self.sorted_array.count) |idx| {
                    matching_nodes.*[idx] = self.sorted_array.items[idx];
                }
                return self.sorted_array.count;
            }
            return error.ValueNotFound;
        }

        pub fn find(self: *const Self, key: []const u8) !T {
            var node_idx: usize = 0;
            var node = self.nodes.items[node_idx];
            var key_idx: usize = 0;

            while (key_idx < key.len) {
                if (self.edge_counts.items[node_idx] == 0) {
                    return error.ValueNotFound;
                }

                var matched = false;
                for (0..self.edge_counts.items[node_idx]) |edge_idx| {
                    const current_edge   = node.edges[edge_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                        matched = true;
                        node_idx = node.edges[edge_idx].child_idx;
                        node     = self.nodes.items[node_idx];
                        key_idx += current_prefix.len;

                        if ((key_idx == key.len) and (self.is_leaf_bitmask.items[node_idx] == 1)) {
                            return node.value;
                        }
                        break;
                    }
                }
                if (!matched) {
                    return error.ValueNotFound;
                }

            }
            if (self.is_leaf_bitmask.items[node_idx] == 1) {
                return node.value;
            }
            return error.ValueNotFound;
        }
    };
}


test "insertion" {
    print("\n\n", .{});

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var trie = try PruningRadixTrie(u32).init(allocator);
    defer trie.deinit();

    try trie.insert("ostritch", 24, 3.0);
    try trie.insert("test", 420, 2.0);
    try trie.insert("testing", 69, 2.0);
    try trie.insert("tes", 24, 2.0);
    try trie.insert("waddup", 54, 4.0);
    try trie.insert("newting", 44, 5.0);
    try trie.insert("tosting", 69, 2.0);
    try trie.insert("toaster", 84, 2.0);
    try trie.insert("magisterialness", 64, 6.0);
    try trie.insert("abracadabra", 121, 7.0);
    try trie.insert("rapper", 85, 8.0);
    try trie.insert("apple", 25, 2.0);
    try trie.insert("apocryphol", 25, 2.0);
    try trie.insert("apacryphol", 25, 2.0);
    try trie.insert("apicryphol", 25, 2.0);
    try trie.insert("eager", 25, 2.0);
    try trie.insert("mantequilla", 25, 2.0);
    try trie.insert("initial", 25, 2.0);
    try trie.insert("initial", 32, 2.0);

    const limit: usize = 10;
    var matching_nodes = try trie.allocator.alloc(PruningRadixTrie(u32).PruningEntry, limit);

    const nodes_found = try trie.getPrefixNodesPruning("tes", &matching_nodes);
    for (0..nodes_found) |i| {
        print("Node {d} - Key: {s} Value: {d}\n", .{i, matching_nodes[i].key, matching_nodes[i].value});
    }

    trie.nodes.items[0].printEdges(0, &trie);

    try std.testing.expectEqual(420, trie.find("test"));
    try std.testing.expectEqual(69, trie.find("testing"));
    try std.testing.expectEqual(24, trie.find("tes"));
    try std.testing.expectEqual(420, trie.find("test"));
    try std.testing.expectEqual(69, trie.find("testing"));
    try std.testing.expectEqual(54, trie.find("waddup"));
    try std.testing.expectEqual(error.ValueNotFound, trie.find("testin"));
    try std.testing.expectEqual(121, trie.find("abracadabra"));
    try std.testing.expectEqual(32, trie.find("initial"));
    try std.testing.expectEqual(25, trie.find("apocryphol"));
    try std.testing.expectEqual(25, trie.find("apacryphol"));
    try std.testing.expectEqual(25, trie.find("apicryphol"));
    try std.testing.expectEqual(25, trie.find("eager"));
    try std.testing.expectEqual(25, trie.find("mantequilla"));
    try std.testing.expectEqual(32, trie.find("initial"));
    try std.testing.expectEqual(64, trie.find("magisterialness"));
}

test "bench" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    // var arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer {
        // _ = gpa.deinit();
    // }
    // const g_allocator = gpa.allocator();

    var keys = std.StringHashMap(usize).init(allocator);
    defer keys.deinit();

    var trie = try PruningRadixTrie(u32).init(allocator);
    defer trie.deinit();

    // const filename = "data/reversed_words.txt";
    // const filename = "data/words.txt";
    // const filename = "data/words_shuffled_1k.txt";
    // const filename = "data/words_shuffled.txt";
    const filename = "data/terms.txt";
    // const filename = "data/enwik9";
    // const filename = "data/duplicate_words.txt";
    // const max_bytes_per_line = 65536;
    const max_bytes_per_line = 1_048_576;
    var file = std.fs.cwd().openFile(filename, .{}) catch {
        return;
    };
    defer file.close();
    var buffered_reader = std.io.bufferedReader(file.reader());

    var _raw_keys: std.ArrayList([]const u8) = std.ArrayList([]const u8).init(allocator);
    var _scores: std.ArrayList(f32) = std.ArrayList(f32).init(allocator);
    defer {
        _ = _raw_keys.deinit();
        _ = _scores.deinit();
    }

    const reader = buffered_reader.reader();
    while (try reader.readUntilDelimiterOrEofAlloc(allocator, '\n', max_bytes_per_line)) |line| {
        if (line.len == 0) continue;
        var it = std.mem.splitSequence(u8, line, "\t");

        var idx: usize = 0;
        while (it.next()) |field| {
            if (field.len == 0) continue;

            if (idx == 0) {
                try _raw_keys.append(field);
                idx += 1;
                continue;
            }

            try _scores.append(try std.fmt.parseFloat(f32, field));
            idx += 1;
        }
    }

    try trie.nodes.ensureTotalCapacity(_raw_keys.items.len);

    const raw_keys = try _raw_keys.toOwnedSlice();
    const _N = raw_keys.len;

    print("Finished reading file\n", .{});
    print("Num keys: {d}\n", .{_N});

    var dup_map = std.StringHashMap(u32).init(allocator);
    defer dup_map.deinit();

    var start = std.time.microTimestamp();
    for (0.._N) |i| {
        if (raw_keys[i].len == 0) continue;
        const val = try keys.fetchPut(raw_keys[i], i);
        if (val) |_| {
            try dup_map.put(raw_keys[i], 0);
        }
    }
    var end = std.time.microTimestamp();
    const elapsed_insert_hashmap = end - start;
    print("Finished inserting into hashmap\n", .{});

    const N = keys.count();

    print("N: {d}\n", .{N});
    print("num_scores: {d}\n", .{_scores.items.len});

    start = std.time.microTimestamp();
    var i: u32 = 0;
    for (0.._N) |j| {
        const init_count = trie.num_keys;
        try trie.insert(raw_keys[j], i, _scores.items[j]);
        if ((trie.num_keys == init_count) and (dup_map.get(raw_keys[j]) == null)) {
            print("Failed to insert key: {s}\n", .{raw_keys[j]});
        }
        i += 1;
    }
    end = std.time.microTimestamp();
    const elapsed_insert_trie = end - start;
    print("Finished inserting into trie\n", .{});

    start = std.time.microTimestamp();
    var keys_not_found: usize = 0;
    for (0.._N) |j| {
        _ = trie.find(raw_keys[j]) catch {
            keys_not_found += 1;
        };
    }
    std.debug.print("Keys not found: {d}\n", .{keys_not_found});
    end = std.time.microTimestamp();
    const elapsed_trie_find = end - start;

    start = std.time.microTimestamp();
    for (0.._N) |j| {
        _ = std.mem.doNotOptimizeAway(keys.get(raw_keys[j]));
    }
    end = std.time.microTimestamp();
    const elapsed_hashmap_find = end - start;

    try std.testing.expectEqual(N, trie.num_keys);

    print("Num trie keys:  {d}\n", .{trie.num_keys});
    print("Num trie nodes: {d}\n", .{trie.num_nodes});
    print("Num trie edges: {d}\n", .{trie.num_edges});
    print("Theoretical trie memory usage: {d}MB\n", .{trie.getMemoryUsage() / 1_048_576});

    const million_insertions_per_second_trie = @as(f32, @floatFromInt(N)) / @as(f32, @floatFromInt(elapsed_insert_trie));
    const million_lookups_per_second_trie    = @as(f32, @floatFromInt(N)) / @as(f32, @floatFromInt(elapsed_trie_find));
    std.debug.print("-------------------  RADIX TRIE  -------------------\n", .{});
    std.debug.print("\nConstruction time:   {}ms\n", .{@divFloor(elapsed_insert_trie, 1000)});
    std.debug.print("Million insertions per second: {d}\n", .{million_insertions_per_second_trie});
    std.debug.print("Million lookups per second:    {d}\n", .{million_lookups_per_second_trie});
    std.debug.print("Average lookup time:           {}ns\n", .{@divFloor(1000 * elapsed_trie_find, N)});

    std.debug.print("\n\n", .{});

    const million_insertions_per_second_hashmap = @as(f32, @floatFromInt(N)) / @as(f32, @floatFromInt(elapsed_insert_hashmap));
    const million_lookups_per_second_hashmap    = @as(f32, @floatFromInt(N)) / @as(f32, @floatFromInt(elapsed_hashmap_find));
    std.debug.print("-------------------  HASHMAP  -------------------\n", .{});
    std.debug.print("\nConstruction time:   {}ms\n", .{@divFloor(elapsed_insert_hashmap, 1000)});
    std.debug.print("Million insertions per second: {d}\n", .{million_insertions_per_second_hashmap});
    std.debug.print("Million lookups per second:    {d}\n", .{million_lookups_per_second_hashmap});
    std.debug.print("Average lookup time:           {}ns\n", .{@divFloor(1000 * elapsed_hashmap_find, N)});

    std.debug.print("\n\n", .{});
    const limit: usize = 10;

    var matching_nodes = try trie.allocator.alloc(PruningRadixTrie(u32).PruningEntry, limit);

    // Bench
    // for (0..10_000_000) |_| {
        // _ = try trie.getPrefixNodesPruning("m", &matching_nodes);
    // }

    // const num_rounds: usize = 1000;
    const num_rounds: usize = 1;
    const start_prefix = std.time.nanoTimestamp();
    // const nodes_found = try trie.getPrefixNodesPruning("m", &matching_nodes);
    for (0..num_rounds) |_| {
        _ = try trie.getPrefixNodesPruning("m", &matching_nodes);
    }
    const end_prefix = std.time.nanoTimestamp();

    const nodes_found = try trie.getPrefixNodesPruning("m", &matching_nodes);

    // const elapsed_prefix = end_prefix - start_prefix;
    const elapsed_prefix = @divFloor((end_prefix - start_prefix), num_rounds);
    print("Prefix search time: {}us\n", .{@divFloor(elapsed_prefix, 1000)});
    print("Prefix search time: {}ns\n", .{elapsed_prefix});
    print("Num nodes found: {d}\n", .{nodes_found});

    for (0..@min(nodes_found, 100)) |idx| {
        print("Node {d} - Key: {s} Value: {d} Score: {d}\n", .{idx, matching_nodes[idx].key, matching_nodes[idx].value, matching_nodes[idx].score});
    }
}
