const std = @import("std");
const print = std.debug.print;

// TODO: Define second node type for nodes with more than ~48 values.
//       Use all charachter frequencies here and do popCount with AVX2 instructions.

const MAX_STRING_LEN = 7;


// Most frequent characters in English.
pub fn getBaseCharFreqTable(comptime num_bits: usize) [256]u8 {
    var table: [256]u8 = undefined;
    for (0..256) |idx| {
        table[idx] = 0;
    }
    const enwik9_byte_freq_ordering = [256]u8 {
        32, 101, 116, 97, 105, 111, 110, 114, 115, 108, 104, 100, 99, 93, 91, 
        117, 109, 112, 103, 10, 102, 121, 46, 39, 119, 98, 44, 118, 59, 49, 
        48, 47, 60, 62, 38, 124, 50, 61, 107, 67, 83, 45, 58, 84, 57, 65, 
        51, 53, 52, 41, 40, 42, 113, 56, 54, 77, 120, 73, 80, 55, 66, 82, 
        68, 69, 72, 76, 70, 71, 78, 87, 85, 37, 74, 79, 122, 125, 123, 106, 
        75, 86, 35, 95, 34, 195, 90, 208, 89, 36, 33, 227, 215, 128, 131, 92, 
        169, 81, 88, 209, 226, 184, 63, 130, 224, 176, 161, 43, 188, 153, 179, 
        164, 229, 129, 197, 206, 194, 173, 148, 189, 190, 178, 181, 168, 186, 
        216, 147, 167, 196, 187, 185, 182, 9, 156, 177, 180, 230, 217, 149, 
        163, 132, 225, 160, 170, 171, 141, 136, 144, 157, 94, 165, 166, 231, 
        183, 133, 152, 137, 191, 175, 159, 162, 207, 140, 134, 135, 174, 145, 
        151, 158, 150, 155, 232, 126, 233, 139, 236, 138, 143, 172, 146, 228, 
        235, 154, 142, 201, 237, 202, 64, 203, 234, 214, 219, 204, 239, 96, 
        199, 218, 213, 198, 210, 220, 211, 205, 200, 238, 240, 212, 222,

        0, 1, 2, 3, 4, 5, 6, 7, 8, 255, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 
        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 192, 193, 221, 223, 241, 
        242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 127
    };

    for (0..num_bits) |idx| {
        table[enwik9_byte_freq_ordering[idx]] = @intCast(idx + 1);
    }
    return table;
}

const FreqStruct = struct {
    freq: u64,
    value: u8,
};

fn getBitMasksU64(comptime T: type) [@bitSizeOf(T)]u64 {
    const num_bits = @bitSizeOf(T);
    var value: u64 = 1;

    var masks: [num_bits]u64 = undefined;
    for (0..num_bits) |idx| {
        masks[idx] = value;
        value <<= 1;
    }
    return masks;
}
const BITMASKS = getBitMasksU64(std.meta.FieldType(RadixNode(void).EdgeData, .freq_char_bitmask));

fn getFullMasksU64(comptime T: type) [@bitSizeOf(T)]u64 {
    const num_bits = @bitSizeOf(T);
    var value: u64 = @as(u64, @intCast(std.math.maxInt(T))) - 1;

    const overall_mask = value;

    var masks: [num_bits]u64 = undefined;
    for (0..num_bits) |idx| {
        masks[idx] = value & overall_mask;
        value <<= 1;
    }
    return masks;
}
const FULL_MASKS = getFullMasksU64(std.meta.FieldType(RadixNode(void).EdgeData, .freq_char_bitmask));

pub inline fn getInsertIdx(
    char_freq_table: *const [256]u8,
    bitmask: u64,
    char: u8,
    ) usize {
    const shift_len: usize = @intCast(char_freq_table[@intCast(char)]);
    return @popCount(bitmask & FULL_MASKS[shift_len]);
}

pub inline fn LCP(key: []const u8, match: []const u8) u8 {
    const max_chars = @min(key.len, match.len);
    for (0..max_chars) |idx| {
        if (key[idx] != match[idx]) return @intCast(idx);
    }
    return @intCast(max_chars);
}

pub fn RadixEdge(comptime _: type) type {
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

pub fn RadixNode(comptime T: type) type {

    return extern struct {
        const Self = @This();
        const value_type = if (@sizeOf(T) <= 8) T else *T;

        // is_leaf is stored in lowest bit of freq_char_bitmask
        edge_data: EdgeData,
        value: value_type,
        edges: [*]RadixEdge(T),

        pub const EdgeData = packed struct(u64) {
            freq_char_bitmask: u55,
            num_edges: u9,
        };

        comptime {
            std.debug.assert(@sizeOf(Self) <= 24);
        }

        pub fn init() Self {
            return Self{
                .edge_data = EdgeData{
                    .freq_char_bitmask = 0,
                    .num_edges = 0,
                },
                .value = undefined,
                .edges = undefined,
            };
        }

        pub inline fn getMaskU64(self: *const Self) u64 {
            return @intCast(self.edge_data.freq_char_bitmask);
        }

        pub inline fn addEdge(
            self: *Self, 
            allocator: std.mem.Allocator,
            edge: *RadixEdge(T),
            ) !void {
            self.edge_data.num_edges += 1;

            var new_capacity: usize = std.math.maxInt(usize);
            switch (self.edge_data.num_edges) {
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

            if (new_capacity != std.math.maxInt(usize)) {
                const new_slice: []RadixEdge(T) = try allocator.realloc(
                        self.edges[0..self.edge_data.num_edges - 1],
                        new_capacity,
                        );
                self.edges = new_slice.ptr;
            }
            self.edges[self.edge_data.num_edges - 1] = edge.*;
        }

        pub inline fn addEdgePos(
            self: *Self, 
            char_freq_table: *const [256]u8,
            allocator: std.mem.Allocator,
            edge: *RadixEdge(T),
            ) !void {
            if (char_freq_table[edge.str[0]] == 0) {
                try self.addEdge(allocator, edge);
                return;
            }

            const insert_idx_new: usize = getInsertIdx(
                char_freq_table,
                self.getMaskU64(), 
                edge.str[0],
                );
            const old_swap_idx: usize   = @intCast(self.edge_data.num_edges);

            std.debug.assert(insert_idx_new <= old_swap_idx);

            try self.addEdge(allocator, edge);
            if (insert_idx_new == old_swap_idx) return;

            // Save for swap. Will be overwritten in memmove.
            const temp = self.edges[old_swap_idx];

            // Memmove
            var idx: usize = @intCast(self.edge_data.num_edges - 1);
            while (idx > insert_idx_new) : (idx -= 1) {
                self.edges[idx] = self.edges[idx - 1];
            }

            // Swap
            self.edges[insert_idx_new] = temp;
        }

        pub fn printChildren(
            self: *const Self, 
            nodes: *const std.ArrayList(RadixNode(T)),
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

        pub fn printEdges(self: *const Self) void {
            for (0..self.edge_data.num_edges) |i| {
                const edge = self.edges[i];
                print("{d}: {s}\n", .{i, edge.str[0..edge.len]});
            }
            print("\n", .{});
        }
    };
}


pub fn RadixTrie(comptime T: type) type {
    return struct {
        const Self = @This();
        const NodeType = RadixNode(T);

        allocator: std.mem.Allocator,
        nodes: std.ArrayList(NodeType),
        num_nodes: usize,
        num_edges: usize,
        num_keys: usize,
        char_freq_table: [256]u8,


        pub const Entry = struct {
            key: []const u8,
            value: T,
        };

        pub fn init(allocator: std.mem.Allocator) !Self {
            var nodes = try std.ArrayList(NodeType).initCapacity(allocator, 16_384);
            const root = NodeType.init();
            try nodes.append(root);
            const num_bits = @bitSizeOf(std.meta.FieldType(NodeType.EdgeData, .freq_char_bitmask));

            return Self{
                .allocator = allocator,
                .nodes = nodes,
                .num_nodes = 1,
                .num_edges = 0,
                .num_keys = 0,
                .char_freq_table = getBaseCharFreqTable(num_bits),
            };
        }

        pub fn initCapacity(allocator: std.mem.Allocator, n: usize) !Self {
            var nodes = try std.ArrayList(NodeType).initCapacity(allocator, n);
            const root = NodeType{
                .edge_data = NodeType.EdgeData{
                    .freq_char_bitmask = 0,
                    .num_edges = 0,
                },
                .value = undefined,
                .edges = undefined,
            };
            try nodes.append(root);
            const num_bits = @bitSizeOf(std.meta.FieldType(NodeType.EdgeData, .freq_char_bitmask));

            return Self{
                .allocator = allocator,
                .nodes = nodes,
                .num_nodes = 1,
                .num_edges = 0,
                .num_keys = 0,
                .char_freq_table = getBaseCharFreqTable(num_bits),
            };
        }

        pub fn deinit(self: *Self) void {
            self.nodes.deinit();
        }

        fn argSortDesc(
            _: void,
            a: FreqStruct,
            b: FreqStruct,
            ) bool {
            return a.freq > b.freq;
        }

        pub fn buildFrequencyTable(self: *Self, words: [][]const u8) void {
            if (self.nodes.items.len > 1) @panic("Nodes already inserted into trie. Can't build new frequency table.");

            var items: [256]FreqStruct = undefined;
            for (0..256) |idx| {
                items[idx].freq = 0;
                items[idx].value = @intCast(idx);
            }

            for (words) |word| {
                for (word, 0..) |char, idx| {
                    // Weight by proximity to front of word. 
                    // Will encounter low depth nodes more frequently.
                    items[@intCast(char)].freq += (word.len - idx);
                }
            }
            std.mem.sort(
                FreqStruct,
                &items,
                {}, 
                argSortDesc,
                );

            const num_entries = (@bitSizeOf(std.meta.FieldType(NodeType.EdgeData, .freq_char_bitmask))) - 1;

            // Sort by frequency
            for (0..num_entries) |idx| {
                self.char_freq_table[@intCast(items[idx].value)] = @intCast(idx + 1);
            }

            for (num_entries..256) |idx| {
                self.char_freq_table[@intCast(items[idx].value)] = 0;
            }
        }

        pub fn getMemoryUsage(self: *Self) usize {
            const bytes = self.nodes.capacity * @sizeOf(NodeType) + self.num_edges * @sizeOf(RadixEdge(T));
            return bytes;
        }

        fn addNode(
            self: *Self, 
            key: []const u8, 
            _node: *NodeType,
            value: T,
        ) !void {
            var node      = _node;
            var rem_chars = key.len;

            var current_idx: usize = 0;
            while (true) {
                var num_chars_edge = rem_chars;
                var is_leaf = true;

                if (rem_chars > MAX_STRING_LEN) {
                    num_chars_edge = MAX_STRING_LEN;
                    is_leaf = false;
                }

                const _new_node = NodeType{
                    .edge_data = .{
                        .freq_char_bitmask = @intFromBool(is_leaf),
                        .num_edges = 0,
                    },
                    .value = if (is_leaf) value else undefined,
                    .edges = undefined,
                };
                try self.nodes.append(_new_node);

                const new_node = &self.nodes.items[self.nodes.items.len - 1];

                const new_edge     = try RadixEdge(T).init(self.allocator);
                new_edge.len       = @truncate(num_chars_edge);
                new_edge.child_idx = self.nodes.items.len - 1;

                @memcpy(
                    new_edge.str[0..num_chars_edge], 
                    key[current_idx..current_idx+num_chars_edge],
                    );

                const mask_idx = @as(usize, @intCast(self.char_freq_table[new_edge.str[0]]));
                node.edge_data.freq_char_bitmask |= @intCast(
                    BITMASKS[mask_idx] & FULL_MASKS[0]
                    );

                if (mask_idx == 0) {
                    try node.addEdge(self.allocator, new_edge);
                } else {
                    try node.addEdgePos(&self.char_freq_table, self.allocator, new_edge);
                }

                if (is_leaf) break;

                rem_chars   -= MAX_STRING_LEN;
                current_idx += num_chars_edge;
                node         = new_node;
            }
        }

        pub fn insert(self: *Self, key: []const u8, value: T) !void {
            if (key.len == 0) return;

            try self.nodes.ensureUnusedCapacity(256);

            var node = &self.nodes.items[0];
            var key_idx: usize = 0;

            while (true) {
                var next_node: *NodeType = undefined;
                var max_edge_idx: usize = 0;
                var max_lcp: u8 = 0;
                var partial: bool = false;

                const shift_len: usize = @intCast(self.char_freq_table[key[key_idx]]);

                if (shift_len > 0) {
                    if ((BITMASKS[shift_len] & node.getMaskU64()) == 0) {

                        // Node doesn't exist. Insert.
                        try self.addNode(key[key_idx..], node, value);
                        self.num_keys += 1;
                        self.num_nodes += 1;
                        self.num_edges += 1;
                        return;
                    }

                    const access_idx: usize = getInsertIdx(
                        &self.char_freq_table, 
                        node.getMaskU64(),
                        key[key_idx],
                        );

                    const edge   = node.edges[access_idx];
                    max_lcp      = LCP(key[key_idx..], edge.str[0..edge.len]);
                    next_node    = &self.nodes.items[node.edges[access_idx].child_idx];
                    partial      = max_lcp < edge.len;
                    max_edge_idx = access_idx;

                } else {
                    const start_idx: usize = @popCount(node.getMaskU64() & FULL_MASKS[0]);
                    for (start_idx..node.edge_data.num_edges) |edge_idx| {
                        const current_edge   = node.edges[edge_idx];
                        const current_prefix = current_edge.str[0..current_edge.len];
                        const lcp = LCP(key[key_idx..], current_prefix);

                        if (lcp > max_lcp) {
                            max_lcp   = lcp;
                            max_edge_idx = edge_idx;
                            next_node = &self.nodes.items[node.edges[edge_idx].child_idx];
                            partial   = lcp < current_prefix.len;
                            break;
                        }
                    }
                    if (max_lcp == 0) {
                        try self.addNode(key[key_idx..], node, value);
                        self.num_keys += 1;
                        self.num_nodes += 1;
                        self.num_edges += 1;
                        return;
                    }
                }

                key_idx += max_lcp;

                // Matched rest of key. Node already exists. Replace.
                if (!partial and (key_idx == key.len)) {
                    // Make terminal if not.
                    self.num_keys += @intFromBool((next_node.edge_data.freq_char_bitmask & 1) == 0);
                    next_node.edge_data.freq_char_bitmask |= 1;
                    next_node.value = value;
                    return;
                }

                const rem_chars: usize = key.len - key_idx;
                if (partial) {
                    // Split
                    if (rem_chars == 0) {
                        var existing_edge = &node.edges[max_edge_idx];
                        const _new_node = NodeType{
                            .edge_data = .{
                                .freq_char_bitmask = 0,
                                .num_edges = 0,
                            },
                            .value = value,
                            .edges = undefined,
                        };
                        try self.nodes.append(_new_node);
                        const new_node = &self.nodes.items[self.nodes.items.len - 1];
                        const new_edge = try RadixEdge(T).init(self.allocator);

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
                        new_node.edge_data.freq_char_bitmask = @truncate(
                            BITMASKS[self.char_freq_table[new_edge.str[0]]] | BITMASKS[0]
                            );

                        try new_node.addEdgePos(&self.char_freq_table, self.allocator, new_edge);
                    } else {
                        var existing_edge = &node.edges[max_edge_idx];
                        const _new_node_1 = NodeType{
                            .edge_data = .{
                                .freq_char_bitmask = 0,
                                .num_edges = 0,
                            },
                            .value = value,
                            .edges = undefined,
                        };
                        try self.nodes.append(_new_node_1);
                        const new_node_1 = &self.nodes.items[self.nodes.items.len - 1];
                        const new_edge_2 = try RadixEdge(T).init(self.allocator);

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

                        new_node_1.edge_data.freq_char_bitmask = @intCast(
                            BITMASKS[self.char_freq_table[new_edge_2.str[0]]] & FULL_MASKS[0]
                            );
                        try new_node_1.addEdgePos(&self.char_freq_table, self.allocator, new_edge_2);

                        try self.addNode(key[key.len-rem_chars..], new_node_1, value);
                    }

                    self.num_keys += 1;
                    return;
                }

                // Traverse
                node = next_node;
            }
            @panic("This should be unreachable.\n");
        }

        pub fn find(self: *const Self, key: []const u8) !T {
            var node = self.nodes.items[0];
            var key_idx: usize = 0;

            while (key_idx < key.len) {
                if (node.edge_data.num_edges == 0) {
                    return error.ValueNotFound;
                }

                const shift_len:  usize = @intCast(self.char_freq_table[key[key_idx]]);
                const access_idx: usize = @popCount(
                    node.getMaskU64() & FULL_MASKS[shift_len]
                    );

                if (shift_len > 0) {
                    std.debug.assert(access_idx < node.edge_data.num_edges);

                    const current_edge   = node.edges[access_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                        node     = self.nodes.items[current_edge.child_idx];
                        key_idx += current_prefix.len;

                        if ((key_idx == key.len) and (node.getMaskU64() & BITMASKS[0] == 1)) {
                            return node.value;
                        }
                    } else {
                        return error.ValueNotFound;
                    }
                } else {
                    var matched = false;
                    for (access_idx..node.edge_data.num_edges) |edge_idx| {
                        const current_edge   = node.edges[edge_idx];
                        const current_prefix = current_edge.str[0..current_edge.len];

                        if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                            matched = true;
                            node     = self.nodes.items[node.edges[edge_idx].child_idx];
                            key_idx += current_prefix.len;

                            if ((key_idx == key.len) and (node.getMaskU64() & BITMASKS[0] == 1)) {
                                return node.value;
                            }
                            break;
                        }
                    }
                    if (!matched) {
                        return error.ValueNotFound;
                    }
                }

            }
            if (node.getMaskU64() & BITMASKS[0] == 1) {
                return node.value;
            }
            return error.ValueNotFound;
        }

        fn gatherChildrenBFS(
            self: *const Self, 
            starting_node: *const NodeType, 
            nodes_array: *[]Entry,
            current_node_idx: *usize,
            current_prefix: []const u8,
            ) !void {

            for (0..starting_node.edge_data.num_edges) |edge_idx| {
                if (current_node_idx.* == nodes_array.len) return;

                const edge  = starting_node.edges[edge_idx];
                const child = self.nodes.items[edge.child_idx];

                const new_prefix = try std.mem.concat(
                    self.allocator, 
                    u8, 
                    &[_][]const u8{current_prefix, edge.str[0..edge.len]},
                    );

                if ((child.edge_data.freq_char_bitmask & 1) == 1) {
                    nodes_array.*[current_node_idx.*] = Entry{
                        .key = new_prefix,
                        .value = child.value,
                    };
                    current_node_idx.* += 1;
                }

                if (current_node_idx.* == nodes_array.len) return;
                try self.gatherChildrenBFS(&child, nodes_array, current_node_idx, new_prefix);
            }
        }

        pub fn getPrefixNodes(
            self: *const Self, 
            key: []const u8, 
            matching_nodes: *[]Entry,
            ) !usize {

            var node = self.nodes.items[0];
            var key_idx: usize = 0;

            var result_idx: usize = 0;

            while (key_idx < key.len) {
                if (node.edge_data.num_edges == 0) {
                    return error.ValueNotFound;
                }

                const shift_len:  usize = @intCast(self.char_freq_table[key[key_idx]]);
                const access_idx: usize = @popCount(
                    node.getMaskU64() & FULL_MASKS[shift_len]
                    );

                if (shift_len > 0) {
                    std.debug.assert(access_idx < node.edge_data.num_edges);

                    const current_edge   = node.edges[access_idx];
                    const current_prefix = current_edge.str[0..current_edge.len];

                    if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                        node     = self.nodes.items[current_edge.child_idx];
                        key_idx += current_prefix.len;

                        if ((key_idx == key.len) and (node.getMaskU64() & BITMASKS[0] == 1)) {
                            if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                                matching_nodes.*[result_idx] = Entry{
                                    .key = key,
                                    .value = node.value,
                                };
                                result_idx += 1;
                            }

                            try self.gatherChildrenBFS(
                                &node, 
                                matching_nodes, 
                                &result_idx,
                                key,
                                );
                            return result_idx;
                        }
                    } else {
                        if (key_idx > 0) {
                            if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                                matching_nodes.*[result_idx] = Entry{
                                    .key = key,
                                    .value = node.value,
                                };
                                result_idx += 1;
                            }

                            try self.gatherChildrenBFS(
                                &node, 
                                matching_nodes, 
                                &result_idx,
                                key[0..key_idx],
                                );
                            return result_idx;
                        }
                        return error.ValueNotFound;
                    }
                } else {
                    var matched = false;
                    for (access_idx..node.edge_data.num_edges) |edge_idx| {
                        const current_edge   = node.edges[edge_idx];
                        const current_prefix = current_edge.str[0..current_edge.len];

                        if (std.mem.startsWith(u8, key[key_idx..], current_prefix)) {
                            matched = true;
                            node     = self.nodes.items[node.edges[edge_idx].child_idx];
                            key_idx += current_prefix.len;

                            if ((key_idx == key.len) and (node.getMaskU64() & BITMASKS[0] == 1)) {
                                if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                                    matching_nodes.*[result_idx] = Entry{
                                        .key = key,
                                        .value = node.value,
                                    };
                                    result_idx += 1;
                                }

                                try self.gatherChildrenBFS(
                                    &node, 
                                    matching_nodes, 
                                    &result_idx,
                                    key,
                                    );
                                return result_idx;
                            }
                            break;
                        }
                    }
                    if (!matched) {
                        if (key_idx > 0) {
                            if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                                matching_nodes.*[result_idx] = Entry{
                                    .key = key,
                                    .value = node.value,
                                };
                                result_idx += 1;
                            }

                            try self.gatherChildrenBFS(
                                &node, 
                                matching_nodes, 
                                &result_idx,
                                key[0..key_idx],
                                );
                            return result_idx;
                        }
                        return error.ValueNotFound;
                    }
                }

            }
            if (node.getMaskU64() & BITMASKS[0] == 1) {
                if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                    matching_nodes.*[result_idx] = Entry{
                        .key = key,
                        .value = node.value,
                    };
                    result_idx += 1;
                }

                try self.gatherChildrenBFS(
                    &node, 
                    matching_nodes, 
                    &result_idx,
                    key,
                    );
                return result_idx;
            }
            if (key_idx > 0) {
                if ((node.edge_data.freq_char_bitmask & 1) == 1) {
                    matching_nodes.*[result_idx] = Entry{
                        .key = key,
                        .value = node.value,
                    };
                    result_idx += 1;
                }

                try self.gatherChildrenBFS(
                    &node, 
                    matching_nodes, 
                    &result_idx,
                    key[0..key_idx],
                    );
                return result_idx;
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

    var trie = try RadixTrie(u32).init(allocator);
    defer trie.deinit();

    try trie.insert("ostritch", 24);
    try trie.insert("test", 420);
    try trie.insert("testing", 69);
    try trie.insert("tes", 24);
    try trie.insert("waddup", 54);
    try trie.insert("newting", 44);
    try trie.insert("tosting", 69);
    try trie.insert("toaster", 84);
    try trie.insert("magisterialness", 64);
    try trie.insert("abracadabra", 121);
    try trie.insert("rapper", 85);
    try trie.insert("apple", 25);
    try trie.insert("apocryphol", 25);
    try trie.insert("apacryphol", 25);
    try trie.insert("apicryphol", 25);
    try trie.insert("eager", 25);
    try trie.insert("mantequilla", 25);
    try trie.insert("initial", 25);
    try trie.insert("initial", 32);

    const limit: usize = 10;
    var matching_nodes = try trie.allocator.alloc(RadixTrie(u32).Entry, limit);

    const nodes_found = try trie.getPrefixNodes("tes", &matching_nodes);
    for (0..nodes_found) |i| {
        print("Node {d} - Key: {s} Value: {d}\n", .{i, matching_nodes[i].key, matching_nodes[i].value});
    }

    // trie.printNodes();
    // trie.nodes.items[0].printEdges();

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

    var trie = try RadixTrie(u32).init(allocator);
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

    const start_freq_table = std.time.microTimestamp();
    _ = trie.buildFrequencyTable(raw_keys);
    const end_freq_table = std.time.microTimestamp();
    const elapsed_freq_table = end_freq_table - start_freq_table;

    print("Finished building frequency table\n", .{});

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
        try trie.insert(raw_keys[j], i);
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


    const million_insertions_per_second_freq_table = @as(f32, @floatFromInt(N)) / @as(f32, @floatFromInt(elapsed_freq_table));
    std.debug.print("------------------  FREQ TABLE ------------------\n", .{});
    std.debug.print("\nFrequency table construction time: {}ms\n", .{@divFloor(elapsed_freq_table, 1000)});
    std.debug.print("Million insertions per second:     {d}\n", .{million_insertions_per_second_freq_table});


    std.debug.print("\n\n", .{});

    const limit: usize = 10;

    var matching_nodes = try trie.allocator.alloc(RadixTrie(u32).Entry, limit);
    const start_prefix = std.time.nanoTimestamp();
    const nodes_found = try trie.getPrefixNodes("m", &matching_nodes);
    const end_prefix = std.time.nanoTimestamp();

    const elapsed_prefix = end_prefix - start_prefix;
    print("Prefix search time: {}us\n", .{@divFloor(elapsed_prefix, 1000)});
    print("Num nodes found: {d}\n", .{nodes_found});

    for (0..@min(nodes_found, 100)) |idx| {
        print("Node {d} - Key: {s} Value: {d}\n", .{idx, matching_nodes[idx].key, matching_nodes[idx].value});
    }
}
