const std = @import("std");
const builtin = @import("builtin");

pub const ENDIANESS = builtin.cpu.arch.endian();

const SchemaElement = packed struct(u64) {
    col_idx: u32,
    byte_size: u32,
};


const DocStoreRow = packed struct {
    num_bytes_literal: u16,
    num_bytes_huffman: u16,
    data: [*]u8,
};

pub const DocStore = struct {
    rows: std.ArrayListUnmanaged(DocStoreRow),

    huffman_tree: HuffmanTree,
    literal_schema: std.ArrayListUnmanaged(SchemaElement),
    huffman_schema: std.ArrayListUnmanaged(SchemaElement),
};
