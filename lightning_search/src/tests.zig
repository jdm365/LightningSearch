const std = @import("std");

const DocStore = @import("storage/doc_store.zig").DocStore;
const TermPos  = @import("server/server.zig").TermPos;


test "DocStore" {
    // TODO: Actually implement.
    // Use sample csv rows and test that read/write returns the expected result.
    const test_csv_rows = "hello,one,two,three\nworld,four,five,six\nzig,seven,eight,nine\n";
    const byte_positions = [_]TermPos{
        TermPos{ .start_pos = 0, .field_len = 5 },
        TermPos{ .start_pos = 6, .field_len = 3 },
        TermPos{ .start_pos = 10, .field_len = 3 },
        TermPos{ .start_pos = 14, .field_len = 5 },

        TermPos{ .start_pos = 20, .field_len = 5 },
        TermPos{ .start_pos = 26, .field_len = 4 },
        TermPos{ .start_pos = 31, .field_len = 4 },
        TermPos{ .start_pos = 36, .field_len = 3 },

        TermPos{ .start_pos = 40, .field_len = 3 },
        TermPos{ .start_pos = 44, .field_len = 5 },
        TermPos{ .start_pos = 50, .field_len = 5 },
        TermPos{ .start_pos = 56, .field_len = 4 },
    };

    var gpa = std.heap.DebugAllocator(.{}){};
    const doc_store = try DocStore.init(
        &gpa,
        &std.ArrayListUnmanaged(usize){},
        &std.ArrayListUnmanaged(usize){},
        &std.ArrayListUnmanaged(usize){},
        "test_dir",
        0,
    );
    defer doc_store.deinit() catch {};

    // Test adding a row
    try doc_store.addRow(byte_positions[0..], test_csv_rows[0..]);

    // Test flushing
    try doc_store.flush();

    // Test getting a row
    var retrieved_row_data = try doc_store.gpa.allocator().alloc(
        u8, 
        doc_store.literal_byte_size_sum,
        );
    defer doc_store.gpa.allocator().free(retrieved_row_data);

    var retrieved_byte_positions = [_]TermPos{
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },

        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },

        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
        TermPos{ .start_pos = 0, .field_len = 0 },
    };
    try doc_store.getRow(0, retrieved_row_data[0..], retrieved_byte_positions[0..]);

    std.debug.print("Retrieved Row Data: {s}\n", .{retrieved_row_data});
}
