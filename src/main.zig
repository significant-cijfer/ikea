const std = @import("std");
const lib = @import("ikea");

const Database = lib.Database;

pub fn main() !void {
    const gpa = std.heap.page_allocator;

    var db = Database.init(gpa);
    defer db.deinit();

    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();

    const root = args.next() orelse return error.NotEnoughArgs;
    const path = args.next() orelse return error.NotEnoughArgs;
    _ = root;

    const stem = std.fs.path.stem(path);
    try db.readParquet(stem, path);
    //db.debugTable(stem);
}
