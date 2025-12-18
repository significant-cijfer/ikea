const std = @import("std");

const cwd = std.fs.cwd;
const splitScalar = std.mem.splitScalar;
const tokenizeScalar = std.mem.tokenizeScalar;

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;

pub const Database = struct {
    allocator: Allocator,
    tables: StringHashMap(View),
    nodes: ArrayList(Node),
    ints: ArrayList(i64),
    flts: ArrayList(f64),

    pub fn init(gpa: Allocator) Database {
        return .{
            .allocator = gpa,
            .tables = .init(gpa),
            .nodes = .empty,
            .ints = .empty,
            .flts = .empty,
        };
    }

    pub fn deinit(self: *Database) void {
        self.ints.deinit(self.allocator);
        self.flts.deinit(self.allocator);
        self.nodes.deinit(self.allocator);
    }

    pub fn readParquet(self: *Database, name: []const u8, path: []const u8) !void {
        _ = name;

        const query = try std.fmt.allocPrint(
            self.allocator,
            "SELECT * FROM read_parquet('{s}')",
            .{path}
        );

        defer self.allocator.free(query);

        const result = try std.process.Child.run(.{
            .allocator = self.allocator,
            .argv = &.{ "duckdb", "-json", "-c", query },
            .max_output_bytes = 1024 * 1024 * 1024,
        });

        defer self.allocator.free(result.stdout);
        defer self.allocator.free(result.stderr);

        if (result.term != .Exited or result.term.Exited != 0) {
            std.debug.print("Python error: {s}\n", .{result.stderr});
            return error.PythonError;
        }

        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            result.stdout,
            .{},
        );

        defer parsed.deinit();

        const array = parsed.value.array.items;

        for (array) |value| {
            std.debug.print("Columns:\n", .{});
            for (value.object.keys(), value.object.values()) |k, v|
                std.debug.print("    {s}: {},\n", .{k, v});
        }
    }
};

const View = struct {
    fields: StringHashMap(Node.idx),
    kinds: StringHashMap(Kind),

    const Kind = enum {
        int, //integer
        flt, //float
    };

    fn allocator(self: View) Allocator {
        return self.fields.allocator;
    }

    fn init(gpa: Allocator, names: []const []const u8) View {
        var fields = .init(gpa);

        for (names) |name| {
            try fields.put(name, .empty);
        }

        return .{ .fields = fields };
    }
};

const Node = struct {
    trunks: [radix]usize,
    leaves: [radix]bool,

    //TODO, test different radix's
    const radix = 8;

    const idx = u64;
};
