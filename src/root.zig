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
    strs: ArrayList([]const u8),

    pub fn init(gpa: Allocator) Database {
        return .{
            .allocator = gpa,
            .tables = .init(gpa),
            .nodes = .empty,
            .ints = .empty,
            .flts = .empty,
            .strs = .empty,
        };
    }

    pub fn deinit(self: *Database) void {
        self.nodes.deinit(self.allocator);
        self.ints.deinit(self.allocator);
        self.flts.deinit(self.allocator);
        self.strs.deinit(self.allocator);
    }

    pub fn readParquet(self: *Database, table: []const u8, path: []const u8) !void {
        var timer = try std.time.Timer.start();

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

        const time_read = timer.lap();

        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            result.stdout,
            .{},
        );

        const time_parse = timer.lap();

        const array = parsed.value.array.items;

        var fields: StringHashMap(ArrayList(u64))   = .init(self.allocator);
        var kinds:  StringHashMap(View.Kind)        = .init(self.allocator);
        var roots:  StringHashMap(Node.idx)         = .init(self.allocator);

        const first = if (array.len == 0) null else array[0].object;

        if (first) |f| for (f.keys(), f.values()) |k, _| {
            try fields.put(k, .empty);
        };

        for (array) |value| {
            for (value.object.keys(), value.object.values()) |key, val| {
                const field = fields.getPtr(key).?;
                const int_idx = self.ints.items.len;
                const flt_idx = self.flts.items.len;
                const str_idx = self.strs.items.len;

                switch (val) {
                    .null => {},
                    .string  => try kinds.put(key, .str),
                    .integer => try kinds.put(key, .int),
                    .float   => try kinds.put(key, .flt),
                    else => unreachable,
                }

                switch (val) {
                    .null => {},
                    .string  => |v| try self.strs.append(self.allocator, v),
                    .integer => |v| try self.ints.append(self.allocator, v),
                    .float   => |v| try self.flts.append(self.allocator, v),
                    else => unreachable,
                }

                switch (val) {
                    .null => {},
                    .string  => try field.append(self.allocator, str_idx),
                    .integer => try field.append(self.allocator, int_idx),
                    .float   => try field.append(self.allocator, flt_idx),
                    else => unreachable,
                }
            }
        }

        var iter = kinds.iterator();

        while (iter.next()) |entry| {
            const name = entry.key_ptr.*;
            const list = fields.get(name).?.items;

            try roots.put(name, try self.treeify(list));
        }

        try self.tables.put(table, .{
            .parsed = parsed,
            .fields = roots,
            .kinds = kinds,
        });

        const time_view = timer.lap();

        std.debug.print("time_read:  {}ns, {}ms\n", .{time_read, time_read/1000000});
        std.debug.print("time_parse: {}ns, {}ms\n", .{time_parse, time_parse/1000000});
        std.debug.print("time_view:  {}ns, {}ms\n", .{time_view, time_view/1000000});
    }

    fn treeify(self: *Database, slice: []const u64) !Node.idx {
        const len = slice.len;

        if (len == 0) return 0;
        if (len == 1) return slice[0];

        const piece = (len + 4 - 1) / 4;
        const a0 = @min(0*piece, len);
        const a1 = @min(1*piece, len);
        const b0 = @min(1*piece, len);
        const b1 = @min(2*piece, len);
        const c0 = @min(2*piece, len);
        const c1 = @min(3*piece, len);
        const d0 = @min(3*piece, len);
        const d1 = @min(4*piece, len);

        try self.nodes.append(self.allocator, .{
            .trunks = .{
                try self.treeify(slice[a0..a1]),
                try self.treeify(slice[b0..b1]),
                try self.treeify(slice[c0..c1]),
                try self.treeify(slice[d0..]),
            },
            .leaves = .{
                a1-a0 < 2,
                b1-b0 < 2,
                c1-c0 < 2,
                d1-d0 < 2,
            },
        });

        return self.nodes.items.len - 1;
    }

    pub fn debugTable(self: Database, table: []const u8) void {
        const view = self.tables.get(table).?;
        var fields = view.fields.iterator();

        while (fields.next()) |entry| {
            const name = entry.key_ptr.*;
            const node = entry.value_ptr.*;

            const kind = view.kinds.get(name).?;
            self.debugNode(kind, node, 0);

            std.debug.print("{s}: {}\n", .{name, node});
        }
    }

    fn debugNode(self: Database, kind: View.Kind, idx: u64, depth: u64) void {
        const node = self.nodes.items[idx];

        for (&node.trunks, &node.leaves) |trunk, leaf| {
            if (leaf) switch (kind) {
                .str => std.debug.print("{s}\n", .{self.strs.items[trunk]}),
                .int => std.debug.print("{}\n", .{self.ints.items[trunk]}),
                .flt => std.debug.print("{}\n", .{self.flts.items[trunk]}),
            } else {
                self.debugNode(kind, trunk, depth+1);
            }
        }
    }
};

const View = struct {
    parsed: ?std.json.Parsed(std.json.Value),
    fields: StringHashMap(Node.idx),
    kinds: StringHashMap(Kind),

    const Kind = enum {
        int, //integer
        flt, //float
        str, //string
    };

    fn allocator(self: View) Allocator {
        return self.fields.allocator;
    }

    pub fn deinit(self: View) void {
        if (self.parsed) |p|
            p.deinit();

        self.fields.deinit();
        self.kinds.deinit();
    }
};

const Node = struct {
    trunks: [radix]idx,
    leaves: [radix]bool,

    //TODO, test different radix's
    const radix = 4;

    const idx = u64;
};
