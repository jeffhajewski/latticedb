//! Helpers over the syntax tree that the planner needs in several places.

const std = @import("std");
const ast = @import("ast.zig");

/// Whether two expressions are written the same way, ignoring where they
/// are written. Function names compare without case, as they are looked up.
///
/// The planner uses this to recognise one expression in two places: an
/// ORDER BY item that names a RETURN item, or the same aggregate call written
/// twice in one projection.
pub fn expressionsEqual(a: *const ast.Expression, b: *const ast.Expression) bool {
    if (std.meta.activeTag(a.*) != std.meta.activeTag(b.*)) return false;
    return switch (a.*) {
        .literal => |al| literalsEqual(al.value, b.literal.value),
        .variable => |av| std.mem.eql(u8, av.name, b.variable.name),
        .parameter => |ap| std.mem.eql(u8, ap.name, b.parameter.name),
        .property_access => |ap| std.mem.eql(u8, ap.property, b.property_access.property) and
            expressionsEqual(ap.object, b.property_access.object),
        .binary => |ab| ab.operator == b.binary.operator and
            expressionsEqual(ab.left, b.binary.left) and
            expressionsEqual(ab.right, b.binary.right),
        .unary => |au| au.operator == b.unary.operator and expressionsEqual(au.operand, b.unary.operand),
        .function_call => |af| blk: {
            const bf = b.function_call;
            if (!std.ascii.eqlIgnoreCase(af.name, bf.name)) break :blk false;
            if (af.distinct != bf.distinct) break :blk false;
            break :blk allEqual(af.arguments, bf.arguments);
        },
        .list => |al| allEqual(al.elements, b.list.elements),
        .map => |am| blk: {
            const bm = b.map;
            if (am.entries.len != bm.entries.len) break :blk false;
            for (am.entries, bm.entries) |ae, be| {
                if (!std.mem.eql(u8, ae.key, be.key)) break :blk false;
                if (!expressionsEqual(ae.value, be.value)) break :blk false;
            }
            break :blk true;
        },
        .index => |ai| expressionsEqual(ai.object, b.index.object) and expressionsEqual(ai.index, b.index.index),
        .slice => |as| expressionsEqual(as.object, b.slice.object) and
            optionalEqual(as.from, b.slice.from) and
            optionalEqual(as.to, b.slice.to),
        .case_expr => |ac| blk: {
            const bc = b.case_expr;
            if (!optionalEqual(ac.subject, bc.subject)) break :blk false;
            if (!optionalEqual(ac.else_expr, bc.else_expr)) break :blk false;
            if (ac.branches.len != bc.branches.len) break :blk false;
            for (ac.branches, bc.branches) |ab, bb| {
                if (!expressionsEqual(ab.when, bb.when) or !expressionsEqual(ab.then, bb.then)) break :blk false;
            }
            break :blk true;
        },
        .comprehension => |ac| blk: {
            const bc = b.comprehension;
            break :blk ac.kind == bc.kind and std.mem.eql(u8, ac.variable, bc.variable) and
                expressionsEqual(ac.list, bc.list) and optionalEqual(ac.where, bc.where) and optionalEqual(ac.map, bc.map);
        },
        // The same subquery, not one written the same way: each has a plan
        // of its own.
        .subquery => |as| as == b.subquery,
        .reduce => |ar| blk: {
            const br = b.reduce;
            break :blk std.mem.eql(u8, ar.accumulator, br.accumulator) and std.mem.eql(u8, ar.variable, br.variable) and
                expressionsEqual(ar.initial, br.initial) and expressionsEqual(ar.list, br.list) and
                expressionsEqual(ar.expression, br.expression);
        },
    };
}

fn allEqual(a: []const *ast.Expression, b: []const *ast.Expression) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!expressionsEqual(x, y)) return false;
    }
    return true;
}

fn optionalEqual(a: ?*const ast.Expression, b: ?*const ast.Expression) bool {
    if (a == null or b == null) return a == null and b == null;
    return expressionsEqual(a.?, b.?);
}

fn literalsEqual(a: ast.LiteralValue, b: ast.LiteralValue) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .integer => |x| x == b.integer,
        // Bitwise, so that two NaN literals are the same text.
        .float => |x| @as(u64, @bitCast(x)) == @as(u64, @bitCast(b.float)),
        .string => |x| std.mem.eql(u8, x, b.string),
        .boolean => |x| x == b.boolean,
        .null_value => true,
    };
}

pub const RewriteError = error{ OutOfMemory, Refused };

/// A copy of `expr` with some subexpressions replaced.
///
/// `context.replace(e)` is asked about every subexpression, outermost first.
/// It returns the replacement, or null to keep `e` and look inside it. The
/// copy is allocated in `allocator`; nothing in `expr` changes, because the
/// tree may be shared through the query cache.
pub fn rewrite(allocator: std.mem.Allocator, expr: *ast.Expression, context: anytype) RewriteError!*ast.Expression {
    if (try context.replace(expr)) |replacement| return replacement;
    const out = try allocator.create(ast.Expression);
    out.* = expr.*;
    switch (expr.*) {
        .literal, .variable, .parameter => {},
        .property_access => |pa| {
            const copy = try allocator.create(ast.PropertyAccess);
            copy.* = pa.*;
            copy.object = try rewrite(allocator, pa.object, context);
            out.* = .{ .property_access = copy };
        },
        .binary => |b| {
            const copy = try allocator.create(ast.BinaryExpr);
            copy.* = b.*;
            copy.left = try rewrite(allocator, b.left, context);
            copy.right = try rewrite(allocator, b.right, context);
            out.* = .{ .binary = copy };
        },
        .unary => |u| {
            const copy = try allocator.create(ast.UnaryExpr);
            copy.* = u.*;
            copy.operand = try rewrite(allocator, u.operand, context);
            out.* = .{ .unary = copy };
        },
        .function_call => |f| {
            const copy = try allocator.create(ast.FunctionCall);
            copy.* = f.*;
            copy.arguments = try rewriteAll(allocator, f.arguments, context);
            out.* = .{ .function_call = copy };
        },
        .list => |l| {
            const copy = try allocator.create(ast.ListExpr);
            copy.* = l.*;
            copy.elements = try rewriteAll(allocator, l.elements, context);
            out.* = .{ .list = copy };
        },
        .map => |m| {
            const copy = try allocator.create(ast.MapExpr);
            copy.* = m.*;
            const entries = try allocator.alloc(ast.PropertyEntry, m.entries.len);
            for (m.entries, entries) |entry, *new_entry| {
                new_entry.* = .{ .key = entry.key, .value = try rewrite(allocator, entry.value, context) };
            }
            copy.entries = entries;
            out.* = .{ .map = copy };
        },
        .index => |i| {
            const copy = try allocator.create(ast.IndexExpr);
            copy.* = i.*;
            copy.object = try rewrite(allocator, i.object, context);
            copy.index = try rewrite(allocator, i.index, context);
            out.* = .{ .index = copy };
        },
        .slice => |s| {
            const copy = try allocator.create(ast.SliceExpr);
            copy.* = s.*;
            copy.object = try rewrite(allocator, s.object, context);
            copy.from = try rewriteOptional(allocator, s.from, context);
            copy.to = try rewriteOptional(allocator, s.to, context);
            out.* = .{ .slice = copy };
        },
        .case_expr => |c| {
            const copy = try allocator.create(ast.CaseExpr);
            copy.* = c.*;
            copy.subject = try rewriteOptional(allocator, c.subject, context);
            copy.else_expr = try rewriteOptional(allocator, c.else_expr, context);
            const branches = try allocator.alloc(ast.CaseBranch, c.branches.len);
            for (c.branches, branches) |branch, *new_branch| {
                new_branch.* = .{
                    .when = try rewrite(allocator, branch.when, context),
                    .then = try rewrite(allocator, branch.then, context),
                };
            }
            copy.branches = branches;
            out.* = .{ .case_expr = copy };
        },
        // Only the list is the enclosing expression's: the rest sees the
        // comprehension's own variable, which a replacement made for the
        // enclosing scope must not reach.
        // Kept as it is: its plan is found by this node.
        .subquery => {},
        .comprehension => |c| {
            const copy = try allocator.create(ast.ListComprehension);
            copy.* = c.*;
            copy.list = try rewrite(allocator, c.list, context);
            out.* = .{ .comprehension = copy };
        },
        .reduce => |r| {
            const copy = try allocator.create(ast.ReduceExpr);
            copy.* = r.*;
            copy.initial = try rewrite(allocator, r.initial, context);
            copy.list = try rewrite(allocator, r.list, context);
            out.* = .{ .reduce = copy };
        },
    }
    return out;
}

/// The pattern subqueries in `expr`, outermost only: those inside one are
/// its own.
pub fn collectSubqueries(allocator: std.mem.Allocator, expr: *const ast.Expression, out: *std.ArrayList(*const ast.PatternSubquery)) error{ OutOfMemory, Refused }!void {
    return collect(allocator, expr, out, false);
}

fn collect(allocator: std.mem.Allocator, expr: *const ast.Expression, out: *std.ArrayList(*const ast.PatternSubquery), local: bool) error{ OutOfMemory, Refused }!void {
    switch (expr.*) {
        .literal, .parameter, .variable => {},
        // Its plan runs apart from the evaluator that holds a comprehension's
        // variable, so it could not see one.
        .subquery => |sq| if (local) return error.Refused else try out.append(allocator, sq),
        .property_access => |p| try collect(allocator, p.object, out, local),
        .binary => |b| {
            try collect(allocator, b.left, out, local);
            try collect(allocator, b.right, out, local);
        },
        .unary => |u| try collect(allocator, u.operand, out, local),
        .function_call => |f| for (f.arguments) |a| try collect(allocator, a, out, local),
        .list => |l| for (l.elements) |e| try collect(allocator, e, out, local),
        .map => |m| for (m.entries) |e| try collect(allocator, e.value, out, local),
        .index => |i| {
            try collect(allocator, i.object, out, local);
            try collect(allocator, i.index, out, local);
        },
        .slice => |s| {
            try collect(allocator, s.object, out, local);
            if (s.from) |f| try collect(allocator, f, out, local);
            if (s.to) |t| try collect(allocator, t, out, local);
        },
        .case_expr => |c| {
            if (c.subject) |s| try collect(allocator, s, out, local);
            for (c.branches) |b| {
                try collect(allocator, b.when, out, local);
                try collect(allocator, b.then, out, local);
            }
            if (c.else_expr) |e| try collect(allocator, e, out, local);
        },
        .comprehension => |c| {
            try collect(allocator, c.list, out, local);
            if (c.where) |w| try collect(allocator, w, out, true);
            if (c.map) |m| try collect(allocator, m, out, true);
        },
        .reduce => |r| {
            try collect(allocator, r.initial, out, local);
            try collect(allocator, r.list, out, local);
            try collect(allocator, r.expression, out, true);
        },
    }
}

/// The first variable `expr` reads from its enclosing scope, skipping those
/// `ignore(name)` accepts and those a comprehension or reduce inside it
/// binds itself.
pub fn firstFreeVariable(expr: *const ast.Expression, ignore: *const fn ([]const u8) bool) ?[]const u8 {
    var scope: Scope = .{};
    return freeIn(expr, ignore, &scope);
}

const Scope = struct {
    names: [32][]const u8 = undefined,
    len: usize = 0,

    fn has(self: *const Scope, name: []const u8) bool {
        for (self.names[0..self.len]) |n| {
            if (std.mem.eql(u8, n, name)) return true;
        }
        return false;
    }
};

fn freeIn(expr: *const ast.Expression, ignore: *const fn ([]const u8) bool, scope: *Scope) ?[]const u8 {
    switch (expr.*) {
        .literal, .parameter => return null,
        // What it reads from the enclosing row is in its patterns and WHERE,
        // whose variables a free-variable check would also have to scope.
        // Reported as its first pattern variable bound outside, if any.
        .subquery => |sq| {
            for (sq.patterns) |pattern| for (pattern.elements) |element| {
                const name = switch (element) {
                    .node => |n| n.variable,
                    .edge => |e| e.variable,
                } orelse continue;
                if (!scope.has(name) and !ignore(name)) return name;
            };
            return null;
        },
        .variable => |v| return if (scope.has(v.name) or ignore(v.name)) null else v.name,
        .property_access => |p| return freeIn(p.object, ignore, scope),
        .binary => |b| return freeIn(b.left, ignore, scope) orelse freeIn(b.right, ignore, scope),
        .unary => |u| return freeIn(u.operand, ignore, scope),
        .function_call => |f| return freeInAll(f.arguments, ignore, scope),
        .list => |l| return freeInAll(l.elements, ignore, scope),
        .map => |m| {
            for (m.entries) |e| if (freeIn(e.value, ignore, scope)) |name| return name;
            return null;
        },
        .index => |i| return freeIn(i.object, ignore, scope) orelse freeIn(i.index, ignore, scope),
        .slice => |s| return freeIn(s.object, ignore, scope) orelse freeInOptional(s.from, ignore, scope) orelse
            freeInOptional(s.to, ignore, scope),
        .case_expr => |c| {
            if (freeInOptional(c.subject, ignore, scope)) |name| return name;
            for (c.branches) |b| if (freeIn(b.when, ignore, scope) orelse freeIn(b.then, ignore, scope)) |name| return name;
            return freeInOptional(c.else_expr, ignore, scope);
        },
        .comprehension => |c| {
            if (freeIn(c.list, ignore, scope)) |name| return name;
            if (scope.len == scope.names.len) return null;
            scope.names[scope.len] = c.variable;
            scope.len += 1;
            defer scope.len -= 1;
            return freeInOptional(c.where, ignore, scope) orelse freeInOptional(c.map, ignore, scope);
        },
        .reduce => |r| {
            if (freeIn(r.initial, ignore, scope) orelse freeIn(r.list, ignore, scope)) |name| return name;
            if (scope.len + 2 > scope.names.len) return null;
            scope.names[scope.len] = r.accumulator;
            scope.names[scope.len + 1] = r.variable;
            scope.len += 2;
            defer scope.len -= 2;
            return freeIn(r.expression, ignore, scope);
        },
    }
}

fn freeInAll(exprs: []const *ast.Expression, ignore: *const fn ([]const u8) bool, scope: *Scope) ?[]const u8 {
    for (exprs) |e| if (freeIn(e, ignore, scope)) |name| return name;
    return null;
}

fn freeInOptional(expr: ?*const ast.Expression, ignore: *const fn ([]const u8) bool, scope: *Scope) ?[]const u8 {
    return freeIn(expr orelse return null, ignore, scope);
}

fn rewriteAll(allocator: std.mem.Allocator, exprs: []*ast.Expression, context: anytype) RewriteError![]*ast.Expression {
    const out = try allocator.alloc(*ast.Expression, exprs.len);
    for (exprs, out) |e, *o| o.* = try rewrite(allocator, e, context);
    return out;
}

fn rewriteOptional(allocator: std.mem.Allocator, expr: ?*ast.Expression, context: anytype) RewriteError!?*ast.Expression {
    const e = expr orelse return null;
    return try rewrite(allocator, e, context);
}
