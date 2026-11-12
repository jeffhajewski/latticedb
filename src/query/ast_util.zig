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
    }
    return out;
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
