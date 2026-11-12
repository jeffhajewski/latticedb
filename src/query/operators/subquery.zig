//! Pattern subqueries inside expressions: `EXISTS { MATCH ... WHERE ... }`,
//! `COUNT { ... }`, a pattern used as a predicate (`WHERE (a)-->(b)`) and a
//! pattern comprehension (`[(a)-->(b) WHERE ... | b.name]`).
//!
//! The planner plans each one as it plans a MATCH, over an `Argument` leaf
//! (see `optional.zig`), and hands the plans to a `SubqueryHost` at the root
//! of the query's plan. The host makes them reachable from the execution
//! context, so the expression evaluator can run a subquery's plan for the
//! row it is evaluating: the argument yields that row once, and every row
//! the plan produces from it is one match.
//!
//! A plan is opened the first time it is needed and run to exhaustion each
//! time, even when the first match settles the answer, because an operator
//! left part way through a round would carry on from the previous row in the
//! next one.

const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../ast.zig");
const executor = @import("../executor.zig");
const Operator = executor.Operator;
const OperatorError = executor.OperatorError;
const Row = executor.Row;
const ExecutionContext = executor.ExecutionContext;
const optional_ops = @import("optional.zig");

/// The plan of one subquery.
pub const SubqueryPlan = struct {
    node: *const ast.PatternSubquery,
    plan: Operator,
    /// The leaves that hand the plan its row: one, or one per query of a
    /// UNION.
    arguments: []const *optional_ops.Argument,
    opened: bool = false,
    /// Opened and closed again for each row: a whole query, whose sorts,
    /// aggregates and UNIONs read their input when opened.
    reopen: bool = false,
    /// Set while a round is running, to refuse a subquery that would have
    /// to run inside itself.
    running: bool = false,

    /// Run the plan for `row`, calling `visitor.visit(match)` for each match.
    pub fn run(self: *SubqueryPlan, ctx: *ExecutionContext, row: *const Row, visitor: anytype) OperatorError!void {
        if (self.running) return OperatorError.EvaluationError;
        if (self.reopen) {
            for (self.arguments) |argument| argument.set(@constCast(row));
            self.running = true;
            defer self.running = false;
            try self.plan.open(ctx);
            defer self.plan.close(ctx);
            while (try self.plan.next(ctx)) |match| try visitor.visit(match);
            return;
        }
        if (!self.opened) {
            try self.plan.open(ctx);
            self.opened = true;
        }
        self.running = true;
        defer self.running = false;
        // The argument hands the row on; nothing writes through it.
        for (self.arguments) |argument| argument.set(@constCast(row));
        while (try self.plan.next(ctx)) |match| try visitor.visit(match);
    }
};

/// The root of a plan whose expressions hold subqueries: it makes their
/// plans reachable through the execution context while the query runs, and
/// closes the ones that ran.
pub const SubqueryHost = struct {
    input: Operator,
    plans: []SubqueryPlan,

    const Self = @This();

    /// Create a host over `input` that owns it and `plans`.
    pub fn init(allocator: Allocator, input: Operator, plans: []SubqueryPlan) !*Self {
        const self = try allocator.create(Self);
        self.* = .{ .input = input, .plans = plans };
        return self;
    }

    pub fn operator(self: *Self) Operator {
        return .{ .vtable = &vtable, .ptr = self };
    }

    /// The plan for `node`, if one was made.
    pub fn find(self: *Self, node: *const ast.PatternSubquery) ?*SubqueryPlan {
        for (self.plans) |*p| {
            if (p.node == node) return p;
        }
        return null;
    }

    const vtable = Operator.VTable{
        .open = open,
        .next = next,
        .close = close,
        .deinit = deinitOp,
    };

    fn open(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        ctx.subqueries = self;
        errdefer ctx.subqueries = null;
        try self.input.open(ctx);
    }

    fn next(ptr: *anyopaque, ctx: *ExecutionContext) OperatorError!?*Row {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.input.next(ctx);
    }

    fn close(ptr: *anyopaque, ctx: *ExecutionContext) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.close(ctx);
        for (self.plans) |*p| {
            if (p.opened) p.plan.close(ctx);
            p.opened = false;
        }
        ctx.subqueries = null;
    }

    fn deinitOp(ptr: *anyopaque, allocator: Allocator) void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.input.deinit(allocator);
        // Each plan's leaf is its argument, which the plan frees.
        for (self.plans) |p| {
            p.plan.deinit(allocator);
            allocator.free(p.arguments);
        }
        allocator.free(self.plans);
        allocator.destroy(self);
    }
};

test "SubqueryHost vtable is complete" {
    const v = SubqueryHost.vtable;
    try std.testing.expect(@TypeOf(v.open) != void);
}
