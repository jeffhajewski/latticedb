//! Canonical key encoding helpers for DISTINCT/GROUP BY deduplication.
//!
//! Encodes values losslessly into self-delimiting byte sequences so equality
//! checks are exact (no truncation-based collisions).

const std = @import("std");
const Allocator = std.mem.Allocator;

const executor = @import("../executor.zig");
const Row = executor.Row;
const SlotValue = executor.SlotValue;
const MAX_SLOTS = executor.MAX_SLOTS;

const expression = @import("../expression.zig");
const EvalResult = expression.EvalResult;

const lattice = @import("lattice");
const PropertyValue = lattice.core.types.PropertyValue;

pub fn appendRowKey(buf: *std.ArrayList(u8), allocator: Allocator, row: *const Row) !void {
    var slot: u8 = 0;
    while (slot < MAX_SLOTS) : (slot += 1) {
        if (!row.hasSlot(slot)) continue;
        try buf.append(allocator, slot);
        try appendSlotValueKey(buf, allocator, row.slots[slot]);
    }
}

pub fn appendSlotValueKey(buf: *std.ArrayList(u8), allocator: Allocator, value: SlotValue) !void {
    switch (value) {
        .empty => try buf.append(allocator, 0),
        .node_ref => |id| {
            try buf.append(allocator, 1);
            try appendU64(buf, allocator, id);
        },
        .edge_ref => |id| {
            try buf.append(allocator, 2);
            try appendU64(buf, allocator, id);
        },
        .property => |pv| {
            try buf.append(allocator, 3);
            try appendPropertyValueKey(buf, allocator, pv);
        },
    }
}

pub fn appendPropertyValueKey(buf: *std.ArrayList(u8), allocator: Allocator, value: PropertyValue) !void {
    switch (value) {
        .null_val => try buf.append(allocator, 0),
        .bool_val => |b| {
            try buf.append(allocator, 1);
            try buf.append(allocator, @intFromBool(b));
        },
        .int_val => |i| {
            try buf.append(allocator, 2);
            try appendU64(buf, allocator, @bitCast(i));
        },
        .float_val => |f| {
            try buf.append(allocator, 3);
            try appendU64(buf, allocator, @bitCast(f));
        },
        .string_val => |s| {
            try buf.append(allocator, 4);
            try appendBytes(buf, allocator, s);
        },
        .bytes_val => |b| {
            try buf.append(allocator, 5);
            try appendBytes(buf, allocator, b);
        },
        .vector_val => |v| {
            try buf.append(allocator, 6);
            try appendLen(buf, allocator, v.len);
            try appendBytes(buf, allocator, std.mem.sliceAsBytes(v));
        },
        .list_val => |list| {
            try buf.append(allocator, 7);
            try appendLen(buf, allocator, list.len);
            for (list) |item| {
                try appendPropertyValueKey(buf, allocator, item);
            }
        },
        .map_val => |entries| {
            try buf.append(allocator, 8);
            try appendLen(buf, allocator, entries.len);
            for (entries) |entry| {
                try appendBytes(buf, allocator, entry.key);
                try appendPropertyValueKey(buf, allocator, entry.value);
            }
        },
    }
}

pub fn appendEvalResultKey(buf: *std.ArrayList(u8), allocator: Allocator, value: EvalResult) !void {
    switch (value) {
        .null_val => try buf.append(allocator, 0),
        .bool_val => |b| {
            try buf.append(allocator, 1);
            try buf.append(allocator, @intFromBool(b));
        },
        .int_val => |i| {
            try buf.append(allocator, 2);
            try appendU64(buf, allocator, @bitCast(i));
        },
        .float_val => |f| {
            try buf.append(allocator, 3);
            try appendU64(buf, allocator, @bitCast(f));
        },
        .string_val => |s| {
            try buf.append(allocator, 4);
            try appendBytes(buf, allocator, s);
        },
        .node_ref => |id| {
            try buf.append(allocator, 5);
            try appendU64(buf, allocator, id);
        },
        .edge_ref => |id| {
            try buf.append(allocator, 6);
            try appendU64(buf, allocator, id);
        },
        .list_val => |list| {
            try buf.append(allocator, 7);
            try appendLen(buf, allocator, list.len);
            for (list) |item| {
                try appendEvalResultKey(buf, allocator, item);
            }
        },
        .vector_val => |v| {
            try buf.append(allocator, 8);
            try appendLen(buf, allocator, v.len);
            try appendBytes(buf, allocator, std.mem.sliceAsBytes(v));
        },
        .map_val => |entries| {
            try buf.append(allocator, 9);
            try appendLen(buf, allocator, entries.len);
            for (entries) |entry| {
                try appendBytes(buf, allocator, entry.key);
                try appendEvalResultKey(buf, allocator, entry.value);
            }
        },
    }
}

fn appendLen(buf: *std.ArrayList(u8), allocator: Allocator, len: usize) !void {
    try appendU64(buf, allocator, @intCast(len));
}

fn appendBytes(buf: *std.ArrayList(u8), allocator: Allocator, bytes: []const u8) !void {
    try appendLen(buf, allocator, bytes.len);
    try buf.appendSlice(allocator, bytes);
}

fn appendU64(buf: *std.ArrayList(u8), allocator: Allocator, value: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}
