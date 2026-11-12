//! The built-in procedures `CALL` can run, by name, with the columns of
//! their rows. Kept apart from the operator that runs them so that the
//! parser can know the columns `YIELD *` brings into scope.

const std = @import("std");

pub const Procedure = enum {
    labels,
    relationship_types,
    property_keys,
    indexes,

    pub const all = [_]Procedure{ .labels, .relationship_types, .property_keys, .indexes };

    /// The name it is called by.
    pub fn name(self: Procedure) []const u8 {
        return switch (self) {
            .labels => "db.labels",
            .relationship_types => "db.relationshipTypes",
            .property_keys => "db.propertyKeys",
            .indexes => "db.indexes",
        };
    }

    /// The columns of its rows, in order.
    pub fn columns(self: Procedure) []const []const u8 {
        return switch (self) {
            .labels => &.{"label"},
            .relationship_types => &.{"relationshipType"},
            .property_keys => &.{"propertyKey"},
            .indexes => &.{ "type", "entityType", "labelOrType", "property" },
        };
    }

    /// The procedure called `text`; names compare without case, as in Neo4j.
    pub fn find(text: []const u8) ?Procedure {
        for (all) |p| {
            if (std.ascii.eqlIgnoreCase(p.name(), text)) return p;
        }
        return null;
    }
};
