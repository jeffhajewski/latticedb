//! HTTP-based Embedding Client
//!
//! Generates embeddings by calling external HTTP endpoints.
//! Supports common embedding APIs (Ollama, OpenAI-compatible).
//!
//! Disabled by default - must be explicitly configured and instantiated.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Supported embedding API formats
pub const ApiFormat = enum {
    /// Ollama API: POST /api/embeddings {"model": "...", "prompt": "..."}
    ollama,
    /// OpenAI-compatible: POST /v1/embeddings {"model": "...", "input": "..."}
    openai,
};

/// Configuration for the embedding client
pub const Config = struct {
    /// HTTP endpoint URL (e.g., "http://localhost:11434/api/embeddings")
    endpoint: []const u8,

    /// Model name to use (e.g., "nomic-embed-text", "text-embedding-3-small")
    model: []const u8 = "nomic-embed-text",

    /// API format to use for request/response
    api_format: ApiFormat = .ollama,

    /// Optional API key for authenticated endpoints
    api_key: ?[]const u8 = null,

    /// Request timeout in milliseconds
    timeout_ms: u32 = 30_000,
};

/// Errors that can occur during embedding generation
pub const EmbeddingError = error{
    /// Failed to connect to the endpoint
    ConnectionFailed,
    /// HTTP request failed
    RequestFailed,
    /// Invalid response from server
    InvalidResponse,
    /// Server returned an error status
    ServerError,
    /// Failed to parse embedding from response
    ParseError,
    /// Endpoint not configured
    NotConfigured,
    /// Out of memory
    OutOfMemory,
    /// Invalid URI
    InvalidUri,
};

/// HTTP-based embedding client
pub const EmbeddingClient = struct {
    const Self = @This();

    allocator: Allocator,
    config: Config,
    http_client: std.http.Client,

    /// Initialize an embedding client with the given configuration
    pub fn init(allocator: Allocator, config: Config) Self {
        return Self{
            .allocator = allocator,
            .config = config,
            .http_client = std.http.Client{ .allocator = allocator },
        };
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        self.http_client.deinit();
    }

    /// Generate an embedding vector for the given text
    /// Caller owns the returned slice and must free it with the same allocator
    pub fn embed(self: *Self, text: []const u8) EmbeddingError![]f32 {
        // Build request body based on API format
        const body = self.buildRequestBody(text) catch return EmbeddingError.OutOfMemory;
        defer self.allocator.free(body);

        // Parse URI
        const uri = std.Uri.parse(self.config.endpoint) catch return EmbeddingError.InvalidUri;

        // Prepare headers
        var headers = std.http.Client.Request.Headers{};
        headers.content_type = .{ .override = "application/json" };

        // Build extra headers for auth
        const auth_header_value = if (self.config.api_key) |key|
            self.formatAuthHeader(key) catch return EmbeddingError.OutOfMemory
        else
            null;
        defer if (auth_header_value) |v| self.allocator.free(v);

        const auth_headers = if (auth_header_value) |v|
            @as([]const std.http.Header, &[_]std.http.Header{
                .{ .name = "Authorization", .value = v },
            })
        else
            @as([]const std.http.Header, &[_]std.http.Header{});

        // Make HTTP request
        var req = std.http.Client.request(&self.http_client, .POST, uri, .{
            .headers = headers,
            .extra_headers = auth_headers,
        }) catch return EmbeddingError.ConnectionFailed;
        defer req.deinit();

        // Send request body
        req.transfer_encoding = .{ .content_length = body.len };
        var body_writer = req.sendBodyUnflushed(&.{}) catch return EmbeddingError.RequestFailed;
        body_writer.writer.writeAll(body) catch return EmbeddingError.RequestFailed;
        body_writer.end() catch return EmbeddingError.RequestFailed;
        (req.connection orelse return EmbeddingError.RequestFailed).flush() catch return EmbeddingError.RequestFailed;

        // Receive response head
        var redirect_buf: [8192]u8 = undefined;
        var response = req.receiveHead(&redirect_buf) catch return EmbeddingError.RequestFailed;

        // Check status
        if (response.head.status != .ok) {
            return EmbeddingError.ServerError;
        }

        // Read response body
        var reader = response.reader(&.{});
        const response_body = reader.allocRemaining(self.allocator, std.Io.Limit.limited(10 * 1024 * 1024)) catch return EmbeddingError.OutOfMemory;
        defer self.allocator.free(response_body);

        // Parse embedding from response
        return self.parseEmbedding(response_body);
    }

    /// Build the JSON request body based on API format
    fn buildRequestBody(self: *Self, text: []const u8) ![]u8 {
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        const writer = buffer.writer();

        switch (self.config.api_format) {
            .ollama => {
                try writer.writeAll("{\"model\":\"");
                try writeJsonEscaped(writer, self.config.model);
                try writer.writeAll("\",\"prompt\":\"");
                try writeJsonEscaped(writer, text);
                try writer.writeAll("\"}");
            },
            .openai => {
                try writer.writeAll("{\"model\":\"");
                try writeJsonEscaped(writer, self.config.model);
                try writer.writeAll("\",\"input\":\"");
                try writeJsonEscaped(writer, text);
                try writer.writeAll("\"}");
            },
        }

        return buffer.toOwnedSlice();
    }

    /// Format authorization header value
    fn formatAuthHeader(self: *Self, key: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.allocator, "Bearer {s}", .{key});
    }

    /// Parse embedding vector from JSON response
    fn parseEmbedding(self: *Self, response: []const u8) EmbeddingError![]f32 {
        // Parse JSON
        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, response, .{}) catch {
            return EmbeddingError.ParseError;
        };
        defer parsed.deinit();

        const root = parsed.value;

        // Extract embedding array based on API format
        const embedding_array = switch (self.config.api_format) {
            .ollama => blk: {
                // Ollama: {"embedding": [0.1, 0.2, ...]}
                if (root.object.get("embedding")) |emb| {
                    if (emb == .array) break :blk emb.array;
                }
                return EmbeddingError.ParseError;
            },
            .openai => blk: {
                // OpenAI: {"data": [{"embedding": [0.1, 0.2, ...]}]}
                if (root.object.get("data")) |data| {
                    if (data == .array and data.array.items.len > 0) {
                        if (data.array.items[0].object.get("embedding")) |emb| {
                            if (emb == .array) break :blk emb.array;
                        }
                    }
                }
                return EmbeddingError.ParseError;
            },
        };

        // Convert to f32 slice
        const result = self.allocator.alloc(f32, embedding_array.items.len) catch {
            return EmbeddingError.OutOfMemory;
        };
        errdefer self.allocator.free(result);

        for (embedding_array.items, 0..) |item, i| {
            result[i] = switch (item) {
                .float => @floatCast(item.float),
                .integer => @floatFromInt(item.integer),
                else => return EmbeddingError.ParseError,
            };
        }

        return result;
    }
};

/// Write a string with JSON escaping
fn writeJsonEscaped(writer: anytype, str: []const u8) !void {
    for (str) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            // Other control characters (excluding \t=0x09, \n=0x0a, \r=0x0d which are handled above)
            0x00...0x08, 0x0b, 0x0c, 0x0e...0x1f => try writer.print("\\u{x:0>4}", .{c}),
            else => try writer.writeByte(c),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

test "config defaults" {
    const config = Config{
        .endpoint = "http://localhost:11434/api/embeddings",
    };

    try std.testing.expectEqualStrings("nomic-embed-text", config.model);
    try std.testing.expectEqual(ApiFormat.ollama, config.api_format);
    try std.testing.expectEqual(@as(?[]const u8, null), config.api_key);
    try std.testing.expectEqual(@as(u32, 30_000), config.timeout_ms);
}

test "json escaping" {
    const allocator = std.testing.allocator;

    var buffer = std.array_list.Managed(u8).init(allocator);
    defer buffer.deinit();

    try writeJsonEscaped(buffer.writer(), "hello \"world\"\ntest\\path");
    try std.testing.expectEqualStrings("hello \\\"world\\\"\\ntest\\\\path", buffer.items);
}

test "build request body ollama" {
    const allocator = std.testing.allocator;

    var client = EmbeddingClient.init(allocator, .{
        .endpoint = "http://localhost:11434/api/embeddings",
        .model = "nomic-embed-text",
        .api_format = .ollama,
    });
    defer client.deinit();

    const body = try client.buildRequestBody("hello world");
    defer allocator.free(body);

    try std.testing.expectEqualStrings(
        "{\"model\":\"nomic-embed-text\",\"prompt\":\"hello world\"}",
        body,
    );
}

test "build request body openai" {
    const allocator = std.testing.allocator;

    var client = EmbeddingClient.init(allocator, .{
        .endpoint = "https://api.openai.com/v1/embeddings",
        .model = "text-embedding-3-small",
        .api_format = .openai,
    });
    defer client.deinit();

    const body = try client.buildRequestBody("hello world");
    defer allocator.free(body);

    try std.testing.expectEqualStrings(
        "{\"model\":\"text-embedding-3-small\",\"input\":\"hello world\"}",
        body,
    );
}

test "parse embedding ollama format" {
    const allocator = std.testing.allocator;

    var client = EmbeddingClient.init(allocator, .{
        .endpoint = "http://localhost:11434/api/embeddings",
        .api_format = .ollama,
    });
    defer client.deinit();

    const response =
        \\{"embedding": [0.1, 0.2, 0.3, -0.5]}
    ;

    const embedding = try client.parseEmbedding(response);
    defer allocator.free(embedding);

    try std.testing.expectEqual(@as(usize, 4), embedding.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), embedding[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2), embedding[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), embedding[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), embedding[3], 0.0001);
}

test "parse embedding openai format" {
    const allocator = std.testing.allocator;

    var client = EmbeddingClient.init(allocator, .{
        .endpoint = "https://api.openai.com/v1/embeddings",
        .api_format = .openai,
    });
    defer client.deinit();

    const response =
        \\{"data": [{"embedding": [0.1, 0.2, 0.3], "index": 0}], "model": "text-embedding-3-small"}
    ;

    const embedding = try client.parseEmbedding(response);
    defer allocator.free(embedding);

    try std.testing.expectEqual(@as(usize, 3), embedding.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), embedding[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.2), embedding[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.3), embedding[2], 0.0001);
}
