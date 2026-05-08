//! Reproduction test for ValueTooLarge on 4 KB pages during multi-conversation writes.
//!
//! This test mimics the full Quipu write pattern that triggers the issue:
//! - Session, Turn, and Message nodes
//! - Edges between them (HAS_TURN, HAS_MESSAGE)
//! - FTS indexing on searchable nodes
//! - Vector embeddings on vector-indexed nodes (Message, Fact, Preference, etc.)
//! - Multiple conversation cycles to grow the B-tree
//!
//! Run: zig build test --filter "quipu_longmemeval_repro"

const std = @import("std");
const lattice = @import("lattice");

const c_api = lattice.c_api;
const lattice_database = c_api.lattice_database;
const lattice_txn = c_api.lattice_txn;
const lattice_error = c_api.lattice_error;
const lattice_value = c_api.lattice_value;
const lattice_value_type = c_api.lattice_value_type;
const lattice_open_options_v2 = c_api.lattice_open_options_v2;
const lattice_txn_mode = c_api.lattice_txn_mode;
const lattice_node_id = c_api.lattice_node_id;

const TEST_DB_PATH = "/tmp/lattice_quipu_repro_test.db";

// Real messages from longmemeval_oracle.jsonl conversation gpt4_2655b836, session 0.
// These are the actual sizes that trigger ValueTooLarge after accumulated writes.
const MESSAGES = [_]struct { role: []const u8, content: []const u8 }{
    .{ .role = "user", .content = "I'm thinking of getting my car detailed soon. Do you know any good detailers in the area or have any recommendations? By the way, I just got my car serviced for the first time on March 15th, and it was a great experience." },
    .{ .role = "assistant", .content = "I'm happy to help you with your question! However, I'm a large language model, I don't have personal experiences or knowledge of specific locations or businesses. I can provide some general advice and suggestions on how to find a good detailer in your area.\n\nTo find a reputable and reliable detailer, you can try the following:\n\n1. Online Reviews: Check review websites such as Yelp, Google Reviews, or Facebook Reviews to see what other customers have to say about their experiences with local detailers.\n2. Ask for Referrals: Ask friends, family, or coworkers who have had their cars detailed recently for recommendations.\n3. Check Certifications: Look for detailers who are certified by organizations such as the International Detailing Association (IDA) or the National Institute of Automotive Service Excellence (ASE).\n4. Check Their Work: Ask to see before-and-after photos of their work or visit their shop to see the quality of their work firsthand.\n5. Get Quotes: Get quotes from several detailers and compare prices and services offered.\n\nAs for your car service experience on March 15th, I'm glad to hear it was a great experience! It's always important to keep your car well-maintained, and it sounds like you're taking good care of your vehicle.\n\nRemember to always do your research and choose a reputable detailer to ensure your car receives the best care possible. Good luck in your search!" },
    .{ .role = "user", .content = "I'll definitely check out those review websites and ask around for referrals. Do you think it's a good idea to get a wax and detailing done every 3-4 months to maintain the car's appearance and protect the paint?" },
    .{ .role = "assistant", .content = "Regular waxing and detailing can definitely help maintain your car's appearance and protect the paint. The frequency of waxing and detailing depends on several factors, including:\n\n1. Climate: If you live in an area with harsh weather conditions, such as heavy rain, snow, or intense sunlight, you may need to wax and detail more frequently (every 2-3 months).\n2. Driving habits: If you drive frequently, park outdoors, or drive on rough roads, you may need to wax and detail more often to remove dirt, grime, and contaminants that can damage the paint.\n3. Paint type: Some paint finishes, like clear coat or ceramic coat, may require more frequent waxing and detailing to maintain their appearance and protection.\n\nIn general, waxing every 3-4 months is a good rule of thumb for most cars. This frequency can help:\n\n1. Protect the paint from UV rays, oxidation, and fading.\n2. Remove dirt, grime, and contaminants that can scratch or damage the paint.\n3. Maintain a glossy, showroom-like appearance.\n4. Make future washing and maintenance easier.\n\nHowever, if you want to take your car's appearance to the next level, you may consider a more frequent detailing schedule, such as:\n\n* Washing every 1-2 weeks to remove dirt and grime.\n* Waxing every 2-3 months to provide an extra layer of protection.\n* Detailing every 3-4 months to deep clean the interior and exterior.\n\nRemember to also consider your car's specific needs, such as any special paint finishes or requirements.\n\nIt's great that you're thinking about maintaining your car's appearance and protection. Regular waxing and detailing can definitely help extend the life of your car's paint and keep it looking its best!" },
    .{ .role = "user", .content = "Yup, that makes sense. I've been keeping track of my gas mileage too, and I've been getting around 32 miles per gallon, which is better than my old car." },
    .{ .role = "assistant", .content = "That's great to hear! Keeping track of your gas mileage is a great way to monitor your car's performance and make adjustments to your driving habits or maintenance routine as needed.\n\nA 32 miles per gallon average is a good starting point, and it's even more impressive considering it's an improvement over your old car. You might want to consider tracking other metrics, such as:\n\n1. Fuel efficiency over time: See if your mileage improves or decreases over time, which can indicate potential issues with your car's engine, tires, or driving habits.\n2. Fuel efficiency in different conditions: Compare your mileage in city driving, highway driving, or in different weather conditions to see how they affect your car's performance.\n3. Maintenance and repair costs: Keep track of how much you're spending on maintenance, repairs, and replacement parts to get a better understanding of your car's overall cost of ownership.\n\nBy tracking these metrics, you'll be able to make data-driven decisions about your car's maintenance, optimize your driving habits, and potentially even save money on fuel costs.\n\nRemember, it's always a good idea to consult your car's owner's manual for recommended maintenance schedules, fuel efficiency tips, and other important information specific to your vehicle.\n\nKeep up the good work, and happy tracking!" },
    .{ .role = "user", .content = "I've also been using the rewards program at the Shell gas station near my office to earn points. Do you think it's worth it to focus on earning points at one gas station or to try to earn points across multiple stations?" },
    .{ .role = "assistant", .content = "That's a great question! Rewards programs can be a fantastic way to save money or earn perks on your gas purchases.\n\nThe strategy you choose depends on your personal preferences, driving habits, and the specific rewards programs offered by different gas stations. Here are some pros and cons of each approach to consider:\n\n**Focusing on one gas station:**\n\nPros:\n\n* You can accumulate points faster and reach rewards thresholds more quickly.\n* You might receive more targeted offers and promotions since you're a loyal customer.\n* It's easier to track your points and redeem rewards.\n\nCons:\n\n* You might be limited to a specific station or location, which could be inconvenient if you travel or have multiple cars.\n* You might miss out on better deals or rewards offered by other stations.\n\n**Earning points across multiple stations:**\n\nPros:\n\n* You can take advantage of better prices or promotions at different stations.\n* You're not tied to a single station, giving you more flexibility when traveling or filling up.\n* You can diversify your rewards earning potential across multiple programs.\n\nCons:\n\n* You might spread yourself too thin, making it harder to accumulate points quickly.\n* You'll need to keep track of multiple programs, which can be more complex.\n\nConsider the following questions to help you decide:\n\n1. Do you frequent a specific gas station regularly, making it convenient to focus on earning points there?\n2. Are the rewards offered by a particular station more valuable to you than others?\n3. Do you travel frequently or have multiple cars, making it beneficial to earn points across multiple stations?\n4. Are you willing to keep track of multiple rewards programs and adapt to changes in their terms and conditions?\n\nUltimately, you can experiment with both approaches and see what works best for your lifestyle and preferences. You might find that focusing on one station yields better rewards, or that earning points across multiple stations provides more flexibility and savings.\n\nRemember to always review the terms and conditions of each rewards program to ensure you understand how points are earned, redeemed, and expire. Happy rewards earning!" },
    .{ .role = "user", .content = "I've also been redeeming points from my credit card to get rewards. I recently redeemed 50,000 points to get a $500 gift card to a car accessories store. I used it to purchase a new car cover, floor mats, and a steering wheel cover, which I'm really happy with." },
    .{ .role = "assistant", .content = "That's awesome! It sounds like you're making the most of your credit card rewards program!\n\nRedeeming 50,000 points for a $500 gift card is a great value, and it's even better that you were able to use it to get some practical and useful items for your car. A new car cover, floor mats, and steering wheel cover can really enhance the overall appearance and comfort of your vehicle.\n\nIt's great that you're thinking strategically about your rewards redemptions and using them to get something that brings you joy and value. Credit card rewards can be a fantastic way to offset the cost of everyday expenses or splurge on something special.\n\nDo you have a favorite way to earn or redeem credit card points, or any tips for maximizing your rewards earnings?\n\nAlso, how do you like your new car accessories? Are there any other car-related purchases or upgrades on your wishlist?" },
    .{ .role = "user", .content = "I'm really happy with my new car accessories, and they've added a nice touch to my car. I've been thinking about getting a car wax and detailing done soon, and I was wondering if you could help me find a good detailer in my area." },
    .{ .role = "assistant", .content = "I'm glad to hear that you're happy with your new car accessories!\n\nAs for finding a good detailer in your area, I'd be happy to help. However, I'm a large language model, I don't have direct access to your location or personal information. But I can provide you with some general tips on how to find a reputable and reliable detailer in your area:\n\n1. Online Reviews: Check review websites such as Yelp, Google Reviews, or Facebook Reviews to see what other customers have to say about their experiences with local detailers.\n2. Ask for Referrals: Ask friends, family, or coworkers who have had their cars detailed recently for recommendations.\n3. Check Certifications: Look for detailers who are certified by organizations such as the International Detailing Association (IDA) or the National Institute of Automotive Service Excellence (ASE).\n4. Check Their Work: Ask to see before-and-after photos of their work or visit their shop to see the quality of their work firsthand.\n5. Get Quotes: Get quotes from several detailers and compare prices and services offered.\n\nBy following these steps, you should be able to find a reputable and reliable detailer in your area who can help you get your car looking its best.\n\nRemember to always do your research and choose a detailer who has a good reputation, provides quality work, and offers competitive pricing. Good luck in your search!" },
};

fn cleanupDb(path: []const u8) void {
    @import("compat").fs.cwd().deleteFile(path) catch {};
    var wal_path_buf: [256]u8 = undefined;
    if (std.fmt.bufPrint(&wal_path_buf, "{s}-wal", .{path})) |wal_path| {
        @import("compat").fs.cwd().deleteFile(wal_path) catch {};
    } else |_| {}
}

fn stringValue(text: []const u8) lattice_value {
    return .{
        .value_type = .string,
        .data = .{ .string_val = .{ .ptr = text.ptr, .len = text.len } },
    };
}

fn hashEmbed(text: []const u8, buf: []f32) void {
    var h: u64 = 0xCBF29CE484222325;
    for (text) |ch| {
        h ^= ch;
        h *%= 0x100000001B3;
    }
    for (buf, 0..) |*item, i| {
        const idx: u64 = @intCast(i + 1);
        h ^= idx *% 0x9E3779B97F4A7C15;
        h *%= 0x100000001B3;
        item.* = @floatFromInt(h & 0x7FFFFFFF);
        item.* = item.* / @as(f32, 2147483647.0);
    }
}

fn createNode(txn: ?*lattice_txn, label: []const u8, qid: []const u8, props: []const u8, node_id_out: *lattice_node_id) !void {
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_create(txn, label.ptr, node_id_out));
    const nid = node_id_out.*;

    const qid_val = stringValue(qid);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, nid, "qid", &qid_val));

    const label_val = stringValue(label);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, nid, "quipuLabel", &label_val));

    const props_val = stringValue(props);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, nid, "propertiesJson", &props_val));

    const kind_val = stringValue("node");
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_property(txn, nid, "recordKind", &kind_val));

    // FTS index (skip internal labels)
    if (!std.mem.eql(u8, label, "Job") and
        !std.mem.eql(u8, label, "Schema") and
        !std.mem.eql(u8, label, "Migration") and
        !std.mem.eql(u8, label, "Idempotency"))
    {
        const searchable = if (props.len > 2048) props[0..2048] else props;
        var fts_buf: [4096]u8 = undefined;
        const fts_text = try std.fmt.bufPrint(&fts_buf, "quipunodeall {s} {s} {s}", .{ qid, label, searchable });
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_fts_index(txn, nid, fts_text.ptr, fts_text.len));
    }

    // Vector embedding (Message, MemoryCard, Fact, Preference, Procedure, Core)
    if (std.mem.eql(u8, label, "Message") or
        std.mem.eql(u8, label, "MemoryCard") or
        std.mem.eql(u8, label, "Fact") or
        std.mem.eql(u8, label, "Preference") or
        std.mem.eql(u8, label, "Procedure") or
        std.mem.eql(u8, label, "Core"))
    {
        var vec_text_buf: [4096]u8 = undefined;
        const vec_text = if (props.len > 2048)
            try std.fmt.bufPrint(&vec_text_buf, "{s} {s}", .{ label, props[0..2048] })
        else
            try std.fmt.bufPrint(&vec_text_buf, "{s} {s}", .{ label, props });

        var vec_buf: [128]f32 = undefined;
        hashEmbed(vec_text, &vec_buf);
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_node_set_vector(txn, nid, "embedding", &vec_buf, vec_buf.len));
    }
}

fn setEdgeProps(
    txn: ?*lattice_txn,
    edge_id: c_api.lattice_edge_id,
    qid: []const u8,
    from_qid: []const u8,
    to_qid: []const u8,
    edge_type: []const u8,
    props: []const u8,
) !void {
    const qid_val = stringValue(qid);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "qid", &qid_val));

    const from_val = stringValue(from_qid);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "fromQid", &from_val));

    const to_val = stringValue(to_qid);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "toQid", &to_val));

    const type_val = stringValue(edge_type);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "edgeType", &type_val));

    const props_val = stringValue(props);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "propertiesJson", &props_val));

    const kind_val = stringValue("edge");
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_set_property(txn, edge_id, "recordKind", &kind_val));
}

fn writeConversation(db: ?*lattice_database, conv_idx: usize) !usize {
    const allocator = std.testing.allocator;

    // Session node
    var txn: ?*lattice_txn = null;
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
    errdefer _ = c_api.lattice_rollback(txn);

    var sess_nid: lattice_node_id = 0;
    const sess_qid = try std.fmt.allocPrint(allocator, "sess-{d:0>3}", .{conv_idx});
    defer allocator.free(sess_qid);

    const sess_props = try std.fmt.allocPrint(allocator,
        \\{{"kind":"session","sessionId":null,"tenantId":null,"userId":null,"agentId":null,"projectId":null,"deleted":false}}
    , .{});
    defer allocator.free(sess_props);

    try createNode(txn, "Session", sess_qid, sess_props, &sess_nid);
    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));

    // Message nodes
    var ok_count: usize = 0;
    for (MESSAGES, 0..) |msg, msg_idx| {
        // Turn node + edge
        txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        errdefer _ = c_api.lattice_rollback(txn);

        var turn_nid: lattice_node_id = 0;
        const turn_qid = try std.fmt.allocPrint(allocator, "turn-{d:0>3}-{d:0>3}", .{ conv_idx, msg_idx });
        defer allocator.free(turn_qid);

        const turn_props = try std.fmt.allocPrint(allocator,
            \\{{"kind":"turn","sessionQid":"{s}","tenantId":null,"userId":null,"agentId":null,"projectId":null,"deleted":false}}
        , .{sess_qid});
        defer allocator.free(turn_props);

        try createNode(txn, "Turn", turn_qid, turn_props, &turn_nid);

        // Edge: Session -> Turn
        var edge_id: c_api.lattice_edge_id = 0;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, sess_nid, turn_nid, "HAS_TURN", &edge_id));
        const turn_edge_qid = try std.fmt.allocPrint(allocator, "edge-session-turn-{d:0>3}-{d:0>3}", .{ conv_idx, msg_idx });
        defer allocator.free(turn_edge_qid);
        var turn_edge_props: [8192]u8 = undefined;
        @memset(&turn_edge_props, 't');
        try setEdgeProps(txn, edge_id, turn_edge_qid, sess_qid, turn_qid, "HAS_TURN", &turn_edge_props);
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_commit(txn));

        // Message node + edge
        txn = null;
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_begin(db, .read_write, &txn));
        errdefer _ = c_api.lattice_rollback(txn);

        var msg_nid: lattice_node_id = 0;
        const msg_qid = try std.fmt.allocPrint(allocator, "msg-{d:0>3}-{d:0>3}", .{ conv_idx, msg_idx });
        defer allocator.free(msg_qid);

        const msg_props = try std.fmt.allocPrint(allocator,
            \\{{"kind":"message","role":"{s}","content":"{s}","createdAt":"2024-01-15T10:00:00Z","tenantId":null,"userId":null,"agentId":null,"projectId":null,"privacyClass":"normal","deleted":false}}
        , .{ msg.role, msg.content });
        defer allocator.free(msg_props);

        try createNode(txn, "Message", msg_qid, msg_props, &msg_nid);

        // Edge: Turn -> Message
        try std.testing.expectEqual(lattice_error.ok, c_api.lattice_edge_create(txn, turn_nid, msg_nid, "HAS_MESSAGE", &edge_id));
        const message_edge_qid = try std.fmt.allocPrint(allocator, "edge-turn-message-{d:0>3}-{d:0>3}", .{ conv_idx, msg_idx });
        defer allocator.free(message_edge_qid);
        var message_edge_props: [8192]u8 = undefined;
        @memset(&message_edge_props, 'm');
        try setEdgeProps(txn, edge_id, message_edge_qid, turn_qid, msg_qid, "HAS_MESSAGE", &message_edge_props);

        const commit_rc = c_api.lattice_commit(txn);
        if (commit_rc != lattice_error.ok) {
            std.debug.print("FAIL: conv={d} msg={d} role={s} content_len={d} props_len={d} rc={s}\n", .{
                conv_idx, msg_idx, msg.role, msg.content.len, msg_props.len, @tagName(commit_rc),
            });
            return error.ValueTooLarge;
        }

        ok_count += 1;
    }

    return ok_count;
}

fn runTest(page_size: u32, conversation_count: usize) !struct { ok: usize, fail: usize, first_fail_conv: ?usize } {
    cleanupDb(TEST_DB_PATH);

    var db: ?*lattice_database = null;
    const options = lattice_open_options_v2{
        .struct_size = @sizeOf(lattice_open_options_v2),
        .create = true,
        .read_only = false,
        .cache_size_mb = 100,
        .page_size = page_size,
        .enable_vector = true,
        .vector_dimensions = 128,
        .enable_wal = true,
    };

    try std.testing.expectEqual(lattice_error.ok, c_api.lattice_open_v2(TEST_DB_PATH, &options, &db));
    defer {
        _ = c_api.lattice_close(db);
        cleanupDb(TEST_DB_PATH);
    }

    var total_ok: usize = 0;
    var first_fail: ?usize = null;
    var conv_idx: usize = 0;
    while (conv_idx < conversation_count) : (conv_idx += 1) {
        const count = writeConversation(db, conv_idx) catch |err| {
            if (err == error.ValueTooLarge and first_fail == null) {
                first_fail = conv_idx;
            }
            break;
        };
        total_ok += count;
        std.debug.print("  conv[{d}] OK ({d} messages)\n", .{ conv_idx, count });
    }

    const fail_count = (conversation_count * MESSAGES.len) - total_ok;
    return .{ .ok = total_ok, .fail = fail_count, .first_fail_conv = first_fail };
}

// ============================================================================
// Main test entry point
// ============================================================================

test "quipu_longmemeval_repro: 4KB and 32KB pages pass" {
    const conversation_count = 20;
    const total_messages = conversation_count * MESSAGES.len;

    std.debug.print("\n=== Test with 4 KB pages ({d} conversations) ===\n", .{conversation_count});
    const r4 = try runTest(4096, conversation_count);
    std.debug.print("4KB:  {d}/{d} messages OK\n", .{ r4.ok, total_messages });

    std.debug.print("\n=== Test with 32 KB pages ({d} conversations) ===\n", .{conversation_count});
    const r32 = try runTest(32768, conversation_count);
    std.debug.print("32KB: {d}/{d} messages OK\n", .{ r32.ok, total_messages });

    std.debug.print("\n=== Summary ===\n", .{});
    std.debug.print("4KB  pages: {s} (first fail conv={?d})\n", .{
        if (r4.fail == 0) "PASS" else "FAIL",
        r4.first_fail_conv,
    });
    std.debug.print("32KB pages: {s}\n", .{if (r32.fail == 0) "PASS" else "FAIL"});

    try std.testing.expectEqual(@as(usize, 0), r4.fail);
    try std.testing.expectEqual(@as(usize, 0), r32.fail);
}
