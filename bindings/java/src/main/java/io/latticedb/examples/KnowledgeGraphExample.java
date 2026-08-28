package io.latticedb.examples;

import io.latticedb.Database;
import io.latticedb.Embedding;
import io.latticedb.FTSSearchOptions;
import io.latticedb.Node;
import io.latticedb.OpenOptions;
import io.latticedb.QueryResult;
import io.latticedb.Transaction;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * Complete example: build a small knowledge graph, store embeddings, index
 * text, then query across graph, vector, and full-text search modes.
 *
 * Run from bindings/java:
 * <pre>
 * mvn -q compile exec:java@run-example -Dexec.args=/tmp/knowledge.db
 * </pre>
 */
public final class KnowledgeGraphExample {
    public static void main(String[] args) throws Exception {
        String path = args.length > 0 ? args[0]
                : Files.createTempFile("knowledge", ".db").toString();
        Files.deleteIfExists(java.nio.file.Path.of(path));

        int dims = 8;
        try (Database db = Database.open(path,
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(dims))) {

            // The chunk text is searchable because an index is declared over the
            // property that holds it.
            db.createNodeFtsIndex("Chunk", "text");

            // --- Build the graph ---
            try (Transaction txn = db.beginWrite()) {
                Node alice = txn.createNode(List.of("Person"),
                        Map.of("name", "Alice", "field", "ML"));
                Node bob = txn.createNode(List.of("Person"),
                        Map.of("name", "Bob", "field", "Systems"));

                List<String[]> docs = List.of(
                        new String[]{"Attention Is All You Need",
                                "The transformer architecture uses self-attention layers"},
                        new String[]{"Scaling Laws for LLMs",
                                "Model performance scales predictably with compute"},
                        new String[]{"LSM Trees",
                                "Log-structured merge trees optimize write-heavy workloads"});

                for (String[] doc : docs) {
                    Node document = txn.createNode(List.of("Document"),
                            Map.of("title", doc[0]));
                    Node chunk = txn.createNode(List.of("Chunk"), Map.of("text", doc[1]));

                    txn.setVector(chunk.id(), "embedding",
                            Embedding.hashEmbed(doc[1], dims));

                    txn.createEdge(chunk.id(), document.id(), "PART_OF");
                    txn.createEdge(document.id(),
                            doc[0].startsWith("LSM") ? bob.id() : alice.id(), "AUTHORED_BY");
                }
                txn.commit();
            }

            // --- Query across search modes ---
            QueryResult results = db.query("""
                    MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
                    RETURN doc.title AS title, chunk.text AS text, author.name AS author
                    """);
            for (Map<String, Object> row : results.rows()) {
                System.out.printf("%s by %s%n", row.get("title"), row.get("author"));
            }

            // --- Vector similarity ---
            System.out.println("\nSimilar to 'self-attention':");
            db.vectorSearch(Embedding.hashEmbed("self-attention transformer", dims),
                    io.latticedb.VectorSearchOptions.defaults().k(2))
                .forEach(r -> System.out.printf("  node %d (distance %.4f)%n",
                        r.nodeId(), r.distance()));

            // --- Full-text search ---
            System.out.println("\nFTS 'transformer attention':");
            db.ftsSearch("Chunk", "text", "transformer attention", FTSSearchOptions.defaults())
                .forEach(r -> System.out.printf("  node %d (score %.4f)%n",
                        r.nodeId(), r.score()));
        }
    }

    private KnowledgeGraphExample() {
    }
}
