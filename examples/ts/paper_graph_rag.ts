#!/usr/bin/env npx ts-node
/**
 * Research Paper Graph RAG — LatticeDB Example
 *
 * Demonstrates why Graph RAG outperforms plain vector RAG by building a
 * citation knowledge graph of 18 real AI/ML papers and querying it with
 * vector search, graph traversal, and full-text search.
 *
 * Usage:
 *     npm run paper-graph-rag
 *     npm run paper-graph-rag -- --ollama
 *     ANTHROPIC_API_KEY=sk-... npm run paper-graph-rag
 */

import { Database, hashEmbed, EmbeddingClient } from "@hajewski/latticedb";
import * as fs from "fs";
import * as path from "path";

// Optional: Anthropic SDK for LLM-powered answers
let Anthropic: any;
try {
  Anthropic = require("@anthropic-ai/sdk").default;
} catch {
  Anthropic = null;
}

// ---------------------------------------------------------------------------
// Load shared data
// ---------------------------------------------------------------------------

interface Paper {
  key: string;
  title: string;
  year: number;
  venue: string;
  author: string;
  topics: string[];
  abstract: string;
}

interface PaperResult {
  nodeId: bigint;
  title: string;
  year: number;
  venue: string;
  abstract: string;
  distance?: number;
  score?: number;
  via?: string;
  paths?: number;
}

const DATA_PATH = path.join(__dirname, "..", "papers.json");
const DATA = JSON.parse(fs.readFileSync(DATA_PATH, "utf-8"));

const PAPERS: Paper[] = DATA.papers;
const CITATIONS: [string, string][] = DATA.citations;
const TOPICS: string[] = DATA.topics;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function printSection(title: string): void {
  console.log(`\n${"=".repeat(70)}`);
  console.log(`  ${title}`);
  console.log(`${"=".repeat(70)}\n`);
}

function printPapers(papers: PaperResult[], prefix = ""): void {
  papers.forEach((p, i) => {
    const extra = p.via ? `  (${p.via})` : "";
    console.log(`  ${prefix}${i + 1}. [${p.year}] ${p.title}${extra}`);
  });
}

function formatContext(papers: PaperResult[]): string {
  return papers
    .map((p) => `- ${p.title} (${p.year}, ${p.venue})\n  ${p.abstract}`)
    .join("\n\n");
}

function findPaper(key: string): Paper | undefined {
  return PAPERS.find((p) => p.key === key);
}

// ---------------------------------------------------------------------------
// Act 0: Build the knowledge graph
// ---------------------------------------------------------------------------

async function createGraph(
  db: Database,
  embedFn: (text: string) => Float32Array
): Promise<{
  papers: number;
  authors: number;
  topics: number;
  citations: number;
  paperIds: Map<string, bigint>;
}> {
  const paperIds = new Map<string, bigint>();
  const authorIds = new Map<string, bigint>();
  const topicIds = new Map<string, bigint>();

  await db.write(async (txn) => {
    // Create topic nodes
    for (const name of TOPICS) {
      const node = await txn.createNode({
        labels: ["Topic"],
        properties: { name },
      });
      topicIds.set(name, node.id);
    }

    // Create author nodes
    for (const paper of PAPERS) {
      if (!authorIds.has(paper.author)) {
        const node = await txn.createNode({
          labels: ["Author"],
          properties: { name: paper.author },
        });
        authorIds.set(paper.author, node.id);
      }
    }

    // Create paper nodes with vectors and FTS
    for (const paper of PAPERS) {
      const node = await txn.createNode({
        labels: ["Paper"],
        properties: {
          title: paper.title,
          year: paper.year,
          venue: paper.venue,
          key: paper.key,
        },
      });
      paperIds.set(paper.key, node.id);

      // Embed and index
      const text = `${paper.title}. ${paper.abstract}`;
      const vec = embedFn(text);
      await txn.setVector(node.id, "embedding", vec);
      await txn.ftsIndex(node.id, text);

      // Author edge
      await txn.createEdge(
        node.id,
        authorIds.get(paper.author)!,
        "AUTHORED_BY"
      );

      // Topic edges
      for (const topicName of paper.topics) {
        await txn.createEdge(node.id, topicIds.get(topicName)!, "ABOUT");
      }
    }

    // Citation edges
    for (const [citingKey, citedKey] of CITATIONS) {
      await txn.createEdge(paperIds.get(citingKey)!, paperIds.get(citedKey)!, "CITES");
    }
  });

  return {
    papers: PAPERS.length,
    authors: authorIds.size,
    topics: TOPICS.length,
    citations: CITATIONS.length,
    paperIds,
  };
}

// ---------------------------------------------------------------------------
// Act 1: Vector search only
// ---------------------------------------------------------------------------

async function vectorSearchOnly(
  db: Database,
  query: string,
  embedFn: (text: string) => Float32Array,
  k = 5
): Promise<PaperResult[]> {
  const queryVec = embedFn(query);
  const results = await db.vectorSearch(queryVec, { k });

  const papers: PaperResult[] = [];
  await db.read(async (txn) => {
    for (const r of results) {
      const title = (await txn.getProperty(r.nodeId, "title")) as string;
      const year = (await txn.getProperty(r.nodeId, "year")) as number;
      const venue = (await txn.getProperty(r.nodeId, "venue")) as string;
      const key = (await txn.getProperty(r.nodeId, "key")) as string;
      const paper = findPaper(key);
      papers.push({
        nodeId: r.nodeId,
        title,
        year,
        venue,
        distance: r.distance,
        abstract: paper?.abstract ?? "",
      });
    }
  });

  return papers;
}

// ---------------------------------------------------------------------------
// Act 2: Graph-enhanced retrieval
// ---------------------------------------------------------------------------

async function graphExpand(
  db: Database,
  seedPapers: PaperResult[]
): Promise<PaperResult[]> {
  const seedIds = new Set(seedPapers.map((p) => p.nodeId));
  const discovered = new Map<bigint, number>(); // nodeId -> path count
  const discoveryMethod = new Map<bigint, string>(); // nodeId -> first method

  function addDiscovery(nodeId: bigint, method: string): void {
    discovered.set(nodeId, (discovered.get(nodeId) ?? 0) + 1);
    if (!discoveryMethod.has(nodeId)) {
      discoveryMethod.set(nodeId, method);
    }
  }

  const expanded: PaperResult[] = [];

  await db.read(async (txn) => {
    for (const seed of seedPapers) {
      const sid = seed.nodeId;
      const shortTitle = seed.title.slice(0, 40) + "...";

      // 1. Papers this seed cites
      const outgoing = await txn.getOutgoingEdges(sid);
      for (const edge of outgoing) {
        if (edge.type === "CITES" && !seedIds.has(edge.targetId)) {
          addDiscovery(edge.targetId, `cited by '${shortTitle}'`);
        }
      }

      // 2. Papers that cite this seed
      const incoming = await txn.getIncomingEdges(sid);
      for (const edge of incoming) {
        if (edge.type === "CITES" && !seedIds.has(edge.sourceId)) {
          addDiscovery(edge.sourceId, `cites '${shortTitle}'`);
        }
      }

      // 3. Other papers by the same author
      for (const edge of outgoing) {
        if (edge.type === "AUTHORED_BY") {
          const authorId = edge.targetId;
          const authorIncoming = await txn.getIncomingEdges(authorId);
          for (const ae of authorIncoming) {
            if (ae.type === "AUTHORED_BY" && !seedIds.has(ae.sourceId)) {
              const authorName = await txn.getProperty(authorId, "name");
              addDiscovery(ae.sourceId, `same author (${authorName})`);
            }
          }
        }
      }
    }

    // Sort by discovery count descending
    const sorted = [...discovered.entries()].sort((a, b) => b[1] - a[1]);

    for (const [nodeId, paths] of sorted) {
      const title = (await txn.getProperty(nodeId, "title")) as string;
      const year = (await txn.getProperty(nodeId, "year")) as number;
      const venue = (await txn.getProperty(nodeId, "venue")) as string;
      const key = (await txn.getProperty(nodeId, "key")) as string;
      const paper = findPaper(key);
      expanded.push({
        nodeId,
        title,
        year,
        venue,
        via: discoveryMethod.get(nodeId)!,
        paths,
        abstract: paper?.abstract ?? "",
      });
    }
  });

  return expanded;
}

// ---------------------------------------------------------------------------
// Act 3: Full-text search
// ---------------------------------------------------------------------------

async function ftsSearchDemo(
  db: Database,
  queryText: string,
  limit = 5
): Promise<PaperResult[]> {
  const results = await db.ftsSearch(queryText, { limit });
  const papers: PaperResult[] = [];

  await db.read(async (txn) => {
    for (const r of results) {
      const title = (await txn.getProperty(r.nodeId, "title")) as string;
      const year = (await txn.getProperty(r.nodeId, "year")) as number;
      const venue = (await txn.getProperty(r.nodeId, "venue")) as string;
      const key = (await txn.getProperty(r.nodeId, "key")) as string;
      const paper = findPaper(key);
      papers.push({
        nodeId: r.nodeId,
        title,
        year,
        venue,
        score: r.score,
        abstract: paper?.abstract ?? "",
      });
    }
  });

  return papers;
}

// ---------------------------------------------------------------------------
// Act 4: Combined Graph RAG pipeline
// ---------------------------------------------------------------------------

async function graphRagPipeline(
  db: Database,
  query: string,
  embedFn: (text: string) => Float32Array,
  k = 5
): Promise<[PaperResult[], PaperResult[]]> {
  // Step 1: Vector search seeds
  const vectorPapers = await vectorSearchOnly(db, query, embedFn, k);

  // Step 2: Graph expansion
  const expanded = await graphExpand(db, vectorPapers);

  // Step 3: FTS boost
  const ftsPapers = await ftsSearchDemo(
    db,
    "knowledge graphs LLM",
    10
  );

  // Step 4: Merge and deduplicate
  const seen = new Set<bigint>();
  const graphRagPapers: PaperResult[] = [];

  for (const p of vectorPapers) {
    if (!seen.has(p.nodeId)) {
      graphRagPapers.push({ ...p, via: "vector search" });
      seen.add(p.nodeId);
    }
  }

  for (const p of expanded) {
    if (!seen.has(p.nodeId)) {
      graphRagPapers.push(p);
      seen.add(p.nodeId);
    }
  }

  for (const p of ftsPapers) {
    if (!seen.has(p.nodeId)) {
      graphRagPapers.push({ ...p, via: "full-text search" });
      seen.add(p.nodeId);
    }
  }

  return [vectorPapers, graphRagPapers];
}

// ---------------------------------------------------------------------------
// Act 5: LLM-powered answer
// ---------------------------------------------------------------------------

async function askLlm(
  question: string,
  context: string
): Promise<string | null> {
  if (!process.env.ANTHROPIC_API_KEY || !Anthropic) return null;

  const client = new Anthropic();
  const response = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 1024,
    system:
      "You are a research assistant. Answer the question based ONLY on " +
      "the provided papers. Cite papers by title when referencing them. " +
      "Be concise but thorough.",
    messages: [
      {
        role: "user" as const,
        content: `Papers:\n${context}\n\nQuestion: ${question}`,
      },
    ],
  });

  const block = response.content[0];
  return block.type === "text" ? block.text : null;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const useOllama = process.argv.includes("--ollama");

  // Choose embedding function
  let embedFn: (text: string) => Float32Array;
  let embedClient: EmbeddingClient | null = null;

  if (useOllama) {
    console.log("Connecting to Ollama for embeddings...");
    embedClient = new EmbeddingClient({ endpoint: "http://localhost:11434" });
    embedFn = (text: string) => embedClient!.embed(text);
  } else {
    embedFn = (text: string) => hashEmbed(text, 128);
  }

  const dbPath = "_paper_graph_rag_demo.db";

  // Clean up any previous run
  for (const suffix of ["", "-wal"]) {
    const p = dbPath + suffix;
    if (fs.existsSync(p)) fs.unlinkSync(p);
  }

  try {
    const db = new Database(dbPath, {
      create: true,
      enableVectors: true,
      vectorDimensions: 128,
    });
    await db.open();

    try {
      // ---- Act 0: Build the graph ----
      printSection("Act 0: Building the Knowledge Graph");
      const stats = await createGraph(db, embedFn);
      console.log(
        `  Created ${stats.papers} papers, ${stats.authors} authors, ` +
          `${stats.topics} topics, ${stats.citations} citations\n`
      );

      const query =
        "How can retrieval improve large language model accuracy?";
      console.log(`  Query: "${query}"\n`);

      // ---- Act 1: Vector search only ----
      printSection("Act 1: Vector Search Only");
      const vectorResults = await vectorSearchOnly(db, query, embedFn);
      printPapers(vectorResults);
      console.log(
        `\n  Vector search found ${vectorResults.length} papers.` +
          "\n  But does it capture the full picture? Let's find out...\n"
      );

      // ---- Act 2: Graph expansion ----
      printSection("Act 2: Graph-Enhanced Retrieval");
      const expanded = await graphExpand(db, vectorResults);
      console.log(
        `  Starting from ${vectorResults.length} seed papers, graph traversal discovered:\n`
      );
      printPapers(expanded);
      if (expanded.length > 0) {
        const top = expanded[0]!;
        console.log(
          `\n  Most connected discovery: '${top.title}'` +
            `\n  Found via: ${top.via} (${top.paths} paths)\n`
        );
      }

      // ---- Act 3: Full-text search ----
      printSection("Act 3: Full-Text Search Precision");
      const ftsResults = await ftsSearchDemo(db, "knowledge graphs");
      console.log('  FTS query: "knowledge graphs"\n');
      if (ftsResults.length > 0) {
        printPapers(ftsResults);
        console.log(
          "\n  FTS adds precision: it found papers that mention " +
            "'knowledge graph' by name,\n  which vector similarity can " +
            "miss when abstracts use different vocabulary.\n"
        );
      } else {
        console.log(
          "  No exact FTS matches (try --ollama for better results).\n"
        );
      }

      // ---- Act 4: Full pipeline ----
      printSection("Act 4: Complete Graph RAG Pipeline");
      const [vectorOnly, graphRag] = await graphRagPipeline(
        db,
        query,
        embedFn
      );

      console.log(`  Vector-only: ${vectorOnly.length} papers`);
      console.log(`  Graph RAG:   ${graphRag.length} papers\n`);

      console.log("  --- Vector RAG context ---");
      printPapers(vectorOnly);
      console.log("\n  --- Graph RAG context (vector + graph + FTS) ---");
      printPapers(graphRag);

      const newCount = graphRag.length - vectorOnly.length;
      console.log(
        `\n  Graph RAG discovered ${newCount} additional papers by ` +
          "traversing citations,\n  author connections, and keyword matches " +
          "— context that vector search alone would miss.\n"
      );

      // ---- Act 5: LLM answers ----
      printSection("Act 5: LLM-Powered Answers (Claude)");

      const vectorContext = formatContext(vectorOnly);
      const graphRagContext = formatContext(graphRag);

      if (!process.env.ANTHROPIC_API_KEY) {
        console.log(
          "  Set ANTHROPIC_API_KEY to see LLM-powered answers.\n" +
            "  Example: ANTHROPIC_API_KEY=sk-... npm run paper-graph-rag\n"
        );
        if (!Anthropic) {
          console.log("  (Also install: npm install @anthropic-ai/sdk)\n");
        }
        console.log("  Here's the prompt that would be sent:\n");
        console.log(`  Question: ${query}`);
        console.log(`  Context papers: ${graphRag.length} papers`);
        console.log(`  Context length: ${graphRagContext.length} chars\n`);
      } else {
        console.log("  Asking Claude with vector-only context...\n");
        const answerVector = await askLlm(query, vectorContext);
        if (answerVector) {
          console.log("  --- Answer from Vector RAG ---");
          for (const line of answerVector.split("\n")) {
            console.log(`  ${line}`);
          }
          console.log();
        }

        console.log("  Asking Claude with Graph RAG context...\n");
        const answerGraph = await askLlm(query, graphRagContext);
        if (answerGraph) {
          console.log("  --- Answer from Graph RAG ---");
          for (const line of answerGraph.split("\n")) {
            console.log(`  ${line}`);
          }
          console.log();
        }

        console.log(
          "  The Graph RAG answer has richer context about foundational works,\n" +
            "  citation relationships, and the full research landscape.\n"
        );
      }

      await db.close();
    } catch (err) {
      await db.close();
      throw err;
    }
  } finally {
    // Cleanup
    for (const suffix of ["", "-wal"]) {
      const p = dbPath + suffix;
      if (fs.existsSync(p)) fs.unlinkSync(p);
    }
    if (embedClient) embedClient.close();
  }

  console.log("Done!");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
