"""
Research Paper Graph RAG — LatticeDB Example

Demonstrates why Graph RAG outperforms plain vector RAG by building a
citation knowledge graph of 18 real AI/ML papers and querying it with
vector search, graph traversal, and full-text search.

Usage:
    python paper_graph_rag.py                        # hash embeddings (no deps)
    python paper_graph_rag.py --ollama               # real embeddings via Ollama
    ANTHROPIC_API_KEY=sk-... python paper_graph_rag.py   # full LLM demo
"""

import argparse
import json
import os
from collections import Counter
from pathlib import Path

from latticedb import Database
from latticedb.embedding import hash_embed

try:
    from anthropic import Anthropic

    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

# ---------------------------------------------------------------------------
# Load shared data
# ---------------------------------------------------------------------------

_DATA_PATH = Path(__file__).resolve().parent.parent / "papers.json"
with open(_DATA_PATH) as f:
    _DATA = json.load(f)

PAPERS: list[dict] = _DATA["papers"]
CITATIONS: list[list[str]] = _DATA["citations"]
TOPICS: list[str] = _DATA["topics"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def print_section(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}\n")


def print_papers(papers: list[dict], prefix: str = "") -> None:
    for i, p in enumerate(papers, 1):
        extra = f"  ({p['via']})" if "via" in p else ""
        print(f"  {prefix}{i}. [{p['year']}] {p['title']}{extra}")


def format_context(papers: list[dict]) -> str:
    lines = []
    for p in papers:
        lines.append(f"- {p['title']} ({p['year']}, {p['venue']})")
        lines.append(f"  {p['abstract']}")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Act 0: Build the knowledge graph
# ---------------------------------------------------------------------------


def create_graph(db: Database, embed_fn) -> dict:
    paper_by_key = {p["key"]: p for p in PAPERS}
    paper_ids = {}  # key -> node_id
    author_ids = {}  # name -> node_id
    topic_ids = {}  # name -> node_id

    with db.write() as txn:
        # Create topic nodes
        for name in TOPICS:
            node = txn.create_node(labels=["Topic"], properties={"name": name})
            topic_ids[name] = node.id

        # Create author nodes
        for paper in PAPERS:
            name = paper["author"]
            if name not in author_ids:
                node = txn.create_node(
                    labels=["Author"], properties={"name": name}
                )
                author_ids[name] = node.id

        # Create paper nodes with vectors and FTS
        for paper in PAPERS:
            node = txn.create_node(
                labels=["Paper"],
                properties={
                    "title": paper["title"],
                    "year": paper["year"],
                    "venue": paper["venue"],
                    "key": paper["key"],
                },
            )
            paper_ids[paper["key"]] = node.id

            # Embed and index
            text = f"{paper['title']}. {paper['abstract']}"
            vec = embed_fn(text)
            txn.set_vector(node.id, "embedding", vec)
            txn.fts_index(node.id, text)

            # Author edge
            txn.create_edge(node.id, author_ids[paper["author"]], "AUTHORED_BY")

            # Topic edges
            for topic_name in paper["topics"]:
                txn.create_edge(node.id, topic_ids[topic_name], "ABOUT")

        # Citation edges
        for citing_key, cited_key in CITATIONS:
            txn.create_edge(paper_ids[citing_key], paper_ids[cited_key], "CITES")

        txn.commit()

    return {
        "papers": len(PAPERS),
        "authors": len(author_ids),
        "topics": len(TOPICS),
        "citations": len(CITATIONS),
        "paper_ids": paper_ids,
    }


# ---------------------------------------------------------------------------
# Act 1: Vector search only
# ---------------------------------------------------------------------------


def vector_search_only(db: Database, query: str, embed_fn, k: int = 5) -> list[dict]:
    query_vec = embed_fn(query)
    results = db.vector_search(query_vec, k=k)

    papers = []
    with db.read() as txn:
        for r in results:
            title = txn.get_property(r.node_id, "title")
            year = txn.get_property(r.node_id, "year")
            venue = txn.get_property(r.node_id, "venue")
            abstract = txn.get_property(r.node_id, "key")
            paper = next((p for p in PAPERS if p["key"] == abstract), None)
            papers.append(
                {
                    "node_id": r.node_id,
                    "title": title,
                    "year": year,
                    "venue": venue,
                    "distance": r.distance,
                    "abstract": paper["abstract"] if paper else "",
                }
            )

    return papers


# ---------------------------------------------------------------------------
# Act 2: Graph-enhanced retrieval
# ---------------------------------------------------------------------------


def graph_expand(db: Database, seed_papers: list[dict]) -> list[dict]:
    seed_ids = {p["node_id"] for p in seed_papers}
    discovered = Counter()  # node_id -> count of discovery paths
    discovery_method = {}  # node_id -> first method that found it

    with db.read() as txn:
        for seed in seed_papers:
            sid = seed["node_id"]

            # 1. Papers this seed cites
            for edge in txn.get_outgoing_edges(sid):
                if edge.edge_type == "CITES" and edge.target_id not in seed_ids:
                    discovered[edge.target_id] += 1
                    if edge.target_id not in discovery_method:
                        discovery_method[edge.target_id] = f"cited by '{seed['title'][:40]}...'"

            # 2. Papers that cite this seed
            for edge in txn.get_incoming_edges(sid):
                if edge.edge_type == "CITES" and edge.source_id not in seed_ids:
                    discovered[edge.source_id] += 1
                    if edge.source_id not in discovery_method:
                        discovery_method[edge.source_id] = f"cites '{seed['title'][:40]}...'"

            # 3. Other papers by the same author
            for edge in txn.get_outgoing_edges(sid):
                if edge.edge_type == "AUTHORED_BY":
                    author_id = edge.target_id
                    for incoming in txn.get_incoming_edges(author_id):
                        if (
                            incoming.edge_type == "AUTHORED_BY"
                            and incoming.source_id not in seed_ids
                        ):
                            discovered[incoming.source_id] += 1
                            if incoming.source_id not in discovery_method:
                                author_name = txn.get_property(author_id, "name")
                                discovery_method[incoming.source_id] = (
                                    f"same author ({author_name})"
                                )

        # Resolve paper details for discovered nodes
        expanded = []
        for node_id, count in discovered.most_common():
            title = txn.get_property(node_id, "title")
            year = txn.get_property(node_id, "year")
            venue = txn.get_property(node_id, "venue")
            key = txn.get_property(node_id, "key")
            paper = next((p for p in PAPERS if p["key"] == key), None)
            expanded.append(
                {
                    "node_id": node_id,
                    "title": title,
                    "year": year,
                    "venue": venue,
                    "via": discovery_method[node_id],
                    "paths": count,
                    "abstract": paper["abstract"] if paper else "",
                }
            )

    return expanded


# ---------------------------------------------------------------------------
# Act 3: Full-text search
# ---------------------------------------------------------------------------


def fts_search_demo(db: Database, query_text: str, limit: int = 5) -> list[dict]:
    results = db.fts_search(query_text, limit=limit)
    papers = []
    with db.read() as txn:
        for r in results:
            title = txn.get_property(r.node_id, "title")
            year = txn.get_property(r.node_id, "year")
            venue = txn.get_property(r.node_id, "venue")
            key = txn.get_property(r.node_id, "key")
            paper = next((p for p in PAPERS if p["key"] == key), None)
            papers.append(
                {
                    "node_id": r.node_id,
                    "title": title,
                    "year": year,
                    "venue": venue,
                    "score": r.score,
                    "abstract": paper["abstract"] if paper else "",
                }
            )
    return papers


# ---------------------------------------------------------------------------
# Act 4: Combined Graph RAG pipeline
# ---------------------------------------------------------------------------


def graph_rag_pipeline(
    db: Database, query: str, embed_fn, k: int = 5
) -> tuple[list[dict], list[dict]]:
    """Returns (vector_only_papers, graph_rag_papers)."""

    # Step 1: Vector search seeds
    vector_papers = vector_search_only(db, query, embed_fn, k=k)

    # Step 2: Graph expansion
    expanded = graph_expand(db, vector_papers)

    # Step 3: FTS boost — find papers matching key terms
    fts_papers = fts_search_demo(db, "knowledge graphs LLM", limit=10)
    fts_ids = {p["node_id"] for p in fts_papers}

    # Step 4: Merge and deduplicate
    seen = set()
    graph_rag_papers = []

    # Add vector results first
    for p in vector_papers:
        if p["node_id"] not in seen:
            p["via"] = "vector search"
            graph_rag_papers.append(p)
            seen.add(p["node_id"])

    # Add graph-expanded results
    for p in expanded:
        if p["node_id"] not in seen:
            graph_rag_papers.append(p)
            seen.add(p["node_id"])

    # Add FTS results not already found
    for p in fts_papers:
        if p["node_id"] not in seen:
            p["via"] = "full-text search"
            graph_rag_papers.append(p)
            seen.add(p["node_id"])

    return vector_papers, graph_rag_papers


# ---------------------------------------------------------------------------
# Act 5: LLM-powered answer
# ---------------------------------------------------------------------------


def ask_llm(question: str, context: str) -> str | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key or not HAS_ANTHROPIC:
        return None

    client = Anthropic()
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=(
            "You are a research assistant. Answer the question based ONLY on "
            "the provided papers. Cite papers by title when referencing them. "
            "Be concise but thorough."
        ),
        messages=[
            {
                "role": "user",
                "content": f"Papers:\n{context}\n\nQuestion: {question}",
            }
        ],
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Research Paper Graph RAG Demo")
    parser.add_argument(
        "--ollama",
        action="store_true",
        help="Use Ollama for real embeddings (requires running Ollama server)",
    )
    args = parser.parse_args()

    # Choose embedding function
    if args.ollama:
        from latticedb.embedding import EmbeddingClient

        print("Connecting to Ollama for embeddings...")
        embed_client = EmbeddingClient("http://localhost:11434")
        embed_fn = embed_client.embed
    else:
        embed_fn = lambda text: hash_embed(text, dimensions=128)

    db_path = Path("_paper_graph_rag_demo.db")

    # Clean up any previous run
    for suffix in ["", "-wal"]:
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()

    try:
        with Database(
            db_path,
            create=True,
            enable_vectors=True,
            vector_dimensions=128,
        ) as db:

            # ---- Act 0: Build the graph ----
            print_section("Act 0: Building the Knowledge Graph")
            stats = create_graph(db, embed_fn)
            print(
                f"  Created {stats['papers']} papers, {stats['authors']} authors, "
                f"{stats['topics']} topics, {stats['citations']} citations\n"
            )

            query = "How can retrieval improve large language model accuracy?"
            print(f'  Query: "{query}"\n')

            # ---- Act 1: Vector search only ----
            print_section("Act 1: Vector Search Only")
            vector_results = vector_search_only(db, query, embed_fn)
            print_papers(vector_results)
            print(
                f"\n  Vector search found {len(vector_results)} papers."
                "\n  But does it capture the full picture? Let's find out...\n"
            )

            # ---- Act 2: Graph expansion ----
            print_section("Act 2: Graph-Enhanced Retrieval")
            expanded = graph_expand(db, vector_results)
            print(f"  Starting from {len(vector_results)} seed papers, graph traversal discovered:\n")
            print_papers(expanded)
            if expanded:
                top = expanded[0]
                print(
                    f"\n  Most connected discovery: '{top['title']}'"
                    f"\n  Found via: {top['via']} ({top['paths']} paths)\n"
                )

            # ---- Act 3: Full-text search ----
            print_section("Act 3: Full-Text Search Precision")
            fts_results = fts_search_demo(db, "knowledge graphs")
            print('  FTS query: "knowledge graphs"\n')
            if fts_results:
                print_papers(fts_results)
                print(
                    "\n  FTS adds precision: it found papers that mention "
                    "'knowledge graph' by name,\n  which vector similarity can "
                    "miss when abstracts use different vocabulary.\n"
                )
            else:
                print("  No exact FTS matches (try --ollama for better results).\n")

            # ---- Act 4: Full pipeline ----
            print_section("Act 4: Complete Graph RAG Pipeline")
            vector_only, graph_rag = graph_rag_pipeline(db, query, embed_fn)

            print(f"  Vector-only: {len(vector_only)} papers")
            print(f"  Graph RAG:   {len(graph_rag)} papers\n")

            print("  --- Vector RAG context ---")
            print_papers(vector_only, prefix="")
            print("\n  --- Graph RAG context (vector + graph + FTS) ---")
            print_papers(graph_rag, prefix="")

            new_count = len(graph_rag) - len(vector_only)
            print(
                f"\n  Graph RAG discovered {new_count} additional papers by "
                "traversing citations,\n  author connections, and keyword matches "
                "— context that vector search alone would miss.\n"
            )

            # ---- Act 5: LLM answers ----
            print_section("Act 5: LLM-Powered Answers (Claude)")

            vector_context = format_context(vector_only)
            graph_rag_context = format_context(graph_rag)

            if not os.environ.get("ANTHROPIC_API_KEY"):
                print(
                    "  Set ANTHROPIC_API_KEY to see LLM-powered answers.\n"
                    "  Example: ANTHROPIC_API_KEY=sk-... python paper_graph_rag.py\n"
                )
                if not HAS_ANTHROPIC:
                    print("  (Also install: pip install anthropic)\n")
                print("  Here's the prompt that would be sent:\n")
                print(f"  Question: {query}")
                print(f"  Context papers: {len(graph_rag)} papers")
                print(f"  Context length: {len(graph_rag_context)} chars\n")
            else:
                print("  Asking Claude with vector-only context...\n")
                answer_vector = ask_llm(query, vector_context)
                if answer_vector:
                    print("  --- Answer from Vector RAG ---")
                    for line in answer_vector.split("\n"):
                        print(f"  {line}")
                    print()

                print("  Asking Claude with Graph RAG context...\n")
                answer_graph = ask_llm(query, graph_rag_context)
                if answer_graph:
                    print("  --- Answer from Graph RAG ---")
                    for line in answer_graph.split("\n"):
                        print(f"  {line}")
                    print()

                print(
                    "  The Graph RAG answer has richer context about foundational works,\n"
                    "  citation relationships, and the full research landscape.\n"
                )

    finally:
        # Cleanup
        for suffix in ["", "-wal"]:
            p = Path(str(db_path) + suffix)
            if p.exists():
                p.unlink()

        if args.ollama:
            embed_client.close()

    print("Done!")


if __name__ == "__main__":
    main()
