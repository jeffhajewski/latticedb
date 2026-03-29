# Graph, Vector, and Full-Text Retrieval Example

This example uses a research-paper citation graph to show how graph traversal, vector similarity, and BM25 full-text search complement each other in one local database. The workflow happens to be Graph RAG, but the point of the demo is to illustrate LatticeDB's underlying graph/vector/text primitives rather than define the database as a RAG product.

## What This Shows

1. **Vector Search** finds semantically similar papers — but misses foundational works and the broader research landscape
2. **Graph Traversal** follows citations, co-authorship, and topic links to discover papers that vector similarity alone cannot surface
3. **Full-Text Search** adds keyword precision for exact terminology
4. **Graph RAG** is one way to combine those primitives to build richer context for an LLM

## Running the Examples

### Python

```bash
cd examples/python
pip install latticedb numpy
python paper_graph_rag.py
```

### TypeScript

```bash
cd examples/ts
npm install
npm run paper-graph-rag
```

### Go

Build the shared library from the repo root first:

```bash
zig build shared
cd examples/go
go run .
```

### With LLM Answers (Optional)

Set `ANTHROPIC_API_KEY` to compare LLM answers using vector-only vs graph-expanded context:

```bash
# Python
pip install anthropic
ANTHROPIC_API_KEY=sk-... python paper_graph_rag.py

# TypeScript
ANTHROPIC_API_KEY=sk-... npm run paper-graph-rag
```

### With Real Embeddings (Optional)

By default the examples use hash-based embeddings (no external services needed). For real embeddings via Ollama:

```bash
ollama serve
ollama pull nomic-embed-text
python paper_graph_rag.py --ollama
```

## The Key Insight

Vector search finds 5 papers directly relevant to "How can retrieval improve large language model accuracy?" — but by traversing the citation graph, author connections, and full-text keyword matches, the graph-expanded workflow discovers 9 additional papers (14 total) including foundational works like "Attention Is All You Need" and the latest Graph RAG research. These are papers that vector similarity misses because their abstracts use different vocabulary, but the citation graph reveals they are deeply connected to the query topic.

When both sets of context are sent to an LLM, the graph-expanded answer is noticeably more comprehensive — it can trace the evolution from transformers through RAG to Graph RAG, cite foundational papers, and identify the current research frontier. The broader point is that LatticeDB lets one local application combine relationship, semantic, and text retrieval over the same data without splitting the workload across separate systems.
