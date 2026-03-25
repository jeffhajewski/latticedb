# Research Paper Graph RAG

Demonstrates why **Graph RAG** finds more relevant context than plain vector search, using a citation knowledge graph of 18 real AI/ML papers.

## What This Shows

1. **Vector Search** finds semantically similar papers — but misses foundational works and the broader research landscape
2. **Graph Traversal** follows citations, co-authorship, and topic links to discover papers that vector similarity alone cannot surface
3. **Full-Text Search** adds keyword precision for exact terminology
4. **Graph RAG** combines all three to build richer context for an LLM

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

Set `ANTHROPIC_API_KEY` to compare LLM answers using vector-only vs Graph RAG context:

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

Vector search finds 5 papers directly relevant to "How can retrieval improve large language model accuracy?" — but by traversing the citation graph, author connections, and full-text keyword matches, Graph RAG discovers 9 additional papers (14 total) including foundational works like "Attention Is All You Need" and the latest Graph RAG research. These are papers that vector similarity misses because their abstracts use different vocabulary, but the citation graph reveals they are deeply connected to the query topic.

When both sets of context are sent to an LLM, the Graph RAG answer is noticeably more comprehensive — it can trace the evolution from transformers through RAG to Graph RAG, cite foundational papers, and identify the current research frontier.
