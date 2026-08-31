#!/usr/bin/env node
/**
 * Deterministic generator for the LatticeDB sample research graph.
 *
 * The output uses the JSON shape that `lattice import` expects:
 *
 *     { "nodes": [{ id, labels, properties }], "edges": [{ source, target, type, properties }] }
 *
 * Every value is drawn from a seeded PRNG, so the same --seed always produces a
 * byte-identical file. Requires Node 22.18+ for native TypeScript type stripping.
 *
 * Usage:
 *     node examples/sample-graph/generate_graph.ts [--out=<file>] [--seed=<n>] [--scale=<f>] [--pretty]
 */

import { writeFileSync } from "node:fs";

type PropertyValue = string | number | boolean;

interface NodeRecord {
  id: string;
  labels: string[];
  properties: Record<string, PropertyValue>;
}

interface EdgeRecord {
  source: string;
  target: string;
  type: string;
  properties: Record<string, PropertyValue>;
}

interface GraphDocument {
  nodes: NodeRecord[];
  edges: EdgeRecord[];
}

interface Options {
  out: string;
  seed: number;
  scale: number;
  pretty: boolean;
}

interface Rng {
  next(): number;
  int(minInclusive: number, maxInclusive: number): number;
  float(min: number, max: number, decimals: number): number;
  pick<T>(items: readonly T[]): T;
  chance(probability: number): boolean;
  sample<T>(items: readonly T[], count: number): T[];
}

/** mulberry32: small, fast, and stable across Node versions. */
function createRng(seed: number): Rng {
  let state: number = seed >>> 0;

  const next = (): number => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t: number = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };

  const int = (minInclusive: number, maxInclusive: number): number =>
    minInclusive + Math.floor(next() * (maxInclusive - minInclusive + 1));

  const float = (min: number, max: number, decimals: number): number => {
    const factor: number = 10 ** decimals;
    return Math.round((min + next() * (max - min)) * factor) / factor;
  };

  const pick = <T,>(items: readonly T[]): T => items[int(0, items.length - 1)];

  const chance = (probability: number): boolean => next() < probability;

  const sample = <T,>(items: readonly T[], count: number): T[] => {
    const pool: T[] = items.slice();
    const taken: number = Math.min(count, pool.length);
    for (let i = 0; i < taken; i += 1) {
      const j: number = int(i, pool.length - 1);
      const swap: T = pool[i];
      pool[i] = pool[j];
      pool[j] = swap;
    }
    return pool.slice(0, taken);
  };

  return { next, int, float, pick, chance, sample };
}

// --- Vocabulary -------------------------------------------------------------

const FIRST_NAMES: readonly string[] = [
  "Ada", "Amara", "Anders", "Aoife", "Bao", "Beatriz", "Camille", "Chidi",
  "Daniela", "Dmitri", "Elena", "Emeka", "Farid", "Freya", "Gabriel", "Hana",
  "Ibrahim", "Ingrid", "Jae", "Julia", "Kaito", "Karim", "Lena", "Liang",
  "Mariam", "Mateo", "Nadia", "Niklas", "Olamide", "Priya", "Rafael", "Rina",
  "Sanjay", "Sofia", "Tariq", "Thandiwe", "Viktor", "Wen", "Yara", "Zoltan",
];

const LAST_NAMES: readonly string[] = [
  "Abara", "Andersen", "Bianchi", "Chowdhury", "Delacroix", "Eriksen", "Faraday",
  "Gallo", "Halvorsen", "Ibrahim", "Jensen", "Kaur", "Kovacs", "Lindqvist",
  "Marchetti", "Mbeki", "Nakamura", "Novak", "Okafor", "Petrova", "Quintero",
  "Rahman", "Ramirez", "Sandoval", "Sato", "Silva", "Sorensen", "Tanaka",
  "Thorne", "Ueda", "Vargas", "Voss", "Wallace", "Wang", "Weber", "Xu",
  "Yamamoto", "Yilmaz", "Zhang", "Zielinski",
];

const TOPIC_NAMES: readonly string[] = [
  "graph neural networks", "vector quantization", "approximate nearest neighbor search",
  "retrieval augmented generation", "knowledge graph embedding", "query optimization",
  "log-structured merge trees", "write-ahead logging", "multi-version concurrency control",
  "columnar storage", "distributed consensus", "vectorized execution",
  "learned index structures", "cardinality estimation", "streaming joins",
  "change data capture", "full-text ranking", "inverted indexes",
  "sparse retrieval", "dense retrieval", "hybrid search", "reranking models",
  "transformer architectures", "attention mechanisms", "model quantization",
  "knowledge distillation", "self-supervised learning", "contrastive learning",
  "graph partitioning", "community detection", "shortest path algorithms",
  "subgraph matching", "temporal graphs", "property graph models",
  "declarative query languages", "schema inference", "data provenance",
  "crash recovery", "storage compaction", "memory-mapped io",
];

const RESEARCH_FIELDS: readonly string[] = [
  "Databases", "Machine Learning", "Distributed Systems", "Information Retrieval",
  "Graph Theory", "Systems Engineering", "Natural Language Processing",
];

const VENUES: readonly { name: string; kind: string; founded: number }[] = [
  { name: "VLDB", kind: "conference", founded: 1975 },
  { name: "SIGMOD", kind: "conference", founded: 1975 },
  { name: "ICDE", kind: "conference", founded: 1984 },
  { name: "CIDR", kind: "conference", founded: 2003 },
  { name: "EuroSys", kind: "conference", founded: 2006 },
  { name: "OSDI", kind: "conference", founded: 1994 },
  { name: "SOSP", kind: "conference", founded: 1967 },
  { name: "NeurIPS", kind: "conference", founded: 1987 },
  { name: "ICML", kind: "conference", founded: 1980 },
  { name: "ICLR", kind: "conference", founded: 2013 },
  { name: "KDD", kind: "conference", founded: 1995 },
  { name: "TheWebConf", kind: "conference", founded: 1994 },
  { name: "ACL", kind: "conference", founded: 1962 },
  { name: "EMNLP", kind: "conference", founded: 1996 },
  { name: "TODS", kind: "journal", founded: 1976 },
  { name: "TKDE", kind: "journal", founded: 1989 },
];

const CITIES: readonly { city: string; country: string }[] = [
  { city: "Zurich", country: "Switzerland" }, { city: "Toronto", country: "Canada" },
  { city: "Lagos", country: "Nigeria" }, { city: "Kyoto", country: "Japan" },
  { city: "Delft", country: "Netherlands" }, { city: "Trondheim", country: "Norway" },
  { city: "Bangalore", country: "India" }, { city: "Sao Paulo", country: "Brazil" },
  { city: "Lisbon", country: "Portugal" }, { city: "Krakow", country: "Poland" },
  { city: "Melbourne", country: "Australia" }, { city: "Haifa", country: "Israel" },
  { city: "Nairobi", country: "Kenya" }, { city: "Seoul", country: "South Korea" },
  { city: "Helsinki", country: "Finland" }, { city: "Santiago", country: "Chile" },
  { city: "Edinburgh", country: "United Kingdom" }, { city: "Montreal", country: "Canada" },
  { city: "Shenzhen", country: "China" }, { city: "Istanbul", country: "Turkey" },
];

const COMPANY_ROOTS: readonly string[] = [
  "Lattice", "Northwind", "Meridian", "Solstice", "Cobalt", "Verdant", "Halcyon",
  "Ironwood", "Quartz", "Terrace", "Beacon", "Foundry",
];

const TITLE_MODIFIERS: readonly string[] = [
  "Adaptive", "Scalable", "Robust", "Efficient", "Incremental", "Lightweight",
  "Hierarchical", "Approximate", "Learned", "Streaming", "Compressed", "Concurrent",
];

const MECHANISMS: readonly string[] = [
  "Index Compression", "Vectorized Execution", "Graph Traversal", "Cost Modeling",
  "Sketching", "Cache Admission", "Batch Scheduling", "Partition Pruning",
  "Query Rewriting", "Adaptive Sampling", "Memory Layouts", "Delta Encoding",
];

const PROPERTIES_STUDIED: readonly string[] = [
  "Cost", "Robustness", "Latency", "Accuracy", "Locality", "Recall", "Durability",
];

const METRICS: readonly string[] = [
  "tail latency", "index build time", "memory footprint", "query throughput",
  "recall@10", "write amplification", "recovery time",
];

const WORKLOADS: readonly string[] = [
  "a 1B-edge citation graph", "a mixed read/write OLTP trace", "an embedded analytics workload",
  "a multi-tenant retrieval service", "a nightly batch ingestion pipeline",
  "an interactive exploration session",
];

// --- Helpers ----------------------------------------------------------------

function titleCase(text: string): string {
  return text
    .split(" ")
    .map((word: string): string => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function slugify(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function pad(value: number, width: number): string {
  return String(value).padStart(width, "0");
}

function scaled(base: number, scale: number): number {
  return Math.max(1, Math.round(base * scale));
}

/**
 * Picks an index with probability proportional to its weight — the preferential
 * attachment that gives citation counts and author productivity a long tail.
 */
function weightedIndex(rng: Rng, weights: readonly number[]): number {
  let total = 0;
  for (const weight of weights) total += weight;
  let target: number = rng.next() * total;
  for (let i = 0; i < weights.length; i += 1) {
    target -= weights[i];
    if (target <= 0) return i;
  }
  return weights.length - 1;
}

// --- Node builders ----------------------------------------------------------

function buildTopics(rng: Rng, count: number): NodeRecord[] {
  return TOPIC_NAMES.slice(0, count).map((name: string, index: number): NodeRecord => ({
    id: `topic:${pad(index + 1, 4)}`,
    labels: ["Topic"],
    properties: {
      name,
      slug: slugify(name),
      field: RESEARCH_FIELDS[index % RESEARCH_FIELDS.length],
      maturity: rng.float(0.1, 1, 2),
    },
  }));
}

function buildVenues(rng: Rng, count: number): NodeRecord[] {
  return VENUES.slice(0, count).map((venue, index: number): NodeRecord => ({
    id: `venue:${pad(index + 1, 4)}`,
    labels: ["Venue"],
    properties: {
      name: venue.name,
      kind: venue.kind,
      founded: venue.founded,
      acceptance_rate: rng.float(0.12, 0.32, 3),
    },
  }));
}

function buildOrganizations(rng: Rng, count: number): NodeRecord[] {
  const organizations: NodeRecord[] = [];
  const used: Set<string> = new Set<string>();

  while (organizations.length < count) {
    const place = CITIES[organizations.length % CITIES.length];
    const kind: string = rng.pick(["university", "lab", "company"]);
    let name: string;
    if (kind === "university") {
      name = rng.chance(0.5)
        ? `University of ${place.city}`
        : `${place.city} Institute of Technology`;
    } else if (kind === "lab") {
      name = `${place.city} Systems Lab`;
    } else {
      name = `${rng.pick(COMPANY_ROOTS)} ${rng.pick(["Labs", "Systems", "Research"])}`;
    }
    if (used.has(name)) continue;
    used.add(name);

    organizations.push({
      id: `org:${pad(organizations.length + 1, 4)}`,
      labels: ["Organization"],
      properties: {
        name,
        kind,
        city: place.city,
        country: place.country,
        headcount: rng.int(40, 4000),
      },
    });
  }

  return organizations;
}

function buildPeople(rng: Rng, count: number): NodeRecord[] {
  const people: NodeRecord[] = [];
  const used: Set<string> = new Set<string>();

  while (people.length < count) {
    const first: string = rng.pick(FIRST_NAMES);
    const last: string = rng.pick(LAST_NAMES);
    let name = `${first} ${last}`;
    if (used.has(name)) {
      const initial: string = rng.pick(FIRST_NAMES).charAt(0);
      name = `${first} ${initial}. ${last}`;
      if (used.has(name)) continue;
    }
    used.add(name);

    const startedYear: number = rng.int(1998, 2022);
    people.push({
      id: `person:${pad(people.length + 1, 4)}`,
      labels: ["Person", "Researcher"],
      properties: {
        name,
        email: `${slugify(name)}@example.org`,
        field: rng.pick(RESEARCH_FIELDS),
        started_year: startedYear,
        h_index: rng.int(1, Math.max(2, 2025 - startedYear)),
        is_faculty: rng.chance(0.4),
      },
    });
  }

  return people;
}

function buildPapers(rng: Rng, count: number): NodeRecord[] {
  const papers: NodeRecord[] = [];

  for (let index = 0; index < count; index += 1) {
    const topic: string = rng.pick(TOPIC_NAMES);
    const modifier: string = rng.pick(TITLE_MODIFIERS);
    const mechanism: string = rng.pick(MECHANISMS);
    const property: string = rng.pick(PROPERTIES_STUDIED);
    const templates: readonly string[] = [
      `${modifier} ${mechanism} for ${titleCase(topic)}`,
      `Rethinking ${titleCase(topic)} with ${mechanism}`,
      `${mechanism}: ${modifier} ${titleCase(topic)} at Scale`,
      `On the ${property} of ${titleCase(topic)}`,
      `Towards ${modifier} ${titleCase(topic)}`,
      `A ${property} Study of ${mechanism} in ${titleCase(topic)}`,
    ];
    const year: number = rng.int(2015, 2025);

    papers.push({
      id: `paper:${pad(index + 1, 4)}`,
      labels: ["Document", "Paper"],
      properties: {
        title: rng.pick(templates),
        year,
        doi: `10.5555/lattice.${year}.${pad(index + 1, 4)}`,
        page_count: rng.int(8, 24),
        peer_reviewed: rng.chance(0.85),
        abstract:
          `We study ${topic} under ${rng.pick(WORKLOADS)}. ` +
          `Combining ${mechanism.toLowerCase()} with ${rng.pick(MECHANISMS).toLowerCase()}, ` +
          `the system improves ${rng.pick(METRICS)} by ${rng.int(8, 62)}% ` +
          `without regressing ${rng.pick(METRICS)}.`,
      },
    });
  }

  return papers;
}

function buildChunks(rng: Rng, papers: readonly NodeRecord[], count: number): NodeRecord[] {
  const chunks: NodeRecord[] = [];
  const sections: readonly string[] = [
    "Introduction", "Background", "Design", "Implementation", "Evaluation", "Related Work",
  ];

  for (let index = 0; index < count; index += 1) {
    const paper: NodeRecord = papers[index % papers.length];
    const section: string = sections[index % sections.length];
    const text: string =
      `${section}. ${rng.pick(MECHANISMS)} is applied to ${rng.pick(TOPIC_NAMES)} ` +
      `on ${rng.pick(WORKLOADS)}. We measure ${rng.pick(METRICS)} across ` +
      `${rng.int(3, 12)} configurations and observe a ${rng.float(1.1, 9.4, 1)}x change ` +
      `in ${rng.pick(METRICS)} relative to the baseline.`;

    chunks.push({
      id: `chunk:${pad(index + 1, 4)}`,
      labels: ["Chunk"],
      properties: {
        text,
        section,
        ordinal: Math.floor(index / papers.length) + 1,
        token_count: Math.ceil(text.length / 4),
        paper_title: String(paper.properties.title),
      },
    });
  }

  return chunks;
}

// --- Edge builders ----------------------------------------------------------

function buildAffiliations(
  rng: Rng,
  people: readonly NodeRecord[],
  orgs: readonly NodeRecord[],
): EdgeRecord[] {
  const edges: EdgeRecord[] = [];
  const roles: readonly string[] = [
    "PhD student", "postdoc", "research scientist", "professor", "engineer",
  ];

  for (const person of people) {
    const primary: NodeRecord = rng.pick(orgs);
    edges.push({
      source: person.id,
      target: primary.id,
      type: "AFFILIATED_WITH",
      properties: {
        role: rng.pick(roles),
        since: rng.int(Number(person.properties.started_year), 2025),
        is_primary: true,
      },
    });

    if (rng.chance(0.22)) {
      const secondary: NodeRecord = rng.pick(orgs);
      if (secondary.id !== primary.id) {
        edges.push({
          source: person.id,
          target: secondary.id,
          type: "AFFILIATED_WITH",
          properties: { role: "visiting", since: rng.int(2018, 2025), is_primary: false },
        });
      }
    }
  }

  return edges;
}

interface AuthorshipResult {
  authored: EdgeRecord[];
  collaborations: EdgeRecord[];
  /** Author ids per paper id, so citations can tell a self-citation from the rest. */
  authorsByPaper: Map<string, Set<string>>;
}

function buildAuthorship(
  rng: Rng,
  people: readonly NodeRecord[],
  papers: readonly NodeRecord[],
): AuthorshipResult {
  const authored: EdgeRecord[] = [];
  const productivity: number[] = people.map((): number => 1);
  const coauthorCounts: Map<string, number> = new Map<string, number>();
  const authorsByPaper: Map<string, Set<string>> = new Map<string, Set<string>>();

  for (const paper of papers) {
    const authorCount: number = rng.int(2, 5);
    const chosen: number[] = [];
    while (chosen.length < authorCount) {
      const index: number = weightedIndex(rng, productivity);
      if (chosen.includes(index)) continue;
      chosen.push(index);
      productivity[index] += 1;
    }

    authorsByPaper.set(paper.id, new Set<string>(chosen.map((i: number): string => people[i].id)));

    chosen.forEach((personIndex: number, position: number): void => {
      authored.push({
        source: people[personIndex].id,
        target: paper.id,
        type: "AUTHORED",
        properties: {
          position: position + 1,
          is_corresponding: position === 0,
          contribution: rng.float(0.05, 0.6, 2),
        },
      });
    });

    for (let i = 0; i < chosen.length; i += 1) {
      for (let j = i + 1; j < chosen.length; j += 1) {
        const a: string = people[Math.min(chosen[i], chosen[j])].id;
        const b: string = people[Math.max(chosen[i], chosen[j])].id;
        const key = `${a}|${b}`;
        coauthorCounts.set(key, (coauthorCounts.get(key) ?? 0) + 1);
      }
    }
  }

  const collaborations: EdgeRecord[] = [];
  for (const [key, papersTogether] of coauthorCounts) {
    const [source, target] = key.split("|");
    collaborations.push({
      source,
      target,
      type: "COLLABORATES_WITH",
      properties: {
        papers_together: papersTogether,
        strength: Math.round((papersTogether / 5) * 100) / 100,
      },
    });
  }

  return { authored, collaborations, authorsByPaper };
}

function buildChunkLinks(
  rng: Rng,
  chunks: readonly NodeRecord[],
  papers: readonly NodeRecord[],
): EdgeRecord[] {
  return chunks.map((chunk: NodeRecord, index: number): EdgeRecord => ({
    source: chunk.id,
    target: papers[index % papers.length].id,
    type: "PART_OF",
    properties: {
      ordinal: Number(chunk.properties.ordinal),
      char_length: String(chunk.properties.text).length,
      confidence: rng.float(0.7, 1, 2),
    },
  }));
}

/**
 * Citations point backwards in time, weighted by how often a paper is already
 * cited and how close it is to the citing year. Preferential attachment alone
 * would pile every citation onto the oldest papers, since they have the longest
 * head start; the exponential recency term keeps most references within a few
 * years while still letting a heavily cited older paper stay visible.
 */
function buildCitations(
  rng: Rng,
  papers: readonly NodeRecord[],
  authorsByPaper: ReadonlyMap<string, Set<string>>,
): EdgeRecord[] {
  const ordered: NodeRecord[] = papers
    .slice()
    .sort((a: NodeRecord, b: NodeRecord): number =>
      Number(a.properties.year) - Number(b.properties.year));
  const citedCounts: number[] = ordered.map((): number => 1);
  const edges: EdgeRecord[] = [];
  const intents: readonly string[] = ["background", "comparison", "method reuse", "motivation"];

  ordered.forEach((paper: NodeRecord, index: number): void => {
    if (index === 0) return;
    const references: number = Math.min(index, rng.int(0, 7));
    const seen: Set<number> = new Set<number>();
    const citingYear: number = Number(paper.properties.year);
    const candidateWeights: number[] = citedCounts
      .slice(0, index)
      .map((count: number, i: number): number => {
        const yearGap: number = citingYear - Number(ordered[i].properties.year);
        return count * Math.exp(-yearGap / 4);
      });

    for (let i = 0; i < references; i += 1) {
      const target: number = weightedIndex(rng, candidateWeights);
      if (seen.has(target)) continue;
      seen.add(target);
      citedCounts[target] += 1;

      const citingAuthors: Set<string> = authorsByPaper.get(paper.id) ?? new Set<string>();
      const citedAuthors: Set<string> = authorsByPaper.get(ordered[target].id) ?? new Set<string>();
      const sharesAuthor: boolean = [...citedAuthors].some((id: string): boolean =>
        citingAuthors.has(id));

      edges.push({
        source: paper.id,
        target: ordered[target].id,
        type: "CITES",
        properties: {
          intent: rng.pick(intents),
          year_gap: Number(paper.properties.year) - Number(ordered[target].properties.year),
          is_self_citation: sharesAuthor,
        },
      });
    }
  });

  return edges;
}

function buildTopicLinks(
  rng: Rng,
  papers: readonly NodeRecord[],
  topics: readonly NodeRecord[],
): EdgeRecord[] {
  const edges: EdgeRecord[] = [];

  for (const paper of papers) {
    for (const topic of rng.sample(topics, rng.int(1, 3))) {
      edges.push({
        source: paper.id,
        target: topic.id,
        type: "ABOUT",
        properties: {
          weight: rng.float(0.2, 1, 2),
          assigned_by: rng.chance(0.7) ? "author" : "classifier",
        },
      });
    }
  }

  return edges;
}

function buildVenueLinks(
  rng: Rng,
  papers: readonly NodeRecord[],
  venues: readonly NodeRecord[],
): EdgeRecord[] {
  return papers.map((paper: NodeRecord): EdgeRecord => ({
    source: paper.id,
    target: rng.pick(venues).id,
    type: "PUBLISHED_IN",
    properties: {
      year: Number(paper.properties.year),
      track: rng.pick(["research", "industry", "short", "demo"]),
      is_oral: rng.chance(0.25),
    },
  }));
}

function buildTopicGraph(rng: Rng, topics: readonly NodeRecord[]): EdgeRecord[] {
  const edges: EdgeRecord[] = [];
  const seen: Set<string> = new Set<string>();

  for (const topic of topics) {
    for (const other of rng.sample(topics, rng.int(1, 3))) {
      if (other.id === topic.id) continue;
      const key = `${topic.id}|${other.id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      edges.push({
        source: topic.id,
        target: other.id,
        type: "RELATED_TO",
        properties: { similarity: rng.float(0.3, 0.95, 2), co_occurrences: rng.int(2, 180) },
      });
    }
  }

  return edges;
}

function buildChunkMentions(
  rng: Rng,
  chunks: readonly NodeRecord[],
  topics: readonly NodeRecord[],
): EdgeRecord[] {
  const edges: EdgeRecord[] = [];

  for (const chunk of chunks) {
    if (!rng.chance(0.55)) continue;
    for (const topic of rng.sample(topics, rng.int(1, 2))) {
      edges.push({
        source: chunk.id,
        target: topic.id,
        type: "MENTIONS",
        properties: { salience: rng.float(0.1, 1, 2), occurrences: rng.int(1, 9) },
      });
    }
  }

  return edges;
}

// --- Entry point ------------------------------------------------------------

function parseOptions(argv: readonly string[]): Options {
  const options: Options = {
    out: "examples/sample-graph/sample_graph.json",
    seed: 20260828,
    scale: 1,
    pretty: false,
  };

  for (const arg of argv) {
    if (arg.startsWith("--out=")) options.out = arg.slice("--out=".length);
    else if (arg.startsWith("--seed=")) options.seed = Number(arg.slice("--seed=".length));
    else if (arg.startsWith("--scale=")) options.scale = Number(arg.slice("--scale=".length));
    else if (arg === "--pretty") options.pretty = true;
    else throw new Error(`unknown argument: ${arg}`);
  }

  if (!Number.isFinite(options.seed)) throw new Error("--seed must be a number");
  if (!Number.isFinite(options.scale) || options.scale <= 0) throw new Error("--scale must be > 0");

  return options;
}

function generate(options: Options): GraphDocument {
  const rng: Rng = createRng(options.seed);

  const topics: NodeRecord[] = buildTopics(rng, Math.min(TOPIC_NAMES.length, scaled(40, options.scale)));
  const venues: NodeRecord[] = buildVenues(rng, Math.min(VENUES.length, scaled(16, options.scale)));
  const organizations: NodeRecord[] = buildOrganizations(rng, scaled(36, options.scale));
  const people: NodeRecord[] = buildPeople(rng, scaled(240, options.scale));
  const papers: NodeRecord[] = buildPapers(rng, scaled(320, options.scale));
  const chunks: NodeRecord[] = buildChunks(rng, papers, scaled(420, options.scale));

  const authorship: AuthorshipResult = buildAuthorship(rng, people, papers);
  const edges: EdgeRecord[] = [
    ...buildAffiliations(rng, people, organizations),
    ...authorship.authored,
    ...authorship.collaborations,
    ...buildChunkLinks(rng, chunks, papers),
    ...buildCitations(rng, papers, authorship.authorsByPaper),
    ...buildTopicLinks(rng, papers, topics),
    ...buildVenueLinks(rng, papers, venues),
    ...buildTopicGraph(rng, topics),
    ...buildChunkMentions(rng, chunks, topics),
  ];

  return { nodes: [...topics, ...venues, ...organizations, ...people, ...papers, ...chunks], edges };
}

function summarize(graph: GraphDocument): void {
  const nodesByLabel: Map<string, number> = new Map<string, number>();
  for (const node of graph.nodes) {
    const label: string = node.labels[0];
    nodesByLabel.set(label, (nodesByLabel.get(label) ?? 0) + 1);
  }

  const edgesByType: Map<string, number> = new Map<string, number>();
  for (const edge of graph.edges) {
    edgesByType.set(edge.type, (edgesByType.get(edge.type) ?? 0) + 1);
  }

  console.log(`nodes: ${graph.nodes.length}`);
  for (const [label, count] of [...nodesByLabel].sort()) {
    console.log(`  ${label.padEnd(14)} ${count}`);
  }
  console.log(`edges: ${graph.edges.length}`);
  for (const [type, count] of [...edgesByType].sort()) {
    console.log(`  ${type.padEnd(18)} ${count}`);
  }
}

function main(): void {
  const options: Options = parseOptions(process.argv.slice(2));
  const graph: GraphDocument = generate(options);
  const json: string = options.pretty ? JSON.stringify(graph, null, 2) : JSON.stringify(graph);
  writeFileSync(options.out, `${json}\n`, "utf8");
  summarize(graph);
  console.log(`written: ${options.out}`);
}

main();
