# RDF: Turtle and N-Triples

`lattice import` reads RDF in Turtle and N-Triples, and `lattice export` writes
a graph back out in either. A vocabulary or a data set that lives in RDF can be
loaded into a database, queried with Cypher, full-text and vector search, and
written back as RDF for the tools that expect it.

This is interchange, not a triple store. The data becomes a property graph:
nodes with labels and properties, connected by edges. There is no SPARQL, no
inference (`rdfs:subClassOf` is an edge you can follow, not a rule that is
applied), and no named graphs.

## A Worked Example

`animals.ttl`:

```turtle
@prefix ex:   <https://example.org/animals/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .

ex:Animal a rdfs:Class ; rdfs:label "Animal"@en, "Tier"@de .
ex:Mammal a rdfs:Class ; rdfs:subClassOf ex:Animal ;
    rdfs:label "Mammal"@en, "Säugetier"@de .
ex:Cat a rdfs:Class ; rdfs:subClassOf ex:Mammal ;
    rdfs:label "Cat"@en, "Katze"@de ;
    skos:definition "A small domesticated carnivorous mammal."@en .
ex:tom a ex:Cat ; rdfs:label "Tom"@en ; ex:age 3 ;
    ex:chases ex:jerry {| ex:since 1940 |} .
ex:jerry rdfs:label "Jerry"@en .
```

Import it, declaring full-text indexes on two of its properties:

```bash
lattice create animals.lattice
lattice import animals.lattice --file=animals.ttl --rdf-fts=rdfs_label,skos_definition
```

```text
Importing animals.ttl (turtle) into animals.lattice...
Import complete
  Triples read:        18 (8 statements, 0.0s, 7604 triples/s)
  Nodes created:       5
  Edges created:       3
  Property values:     10 on nodes, 1 on edges
  Labels added:        4
  rdf:type as:         labels (2 distinct classes; labels up to 1000 with the existing labels)
  Names created:       9 (0 numbered to stay distinct), prefixes stored: 3
  Declared the property index Resource(iri)
  Full-text indexes declared: 2
```

Every IRI is now a node labelled `Resource` with an `iri` property, and each
predicate a name made of its prefix and local part. The superclasses of `Cat`,
by following `rdfs:subClassOf` edges any number of steps:

```bash
lattice exec animals.lattice --query="MATCH (c:Resource {iri: 'https://example.org/animals/Cat'})-[:rdfs_subClassOf*1..]->(super) RETURN super.rdfs_label AS class, super.rdfs_label_de AS german"
```

```text
┌──────────┬─────────────┐
│ class    │ german      │
├──────────┼─────────────┤
│ "Mammal" │ "Säugetier" │
│ "Animal" │ "Tier"      │
└──────────┴─────────────┘
```

`rdf:type` became a label, and the RDF-star annotation an edge property:

```bash
lattice exec animals.lattice --query="MATCH (n:ex_Cat)-[r:ex_chases]->(m) RETURN n.rdfs_label AS cat, m.rdfs_label AS mouse, r.ex_since AS since, n.ex_age AS age"
```

```text
┌───────┬─────────┬───────┬─────┐
│ cat   │ mouse   │ since │ age │
├───────┼─────────┼───────┼─────┤
│ "Tom" │ "Jerry" │ 1940  │ 3   │
└───────┴─────────┴───────┴─────┘
```

Full-text search over the definitions, and a count by type:

```bash
lattice exec animals.lattice --query="MATCH (n:Resource) WHERE n.skos_definition @@ 'carnivorous' RETURN n.iri"
lattice exec animals.lattice --query="MATCH (n:rdfs_Class) RETURN count(n) AS classes"
```

```text
┌───────────────────────────────────┐
│ n.iri                             │
├───────────────────────────────────┤
│ "https://example.org/animals/Cat" │
└───────────────────────────────────┘
┌─────────┐
│ classes │
├─────────┤
│ 3       │
└─────────┘
```

And back out as Turtle:

```bash
lattice export animals.lattice --file=animals_out.ttl
```

```turtle
@prefix : <urn:lattice:> .
@prefix ex: <https://example.org/animals/> .
@prefix lattice: <https://latticedb.org/ns#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Animal
    a rdfs:Class ;
    rdfs:label "Animal"@en, "Tier"@de .

ex:Mammal
    a rdfs:Class ;
    rdfs:label "Mammal"@en, "Säugetier"@de ;
    rdfs:subClassOf ex:Animal .

ex:Cat
    a rdfs:Class ;
    rdfs:label "Cat"@en, "Katze"@de ;
    skos:definition "A small domesticated carnivorous mammal."@en ;
    rdfs:subClassOf ex:Mammal .

ex:tom
    a ex:Cat ;
    rdfs:label "Tom"@en ;
    ex:age 3 ;
    ex:chases ex:jerry {| ex:since 1940 |} .

ex:jerry
    rdfs:label "Jerry"@en .
```

The file is the same RDF graph as the one imported, in a different order.

## Choosing the Syntax

The file extension chooses: `.ttl` and `.turtle` are Turtle, `.nt` and
`.ntriples` N-Triples. For another extension, say which with
`--format=turtle` or `--format=ntriples`. Both syntaxes are read as RDF 1.1
says, and pass the W3C Turtle and N-Triples test suites; Turtle-star and
N-Triples-star (`<< s p o >>` and `{| ... |}`) are read too.

## How RDF Becomes a Graph

| RDF | In the graph |
|-----|--------------|
| An IRI | A node labelled `Resource` with the IRI in its `iri` property |
| A blank node | A node with no label and no `iri` |
| `s rdf:type C` | The label for `C` on `s` (or an edge, see below) |
| `s p "literal"` | A property of `s` named for `p` |
| `s p o`, `o` an IRI or blank node | An edge of the type named for `p` from `s` to `o` |
| `s p ( "a" "b" )` | A list property named for `p`, with `_list` added |
| `s p o {\| q v \|}`, `<< s p o >> q v` | A property `q` of the edge `s p o` |

Import declares the property index `Resource(iri)` if the database does not
have it, and finds every IRI through it, so a second file imported into the same
database adds to the nodes the first one made. Blank nodes are local to their
file: `_:b1` in two files is two nodes.

### Names

Labels, edge types and property names are written `<prefix>_<local>`:
`rdfs:label` is `rdfs_label`, `skos:prefLabel` is `skos_prefLabel`, and a name
in the namespace of the empty prefix is just its local part. Characters Cypher
cannot have in a name become `_`, a name that starts with a digit gets a `_` in
front, and a name is cut at 200 bytes. Two IRIs never share a name: when one
would, the second gets `_2`, and the import reports how many it numbered.

The prefix is the one the database already has for the namespace, else the
file's own, else a well-known one (`rdf`, `rdfs`, `owl`, `xsd`, `skos`, `dc`,
`dcterms`, `foaf`, `schema`, `prov`, `sh`, `dcat`, `org` and a few more), else
`ns1`, `ns2`, and so on. One namespace has one prefix in a database: the first
one it was given.

Import keeps the table of names and prefixes in the database, as nodes labelled
`_RdfName` and `_RdfNamespace`, and export reads it to turn names back into
IRIs. These nodes are visible to Cypher and to the JSON and CSV exports; leave
them in place for as long as you want to export the graph as RDF.

### Values

| Literal | Stored as |
|---------|-----------|
| `xsd:integer` and its subtypes (`int`, `long`, `nonNegativeInteger`, ...) | Integer. One that does not fit in 64 bits fails its statement |
| `xsd:double`, `xsd:float` | Float |
| `xsd:decimal` | Float when the float is exactly the decimal written, else the text (`--rdf-decimal=string`: always the text) |
| `xsd:boolean` | Boolean |
| A plain or `xsd:string` literal, `"text"@tag` | String |
| `xsd:base64Binary` | Bytes |
| `rdf:JSON` holding an object | Map |
| `"[0.1, 0.2]"^^lattice:vector` (`https://latticedb.org/ns#vector`) | The node's vector (needs `--enable-vector`) |
| Any other datatype (`xsd:date`, `xsd:anyURI`, your own) | The text; the datatype is remembered for export |

Each property remembers the datatype of the first value it gets of each kind
(integer, float, string, bytes). A later value of the same kind with a
different datatype is kept as a map, such as
`{"@value": "plain", "@type": "http://www.w3.org/2001/XMLSchema#string"}`,
so that nothing is lost; the import counts these.

### Languages

Strings in one language, English unless `--rdf-lang` says otherwise, go to the
property itself (`rdfs_label`), so one full-text index covers them. Each other
language gets a property of its own, its tag added in lower case with `-` as `_`:
`rdfs_label_de`, `rdfs_label_en_gb`. Untagged strings go to the property too,
unless it already holds tagged ones; then they go to `<name>_plain`, and tagged
strings arriving at a property that holds untagged ones go to `<name>_en`.
Loading several vocabularies into one database often mixes the two:

```bash
lattice exec vocab.lattice --query="MATCH (c:rdfs_Class) RETURN coalesce(c.rdfs_label, c.rdfs_label_plain) AS label LIMIT 3"
```

```text
┌──────────────────┐
│ label            │
├──────────────────┤
│ "Label Property" │
│ "Person"         │
│ "Agent"          │
└──────────────────┘
```

Here SKOS, imported first, gave `rdfs:label` English strings, and FOAF's and
schema.org's untagged labels went to `rdfs_label_plain`.

### Repeated Values, Collections and Blank Nodes

A subject with several values for one predicate gets a list property, in file
order, each value once: RDF is a set of triples. Several IRI objects are several
edges; a triple that is already there is not added again.

A collection of literals, `( "a" "b" )`, is a list property named `<name>_list`
and is written back as a collection. A collection with IRIs or blank nodes in
it is stored as RDF spells it, a chain of blank nodes with `rdf_first` and
`rdf_rest` edges ending at `rdf:nil`.

### Types as Labels or Edges

`--rdf-types=labels` makes each `rdf:type` a label, and `--rdf-types=edges` an
edge `rdf_type` to the class's node. The default, `auto`, reads the file once
before importing and uses labels unless the classes it names and the labels the
database already has come to more than 1,000; the import says which it chose.
A class that is a blank node is always an edge.

### Statements About Triples

An annotation (`{| ... |}`) or a statement about a quoted triple
(`<< s p o >> q v .`) becomes a property of the edge `s p o`, which is created
if the file does not assert it. A literal value is stored as a property value
would be; an IRI value as the map `{"@id": "<iri>"}`. Refused, with a message
naming the triple: a quoted triple whose object is a literal (a property cannot
have properties), a quoted triple as an object, and a quoted triple inside
another.

## Errors and Atomicity

A record is one statement: one N-Triples line, or one Turtle statement up to
its `.` (a subject with all its predicates). Statements are committed in
batches: a batch ends with the statement that brings it to `--batch-size`
triples (default 1000), so a statement is never split between two
transactions. A statement that cannot be stored
fails the import, naming the triple and why; batches committed before it are
kept:

```text
Error: Import failed at line 4, column 29: <http://example.org/b> <http://example.org/count> "123456789012345678901234567890"^^<http://www.w3.org/2001/XMLSchema#integer>: 123456789012345678901234567890 does not fit in a 64-bit integer
Nothing was imported. Use --on-error=skip to import the statements that are valid.
```

With `--on-error=skip`, each statement is committed on its own, a failing one
is left out whole, with every other triple of that statement, and each is
listed. A syntax error in Turtle always stops the import, because after it
there is no telling where the next statement starts; with `--rdf-types=auto`
the whole file is read before anything is written, so a syntax error stops it
with nothing imported. In N-Triples, `--on-error=skip` goes on at the next line.

Importing a file again adds nothing that is already there, except its blank
nodes, which are new nodes each time.

## Exporting

```bash
lattice export animals.lattice --file=animals.ttl
lattice export animals.lattice --file=animals.nt
```

Turtle writes each node once, with its types, properties and edges grouped
under it (`;` between predicates, `,` between objects), and abbreviates IRIs
with the prefixes the database has. N-Triples writes one triple per line and is
the one to use for very large graphs and line-based tools. `--labels` and
`--query` choose what to export as for the other formats (see
[Data Export](./data-export.md)); with `--labels`, each node's outgoing edges
are written whether or not the node at the other end was chosen.

Nodes and names that came from RDF get their IRIs back. For a graph built in
LatticeDB:

- A label, edge type or property name that is not in the table is written
  under a base, `urn:lattice:` unless `--base` gives another, and the base is
  declared as the empty prefix, so `:Person` and `:name` read back as `Person`
  and `name`.
- A node without an IRI is a blank node, `_:n<id>`. With `--base`, it is the
  IRI `<base>node/<id>` instead, which other tools can refer to.
- Bytes are `xsd:base64Binary`, maps `rdf:JSON`, and a list one triple per
  element. Vectors are written when the database is opened with
  `--enable-vector` and its dimensions, as `"[...]"^^lattice:vector`;
  `--rdf-vectors=omit` leaves them out.
- Edge properties are RDF-star annotations. `--rdf-edge-properties=reify`
  writes an `rdf:Statement` for each edge that has properties instead, for
  tools that do not read RDF-star, and `--rdf-edge-properties=drop` leaves them
  out and says how many values it left out.

```bash
lattice export social.lattice --file=social.ttl --base=https://example.org/social/
```

## What Comes Back Changed

Turtle to a database and back gives the same RDF graph, compared as RDF
compares graphs (blank nodes matched up, numbers by value), except for:

- the way numbers and booleans are written: `+01` comes back as `1`, `1E3` as
  `1.0e3`, `"1"^^xsd:boolean` as `true`;
- a prefix, when the database already had another prefix for that namespace;
- statements about a triple the file did not assert: import asserts it;
- a collection with an IRI or blank node in it, which comes back as the
  `rdf:first`/`rdf:rest` triples it stands for rather than as `( ... )`;
- comments, order and layout.

A database to Turtle and back gives the same labels, properties and edges,
except for:

- node IDs, and the `_RdfName` and `_RdfNamespace` nodes the import adds;
- two edges of one type between the same two nodes, which are one triple in RDF
  and come back as one edge;
- a list of one element, which comes back as a single value, an empty list,
  which does not come back, and repeated elements of a list, which come back once;
- a list inside a list, which comes back as `rdf:JSON` text;
- vectors, which come back normalised to unit length under the cosine metric,
  as with every export;
- with `--base`, nodes come back labelled `Resource` with an `iri`;
- with `--rdf-edge-properties=reify`, edge properties come back as
  `rdf:Statement` nodes, not as properties of the edge.

## Other RDF Formats

RDF/XML, JSON-LD, TriG and N-Quads are not read or written. RDF/XML and JSON-LD
convert to Turtle or N-Triples in one step with
[rapper](https://librdf.org/raptor/rapper.html) or Apache Jena's
[riot](https://jena.apache.org/documentation/io/):

```bash
rapper -i rdfxml -o ntriples ontology.rdf > ontology.nt
riot --output=turtle data.jsonld > data.ttl
```

TriG and N-Quads carry named graphs, which have no place in the mapping: it
would take a graph name on every node and edge, as a property or a label, and an
export that groups by it. Converting to N-Triples drops the graph names.

SPARQL, inference (RDFS, OWL) and SHACL validation are not part of LatticeDB.
Query the graph with Cypher, and validate or reason with an RDF tool on the
export.

## Performance

Import and export stream: memory does not grow with the file. On a release
build, on a busy machine, with a graph of 100,000 people, each with a type, four
literal properties and five edges (1,000,000 triples):

| | Time | Peak memory |
|---|---:|---:|
| Import, N-Triples (107 MB) | 23.7 s, 42,000 triples/s | 46 MB |
| Import, Turtle (19 MB) | 23.8 s, 42,000 triples/s | 46 MB |
| Import, the same graph as CSV (100,000 nodes, 500,000 edges) | 11.2 s | 61 MB |
| Export, Turtle | 1.7 s | 45 MB |
| Export, N-Triples | 1.3 s | 45 MB |

Parsing alone takes 0.7 s for the N-Triples file and 0.2 s for the Turtle one,
in 2 MB of memory; the rest of an import is the database writing and
committing. CSV is faster for the same graph because one CSV row
sets all of a node's properties at once, where RDF has a triple for each.
