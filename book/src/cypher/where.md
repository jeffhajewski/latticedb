# WHERE and Filtering

`WHERE` filters matched patterns based on conditions.

## Comparison Operators

```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n.name
MATCH (n:Person) WHERE n.age >= 30 RETURN n.name
MATCH (n:Person) WHERE n.age < 30 RETURN n.name
MATCH (n:Person) WHERE n.age <= 30 RETURN n.name
MATCH (n:Person) WHERE n.name = "Alice" RETURN n
MATCH (n:Person) WHERE n.name <> "Alice" RETURN n
```

## Boolean Logic

Combine conditions with `AND`, `OR`, `NOT`, and `XOR`:

```cypher
MATCH (n:Person)
WHERE n.age > 25 AND n.field = "ML"
RETURN n.name

MATCH (n:Person)
WHERE n.field = "ML" OR n.field = "AI"
RETURN n.name

MATCH (n:Person)
WHERE NOT n.field = "Systems"
RETURN n.name
```

A predicate can be true, false or null: a comparison with a missing property is
null, and `NOT`, `AND` and `OR` follow three-valued logic. `WHERE` keeps a row only
when the predicate is true, so `WHERE n.email <> "bob@x.org"` skips people with no
email, and so does `WHERE NOT n.email = "bob@x.org"`. See
[Functions](./functions.md#null) for the full rules.

## Null Checks

```cypher
MATCH (n:Person) WHERE n.email IS NULL RETURN n.name
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name
```

## String Predicates

```cypher
MATCH (n:Person) WHERE n.name CONTAINS "li" RETURN n
MATCH (n:Person) WHERE n.name STARTS WITH "A" RETURN n
MATCH (n:Person) WHERE n.name ENDS WITH "ce" RETURN n
```

## Regular Expressions

`=~` matches a string against a regular expression. The whole string has to match,
so `"A.*"` finds names starting with "A" and `"li"` matches only the string "li":

```cypher
MATCH (n:Person) WHERE n.name =~ "A.*" RETURN n.name
MATCH (n:Person) WHERE n.name =~ "(?i)a.*" RETURN n.name
```

The supported syntax is a subset of Java's: character classes, `\d`, `\w`, `\s`
and their negations, groups, alternation, `*`, `+`, `?` and `{n,m}` (lazy forms are
accepted), the anchors `^`, `$`, `\A`, `\z`, `\b` and `\B`, and the flags `(?i)`
and `(?s)` at the start of the pattern. Matching follows UTF-8 characters, and its
time grows with the length of the text times the size of the pattern, whatever the
pattern, so a pattern taken from a parameter cannot make a query run away.
Backreferences, lookaround and possessive quantifiers fail the query rather than
being misread. A null on either side gives null.

## Property Existence

Test whether a property exists by checking for null:

```cypher
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n
```

## Inline Property Matching

Properties in the MATCH pattern act as equality filters:

```cypher
// These are equivalent:
MATCH (n:Person {name: "Alice"}) RETURN n
MATCH (n:Person) WHERE n.name = "Alice" RETURN n
```

## Vector Distance

Filter by vector similarity using `<=>`:

```cypher
MATCH (c:Chunk)
WHERE c.embedding <=> $query < 0.5
RETURN c.text
```

See [Vector Search](./vector-search.md).

## Full-Text Search

Filter by text content using `@@`:

```cypher
MATCH (d:Document)
WHERE d.content @@ "neural networks"
RETURN d.title
```

See [Full-Text Search](./full-text-search.md).
