// Most cited papers in the sample graph.
MATCH (citing:Paper)-[:CITES]->(cited:Paper)
RETURN cited.title, cited.year, count(citing) AS citations
ORDER BY citations DESC
LIMIT 10;
