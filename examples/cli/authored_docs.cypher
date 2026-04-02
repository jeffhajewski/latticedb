MATCH (person:Person)-[:AUTHORED]->(doc:Document)
RETURN person.name AS author, doc.title AS title, doc.year AS year
ORDER BY person.name
