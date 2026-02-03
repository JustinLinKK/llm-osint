// 0002_indexes.cypher
// Optional performance indexes

CREATE INDEX idx_person_name IF NOT EXISTS
FOR (p:Person) ON (p.name);

CREATE INDEX idx_org_name IF NOT EXISTS
FOR (o:Organization) ON (o.name);

CREATE INDEX idx_location_country IF NOT EXISTS
FOR (l:Location) ON (l.country);

CREATE INDEX idx_location_name IF NOT EXISTS
FOR (l:Location) ON (l.name);

CREATE INDEX idx_article_title IF NOT EXISTS
FOR (a:Article) ON (a.title);
