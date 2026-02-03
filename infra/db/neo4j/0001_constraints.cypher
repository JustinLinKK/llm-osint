// 0001_constraints.cypher
// Core node constraints for OSINT graph schema

// PERSON — unique person identifier
CREATE CONSTRAINT person_unique_id IF NOT EXISTS
FOR (p:Person)
REQUIRE p.person_id IS UNIQUE;

// ORGANIZATION — unique identifier
CREATE CONSTRAINT org_unique_id IF NOT EXISTS
FOR (o:Organization)
REQUIRE o.org_id IS UNIQUE;

// LOCATION — unique location identifier
CREATE CONSTRAINT location_unique_id IF NOT EXISTS
FOR (l:Location)
REQUIRE l.location_id IS UNIQUE;

// DOMAIN — unique domain name
CREATE CONSTRAINT domain_unique_name IF NOT EXISTS
FOR (d:Domain)
REQUIRE d.name IS UNIQUE;

// EMAIL — unique email value
CREATE CONSTRAINT email_unique_addr IF NOT EXISTS
FOR (e:Email)
REQUIRE e.address IS UNIQUE;

// ARTICLE/SOURCE — unique URL or source identifier
CREATE CONSTRAINT article_unique_uri IF NOT EXISTS
FOR (a:Article)
REQUIRE a.uri IS UNIQUE;

// SNIPPET — unique snippet identifier
CREATE CONSTRAINT snippet_unique_id IF NOT EXISTS
FOR (s:Snippet)
REQUIRE s.snippet_id IS UNIQUE;
