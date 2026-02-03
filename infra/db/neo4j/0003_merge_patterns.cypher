// 0003_merge_patterns.cypher
// Canonical merge patterns with lat/lon threshold for Location
// Expected parameters: location_id, name, country, lat, lon, threshold_meters

// Merge by distance threshold, otherwise create new
CALL {
  WITH $lat AS lat, $lon AS lon, $threshold_meters AS threshold
  MATCH (l:Location)
  WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
    AND distance(point({latitude: l.lat, longitude: l.lon}), point({latitude: lat, longitude: lon})) < threshold
  RETURN l AS loc
  LIMIT 1
  UNION
  WITH $location_id AS location_id
  MERGE (loc:Location {location_id: location_id})
  RETURN loc
}
SET loc.name = coalesce(loc.name, $name),
    loc.country = coalesce(loc.country, $country),
    loc.lat = coalesce(loc.lat, $lat),
    loc.lon = coalesce(loc.lon, $lon)
RETURN loc;

// Example usage for relationships
// MERGE (p:Person {person_id: $person_id})
// MERGE (loc:Location {plus_code: $plus_code})
// MERGE (p)-[:LOCATED_IN]->(loc);
