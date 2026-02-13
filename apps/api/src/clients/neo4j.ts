import neo4j from "neo4j-driver";
import { cfg } from "../config.js";

export const neo4jDriver = neo4j.driver(
  cfg.neo4j.uri,
  neo4j.auth.basic(cfg.neo4j.user, cfg.neo4j.password)
);
