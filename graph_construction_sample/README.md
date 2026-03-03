# KG Construction Pipeline

Async, cleanup-focused knowledge graph construction using:
- LLM extraction: `Qwen/Qwen3-32B`
- Embeddings: `qwen3-embed-0.6b`
- Endpoint: `https://localllm.frederickpi.com` (OpenAI-compatible)

## What it does

1. Extracts entities and relations from all articles in one stage.
2. Deduplicates exact matches first (normalized names).
3. Merges entities sequentially with hybrid retrieval:
   - top-k1 embedding cosine similarity (default `30`)
   - top-k2 string-overlap index similarity (default `30`)
4. Merges relations with the same strategy.
5. Supports merging new batches into an existing graph.

## Key infrastructure constraints implemented

- User prompt suffix: `\n \\no_think \n`
- Strips `<think>...</think>` before use
- JSON parsing retries up to 5
- Request retries with exponential backoff
- LLM semaphore concurrency default: `16`
- Embedding semaphore concurrency default: `16`
- Embedding batch size default: `64`

## Install

```bash
cd KG-construction
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Input format

Supports `.jsonl` or `.json`.

Each article should include a text field (default key: `text`, fallback keys include `content`, `article`, `body`, `summary`).

Example `.jsonl` line:

```json
{"id":"a1","text":"OpenAI announced ..."}
```

## Run

```bash
python kg_pipeline.py \
  --articles ./articles.jsonl \
  --output ./graph.json
```

This now writes:
- `graph.json` (readable graph structure, no vectors)
- `graph.embeddings.json` (node/edge embedding vectors)

Incremental merge into existing graph:

```bash
python kg_pipeline.py \
  --articles ./new_batch.jsonl \
  --existing-graph ./graph.json \
  --existing-embeddings ./graph.embeddings.json \
  --output ./graph_merged.json
```

Optional explicit embeddings output path:

```bash
python kg_pipeline.py \
  --articles ./articles.jsonl \
  --output ./graph.json \
  --embeddings-output ./graph_vectors.json
```

## Output schema

### Node fields
- `node_id` (string)
- `type` (string)
- `alt_names` (array of string)
- `created_at` (timestamp)
- `updated_at` (timestamp)
- `attributes` (array of string)
- `canonical_name` (string)

### Edge fields
- `edge_id` (string)
- `src_id` (string)
- `dst_id` (string)
- `rel_type` (string; optional/empty allowed)
- `created_at` (timestamp)
- `updated_at` (timestamp)
- `canonical_name` (string)
- `alt_names` (array of string)

### Embeddings file fields (`*.embeddings.json`)
- `node_embeddings`: array of `{node_id, embedding}`
- `edge_embeddings`: array of `{edge_id, embedding}`
- `metadata`: generation metadata and counts

## Interactive visualization (Dash Cytoscape)

Run:

```bash
python kg_dash_viewer.py --graph ./google100_graph.json
```

Then open:

```text
http://127.0.0.1:8050
```

Controls included:
- text search across canonical name / aliases / attributes
- node type and relation type filters
- minimum degree and node limit
- ego-network focus (center node + depth)
- layout switcher and edge label toggle


LLM Knowledge Graph Construction (with emphasis on cleanup) 260206

Entity Definition: 
Entities nodes schema: (1) node_id, string; (2) embedding, array of float; (3) type, string; (4) alt_names, array of string; (5) created_at, timestamp; (6) updated_at, timestamp; (7) attributes, array of string; (8) canonical name, string. 

Edges / relation schema: (1) edge_id, string; (2) src_id, string; (3) dst_id, string; (4) rel_type, string, optional; (5) created_at, timestamp; (6) updated_at, timestamp; (7) canonical name; (8) alt_names, array of string.  

Note: embedding are calculated via concatenating all alt_names / canonical names from the first time. This will not update. 

Ideally,  we shall adopt a 2 stage information extraction workflow for each.
The 1st stage, we extract entities only.
The 2nd stage, we extract relations (passing in the extracted entities). 
Both rely on LLM (we will try Qwen-32B for now).

Importantly, we are not keeping a closed category of entity / relation type. They are descriptive purpose, and will only be used for the disambiguation later. 

However, given consideration of economicness, we only do 1-stage, extracting both entities / relations at the same time. Note that LLM is not responsible for id / time etc metadata. 


Merging: 
We shall not do sequential merging (i.e. not extract article_A -> merge into KG -> extract article_B -> merge into KG -> extract article_C  -> merge ….) 

We shall do instead: extract all articles, get the entire  collection of entities / tuples, and then do KG construction from this collection. 

We will always start from deduplication first (need to preprocess the entity name (e.g. lower, remove punctuations, etc)).

Then for each entity, we will pick the top k1 (default k1 = 30)  similar entities based embedding cosine similarity, and k2 (default k2 = 30) based on string similarity (we need fast computation of string overlap, it is recommended that we maintain a simple search index). Be ware that we will need to preprocess the entity name (e.g. lower, remove punctuations, etc) before we calculate the string similarity. Because of this hybrid nature, it is recommended that we employ some ready to use frameworks (e.g. llamaindex or similar) for the purpose. 

We will go sequentially. For each entity (we call it query entity), we will have itself and its candidate. We will pass the entire entity information into the LLM context, and the LLM will decide which entities should be merged together (pick a canonical name). Note that we shall only let the LLM decide which candidate can be merged with the query entity. We will not decide whether the rest candidate entities could be merged together. We shall ask the LLM to pick a best name for the cluster (where query entity and selected candidates are aggregated), and creating a new node + remove the merged nodes, and the new node shall store all alt_names, and its embedding should be the average of all candidate embeddings, if any.

Relation merging works the same way. 

For new batches to be merged into existing database, we shall be using the same approach. 


