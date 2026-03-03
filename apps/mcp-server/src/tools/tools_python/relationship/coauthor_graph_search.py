from __future__ import annotations

from typing import Any, Dict, List

from technical.common import clean_text


def _as_publication_list(input_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("publication_data", "publications", "records"):
        value = input_data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    person_name = str(input_data.get("person_name") or "").strip().lower()
    publications = _as_publication_list(input_data)
    coauthor_counts: Dict[str, int] = {}
    venue_counts: Dict[str, int] = {}
    pair_to_titles: Dict[str, List[str]] = {}
    graph_nodes: Dict[str, Dict[str, Any]] = {}
    graph_edges: Dict[str, Dict[str, Any]] = {}

    for item in publications:
        title = clean_text(item.get("title") or item.get("name") or "", max_len=200)
        author_values = item.get("authors") or item.get("coauthors") or item.get("author_names") or []
        authors: List[str] = []
        if isinstance(author_values, list):
            for author in author_values:
                if isinstance(author, str):
                    authors.append(author)
                elif isinstance(author, dict) and isinstance(author.get("name"), str):
                    authors.append(author["name"])
        for author in authors:
            normalized = author.strip()
            if not normalized or normalized.lower() == person_name:
                continue
            coauthor_counts[normalized] = coauthor_counts.get(normalized, 0) + 1
            graph_nodes[normalized] = {"id": normalized, "type": "Person", "label": normalized}
            graph_edges[f"person|COAUTHOR_OF|{normalized}"] = {
                "src": "person",
                "rel": "COAUTHOR_OF",
                "dst": normalized,
                "count": coauthor_counts[normalized],
            }
            if title:
                pair_to_titles.setdefault(normalized, []).append(title)

        for key in ("venue", "journal", "conference"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                venue = clean_text(value, max_len=140)
                venue_counts[venue] = venue_counts.get(venue, 0) + 1
                graph_nodes[venue] = {"id": venue, "type": "Venue", "label": venue}
                if title:
                    graph_nodes[title] = {"id": title, "type": "Paper", "label": title}
                    graph_edges[f"{title}|PUBLISHED_IN|{venue}"] = {"src": title, "rel": "PUBLISHED_IN", "dst": venue, "count": 1}
                break
        if title:
            graph_nodes[title] = {"id": title, "type": "Paper", "label": title}
            for author in authors:
                normalized = author.strip()
                if not normalized:
                    continue
                graph_nodes[normalized] = {"id": normalized, "type": "Person", "label": normalized}
                graph_edges[f"{normalized}|COAUTHOR_OF|{title}"] = {"src": normalized, "rel": "COAUTHOR_OF", "dst": title, "count": 1}

    coauthors = [
        {"name": name, "count": count}
        for name, count in sorted(coauthor_counts.items(), key=lambda item: (-item[1], item[0].lower()))
    ]
    shared_venues = [
        {"venue": venue, "count": count}
        for venue, count in sorted(venue_counts.items(), key=lambda item: (-item[1], item[0].lower()))
    ]
    clusters = []
    for index, (name, count) in enumerate(sorted(coauthor_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:6], start=1):
        clusters.append(
            {
                "label": f"Group {index}",
                "members": [name],
                "representative_works": pair_to_titles.get(name, [])[:3],
            }
        )
    return {
        "tool": "coauthor_graph_search",
        "coauthors": coauthors[:20],
        "shared_venues": shared_venues[:20],
        "collaborationGraph": {
            "nodes": list(graph_nodes.values())[:80],
            "edges": list(graph_edges.values())[:120],
        },
        "clusters": clusters,
    }
