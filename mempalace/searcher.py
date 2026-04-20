#!/usr/bin/env python3
"""
searcher.py — Find anything. Exact words.

Semantic search against the palace.
Returns verbatim text — the actual words, never summaries.
"""

from __future__ import annotations

import sys
from pathlib import Path

import chromadb

from .miner import _deterministic_embedding
from .structure_helpers import StructureManager
from .structure_store import StructureStore
from .tracing import absolute_lineage, local_lineage


class _StructureTraceResolver:
    def __init__(self, palace_path: str):
        self._manager = StructureManager(StructureStore.default_db_path(palace_path))
        self._cache = {}
        self._closed = False

    def close(self):
        if not self._closed:
            self._manager.close()
            self._closed = True

    def _render_breadcrumb(self, lineage_steps: list) -> str:
        return " > ".join(step.label for step in reversed(lineage_steps))

    def _gateway_crossings(self, absolute_steps: list) -> list[dict]:
        crossings = []
        for step in absolute_steps:
            if step.link_type == "gateway_domain_transition":
                crossings.append(
                    {
                        "gateway_node_id": step.node_id,
                        "domain_id": step.domain_id,
                        "label": step.label,
                        "flavor": step.flavor,
                    }
                )
        return crossings

    def resolve(self, meta: dict) -> dict:
        domain_id = meta.get("domain_id")
        container_node_id = meta.get("container_node_id")

        base = {
            "domain_id": domain_id,
            "container_node_id": container_node_id,
            "structured": bool(domain_id and container_node_id),
            "local_lineage": [],
            "absolute_lineage": [],
            "local_breadcrumb": None,
            "absolute_breadcrumb": None,
            "gateway_crossings": [],
        }

        if not base["structured"]:
            return base

        cache_key = (domain_id, container_node_id)
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            node = self._manager.store.get_node(container_node_id)
            if node is None or node.domain_id != domain_id:
                self._cache[cache_key] = base
                return base

            local_steps = local_lineage(self._manager.store, container_node_id)
            absolute_steps = absolute_lineage(self._manager.store, container_node_id)

            resolved = {
                **base,
                "local_lineage": [
                    {
                        "node_id": s.node_id,
                        "domain_id": s.domain_id,
                        "label": s.label,
                        "node_type": s.node_type,
                        "flavor": s.flavor,
                        "link_type": s.link_type,
                    }
                    for s in local_steps
                ],
                "absolute_lineage": [
                    {
                        "node_id": s.node_id,
                        "domain_id": s.domain_id,
                        "label": s.label,
                        "node_type": s.node_type,
                        "flavor": s.flavor,
                        "link_type": s.link_type,
                    }
                    for s in absolute_steps
                ],
                "local_breadcrumb": self._render_breadcrumb(local_steps),
                "absolute_breadcrumb": self._render_breadcrumb(absolute_steps),
                "gateway_crossings": self._gateway_crossings(absolute_steps),
            }
            self._cache[cache_key] = resolved
            return resolved
        except Exception:
            self._cache[cache_key] = base
            return base


def _query_collection(col, query: str, n_results: int, where: dict | None = None) -> dict:
    kwargs = {
        "n_results": n_results,
        "include": ["documents", "metadatas", "distances"],
    }
    if where:
        kwargs["where"] = where

    try:
        return col.query(query_embeddings=[_deterministic_embedding(query)], **kwargs)
    except Exception:
        return col.query(query_texts=[query], **kwargs)


def _build_where(wing: str | None, room: str | None) -> dict:
    if wing and room:
        return {"$and": [{"wing": wing}, {"room": room}]}
    if wing:
        return {"wing": wing}
    if room:
        return {"room": room}
    return {}


def _format_hit(doc: str, meta: dict, dist: float, resolver: _StructureTraceResolver | None) -> dict:
    structured = resolver.resolve(meta) if resolver else {
        "domain_id": meta.get("domain_id"),
        "container_node_id": meta.get("container_node_id"),
        "structured": False,
        "local_lineage": [],
        "absolute_lineage": [],
        "local_breadcrumb": None,
        "absolute_breadcrumb": None,
        "gateway_crossings": [],
    }

    return {
        "text": doc,
        "wing": meta.get("wing", "unknown"),
        "room": meta.get("room", "unknown"),
        "source_file": Path(meta.get("source_file", "?")).name,
        "similarity": round(1 - dist, 3),
        "domain_id": structured["domain_id"],
        "container_node_id": structured["container_node_id"],
        "local_lineage": structured["local_lineage"],
        "absolute_lineage": structured["absolute_lineage"],
        "local_breadcrumb": structured["local_breadcrumb"],
        "absolute_breadcrumb": structured["absolute_breadcrumb"],
        "gateway_crossings": structured["gateway_crossings"],
        "metadata": meta,
    }


def search(query: str, palace_path: str, wing: str = None, room: str = None, n_results: int = 5):
    """
    Search the palace. Returns verbatim drawer content.
    Optionally filter by wing (project) or room (aspect).
    """
    try:
        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_drawers")
    except Exception:
        print(f"\n  No palace found at {palace_path}")
        print("  Run: mempalace init <dir> then mempalace mine <dir>")
        sys.exit(1)

    where = _build_where(wing, room)

    try:
        results = _query_collection(col, query, n_results=n_results, where=where or None)
    except Exception as e:
        print(f"\n  Search error: {e}")
        sys.exit(1)

    docs = results["documents"][0]
    metas = results["metadatas"][0]
    dists = results["distances"][0]

    if not docs:
        print(f'\n  No results found for: "{query}"')
        return

    resolver = _StructureTraceResolver(palace_path)
    try:
        print(f"\n{'=' * 60}")
        print(f'  Results for: "{query}"')
        if wing:
            print(f"  Wing: {wing}")
        if room:
            print(f"  Room: {room}")
        print(f"{'=' * 60}\n")

        for i, (doc, meta, dist) in enumerate(zip(docs, metas, dists), 1):
            hit = _format_hit(doc, meta, dist, resolver)

            print(f"  [{i}] {hit['wing']} / {hit['room']}")
            print(f"      Source: {hit['source_file']}")
            print(f"      Match:  {hit['similarity']}")
            if hit["domain_id"] and hit["container_node_id"]:
                print(f"      Domain: {hit['domain_id']}")
                print(f"      Node:   {hit['container_node_id']}")
                if hit["absolute_breadcrumb"]:
                    print(f"      Path:   {hit['absolute_breadcrumb']}")
            print()
            for line in doc.strip().split("\n"):
                print(f"      {line}")
            print()
            print(f"  {'─' * 56}")

        print()
    finally:
        resolver.close()


def search_memories(
    query: str, palace_path: str, wing: str = None, room: str = None, n_results: int = 5
) -> dict:
    """
    Programmatic search — returns a dict instead of printing.
    Used by the MCP server and other callers that need data.
    """
    try:
        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_drawers")
    except Exception as e:
        return {"error": f"No palace found at {palace_path}: {e}"}

    where = _build_where(wing, room)

    try:
        results = _query_collection(col, query, n_results=n_results, where=where or None)
    except Exception as e:
        return {"error": f"Search error: {e}"}

    docs = results["documents"][0]
    metas = results["metadatas"][0]
    dists = results["distances"][0]

    resolver = _StructureTraceResolver(palace_path)
    try:
        hits = [_format_hit(doc, meta, dist, resolver) for doc, meta, dist in zip(docs, metas, dists)]
    finally:
        resolver.close()

    return {
        "query": query,
        "filters": {"wing": wing, "room": room},
        "results": hits,
    }
