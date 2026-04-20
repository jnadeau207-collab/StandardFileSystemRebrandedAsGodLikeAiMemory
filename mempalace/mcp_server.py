#!/usr/bin/env python3
"""MemPalace MCP server with structure-aware tools and backward compatibility."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
from datetime import datetime

import chromadb

from . import __version__
from .closets import generate_closets, get_closet, list_closets
from .config import MempalaceConfig
from .dialect import Dialect
from .knowledge_graph import KnowledgeGraph
from .layers import MemoryStack
from .miner import _deterministic_embedding
from .palace_graph import find_tunnels, graph_stats, traverse
from .searcher import search_memories
from .structure_helpers import StructureManager
from .structure_store import StructureStore
from .tracing import absolute_lineage, local_lineage

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
logger = logging.getLogger("mempalace_mcp")

_config = MempalaceConfig()
_kg = KnowledgeGraph()


def _get_collection(create=False):
    try:
        client = chromadb.PersistentClient(path=_config.palace_path)
        if create:
            return client.get_or_create_collection(_config.collection_name)
        return client.get_collection(_config.collection_name)
    except Exception:
        return None


def _no_palace():
    return {
        "error": "No palace found",
        "palace_path": _config.palace_path,
        "hint": "Run: mempalace init <dir> && mempalace mine <dir>",
    }


def _structure_manager() -> StructureManager:
    return StructureManager(_config.structure_db_path)


def _id_error(field: str, value: str, expected_prefix: str) -> dict:
    return {
        "error": "invalid_id",
        "field": field,
        "value": value,
        "expected_prefix": expected_prefix,
    }


def _validate_id(value: str, prefix: str) -> bool:
    return bool(re.match(rf"^{prefix}[a-f0-9]+$", value or ""))


def _lineage_payload(store: StructureStore, node_id: str) -> dict:
    local_steps = local_lineage(store, node_id)
    absolute_steps = absolute_lineage(store, node_id)

    local = [
        {
            "domain_id": s.domain_id,
            "node_id": s.node_id,
            "label": s.label,
            "node_type": s.node_type,
            "flavor": s.flavor,
            "link_type": s.link_type,
        }
        for s in local_steps
    ]
    absolute = [
        {
            "domain_id": s.domain_id,
            "node_id": s.node_id,
            "label": s.label,
            "node_type": s.node_type,
            "flavor": s.flavor,
            "link_type": s.link_type,
        }
        for s in absolute_steps
    ]

    breadcrumb_local = " > ".join(step["label"] for step in reversed(local))
    breadcrumb_absolute = " > ".join(step["label"] for step in reversed(absolute))
    domain_chain = []
    for step in reversed(absolute):
        if step["domain_id"] not in domain_chain:
            domain_chain.append(step["domain_id"])

    crossings = [
        {
            "domain_id": step["domain_id"],
            "gateway_node_id": step["node_id"],
            "label": step["label"],
            "flavor": step["flavor"],
        }
        for step in absolute
        if step["link_type"] == "gateway_domain_transition"
    ]

    node = store.get_node(node_id)
    return {
        "domain_id": node.domain_id,
        "container_node_id": node.node_id,
        "local_lineage": local,
        "absolute_lineage": absolute,
        "local_breadcrumb": breadcrumb_local,
        "absolute_breadcrumb": breadcrumb_absolute,
        "domain_chain": domain_chain,
        "gateway_crossings": crossings,
    }


def _resolve_node_candidates(store: StructureStore, label: str, domain_id: str = None, node_type: str = None, parent_node_id: str = None) -> list[dict]:
    sql = "SELECT node_id, domain_id, node_type, label, parent_node_id, flavor FROM nodes WHERE label = ?"
    params = [label]
    if domain_id:
        sql += " AND domain_id = ?"
        params.append(domain_id)
    if node_type:
        sql += " AND node_type = ?"
        params.append(node_type)
    if parent_node_id:
        sql += " AND parent_node_id = ?"
        params.append(parent_node_id)
    rows = store.conn.execute(sql, tuple(params)).fetchall()
    return [dict(row) for row in rows]


# ==================== READ TOOLS ====================
def tool_status():
    col = _get_collection()
    if not col:
        return _no_palace()
    wings, rooms, structured_domains = {}, {}, set()
    structured_count = 0
    all_meta = col.get(include=["metadatas"]).get("metadatas", [])
    for m in all_meta:
        w = m.get("wing", "unknown")
        r = m.get("room", "unknown")
        wings[w] = wings.get(w, 0) + 1
        rooms[r] = rooms.get(r, 0) + 1
        if m.get("domain_id") and m.get("container_node_id"):
            structured_count += 1
            structured_domains.add(m["domain_id"])
    return {
        "total_drawers": len(all_meta),
        "structured_drawers": structured_count,
        "unstructured_drawers": len(all_meta) - structured_count,
        "structured_domains": sorted(structured_domains),
        "wings": wings,
        "rooms": rooms,
        "palace_path": _config.palace_path,
        "structure_db_path": _config.structure_db_path,
        "protocol": PALACE_PROTOCOL,
        "aaak_dialect": AAAK_SPEC,
    }


def tool_list_wings():
    col = _get_collection()
    if not col:
        return _no_palace()
    wings = {}
    for m in col.get(include=["metadatas"]).get("metadatas", []):
        w = m.get("wing", "unknown")
        wings[w] = wings.get(w, 0) + 1
    return {"wings": wings}


def tool_list_rooms(wing: str = None):
    col = _get_collection()
    if not col:
        return _no_palace()
    rooms = {}
    kwargs = {"include": ["metadatas"]}
    if wing:
        kwargs["where"] = {"wing": wing}
    for m in col.get(**kwargs).get("metadatas", []):
        r = m.get("room", "unknown")
        rooms[r] = rooms.get(r, 0) + 1
    return {"wing": wing or "all", "rooms": rooms}


def tool_get_taxonomy():
    col = _get_collection()
    if not col:
        return _no_palace()
    taxonomy = {}
    for m in col.get(include=["metadatas"]).get("metadatas", []):
        w = m.get("wing", "unknown")
        r = m.get("room", "unknown")
        taxonomy.setdefault(w, {})
        taxonomy[w][r] = taxonomy[w].get(r, 0) + 1
    return {"taxonomy": taxonomy}


def tool_search(query: str, limit: int = 5, wing: str = None, room: str = None):
    return search_memories(query, palace_path=_config.palace_path, wing=wing, room=room, n_results=limit)


def tool_check_duplicate(content: str, threshold: float = 0.9):
    col = _get_collection()
    if not col:
        return _no_palace()
    try:
        results = col.query(
            query_embeddings=[_deterministic_embedding(content)],
            n_results=5,
            include=["metadatas", "documents", "distances"],
        )
    except Exception:
        results = col.query(
            query_texts=[content], n_results=5, include=["metadatas", "documents", "distances"]
        )

    duplicates = []
    if results.get("ids") and results["ids"][0]:
        for i, drawer_id in enumerate(results["ids"][0]):
            similarity = round(1 - results["distances"][0][i], 3)
            if similarity < threshold:
                continue
            meta = results["metadatas"][0][i]
            duplicates.append(
                {
                    "id": drawer_id,
                    "wing": meta.get("wing", "?"),
                    "room": meta.get("room", "?"),
                    "domain_id": meta.get("domain_id"),
                    "container_node_id": meta.get("container_node_id"),
                    "similarity": similarity,
                }
            )
    return {"is_duplicate": len(duplicates) > 0, "matches": duplicates}


def tool_get_aaak_spec():
    return {"aaak_spec": AAAK_SPEC}


def tool_traverse_graph(start_room: str, max_hops: int = 2):
    col = _get_collection()
    if not col:
        return _no_palace()
    return traverse(start_room, col=col, config=_config, max_hops=max_hops)


def tool_find_tunnels(wing_a: str = None, wing_b: str = None):
    col = _get_collection()
    if not col:
        return _no_palace()
    return find_tunnels(wing_a, wing_b, col=col, config=_config)


def tool_graph_stats():
    col = _get_collection()
    if not col:
        return _no_palace()
    return graph_stats(col=col, config=_config)


# ==================== STRUCTURE TOOLS ====================
def tool_structure_trace_node(node_id: str):
    if not _validate_id(node_id, "node_"):
        return _id_error("node_id", node_id, "node_")
    manager = _structure_manager()
    try:
        node = manager.store.get_node(node_id)
        if node is None:
            return {"error": "not_found", "node_id": node_id}
        payload = _lineage_payload(manager.store, node_id)
        payload["node_type"] = node.node_type
        payload["label"] = node.label
        payload["flavor"] = node.flavor
        return payload
    finally:
        manager.close()


def tool_structure_trace_drawer(drawer_id: str = None, memory_id: str = None):
    drawer_id = drawer_id or memory_id
    if not drawer_id:
        return {"error": "missing_id", "message": "Provide drawer_id or memory_id"}

    col = _get_collection()
    if not col:
        return _no_palace()

    existing = col.get(ids=[drawer_id], include=["metadatas", "documents"])
    if not existing.get("ids"):
        return {"error": "not_found", "drawer_id": drawer_id}

    meta = existing["metadatas"][0]
    result = {
        "drawer_id": drawer_id,
        "wing": meta.get("wing"),
        "room": meta.get("room"),
        "domain_id": meta.get("domain_id"),
        "container_node_id": meta.get("container_node_id"),
    }

    if meta.get("domain_id") and meta.get("container_node_id"):
        trace = tool_structure_trace_node(meta["container_node_id"])
        if "error" not in trace:
            result.update(trace)
    return result


def tool_structure_validate():
    manager = _structure_manager()
    try:
        errors = []
        domain_rows = manager.store.conn.execute("SELECT * FROM domains").fetchall()
        node_rows = manager.store.conn.execute("SELECT * FROM nodes").fetchall()

        for d in domain_rows:
            roots = manager.store.conn.execute(
                "SELECT COUNT(1) as c FROM nodes WHERE domain_id = ? AND is_root = 1", (d["domain_id"],)
            ).fetchone()["c"]
            if roots != 1:
                errors.append({"type": "domain_root_count", "domain_id": d["domain_id"], "count": roots})
            if d["parent_domain_id"] and not d["entry_gateway_id"]:
                errors.append({"type": "missing_entry_gateway", "domain_id": d["domain_id"]})

        for n in node_rows:
            if n["parent_node_id"]:
                parent = manager.store.get_node(n["parent_node_id"])
                if parent is None or parent.domain_id != n["domain_id"]:
                    errors.append({"type": "invalid_parent", "node_id": n["node_id"]})

        for n in node_rows:
            try:
                absolute_lineage(manager.store, n["node_id"])
            except Exception as exc:
                errors.append({"type": "trace_failure", "node_id": n["node_id"], "error": str(exc)})

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "domain_count": len(domain_rows),
            "node_count": len(node_rows),
        }
    finally:
        manager.close()


def tool_structure_resolve(
    node_id: str = None,
    wing: str = None,
    room: str = None,
    domain_id: str = None,
    label: str = None,
    node_type: str = None,
    parent_node_id: str = None,
):
    manager = _structure_manager()
    try:
        if node_id:
            if not _validate_id(node_id, "node_"):
                return _id_error("node_id", node_id, "node_")
            node = manager.store.get_node(node_id)
            if node is None:
                return {"error": "not_found", "node_id": node_id}
            return {"resolved": True, "node_id": node.node_id, "domain_id": node.domain_id, "label": node.label, "node_type": node.node_type}

        if wing and room:
            if domain_id:
                if not _validate_id(domain_id, "dom_"):
                    return _id_error("domain_id", domain_id, "dom_")
                base_domain = manager.store.get_domain(domain_id)
                if base_domain is None:
                    return {"error": "not_found", "domain_id": domain_id}
                base_root = manager.store.get_root_node(domain_id)
                if base_root is None:
                    return {"error": "invalid_structure", "message": "Domain has no root node", "domain_id": domain_id}
            else:
                base_domain, base_root = manager.store.ensure_main_domain()
            wing_nodes = _resolve_node_candidates(manager.store, wing, domain_id=base_domain.domain_id, node_type="wing", parent_node_id=base_root.node_id)
            if len(wing_nodes) == 0:
                return {"error": "not_found", "wing": wing}
            if len(wing_nodes) > 1:
                return {"error": "ambiguous", "kind": "wing", "candidates": wing_nodes}
            room_nodes = _resolve_node_candidates(manager.store, room, domain_id=base_domain.domain_id, node_type="room", parent_node_id=wing_nodes[0]["node_id"])
            if len(room_nodes) == 0:
                return {"error": "not_found", "room": room, "wing": wing}
            if len(room_nodes) > 1:
                return {"error": "ambiguous", "kind": "room", "candidates": room_nodes}
            return {"resolved": True, "domain_id": base_domain.domain_id, "wing_node_id": wing_nodes[0]["node_id"], "container_node_id": room_nodes[0]["node_id"]}

        if label:
            candidates = _resolve_node_candidates(manager.store, label, domain_id=domain_id, node_type=node_type, parent_node_id=parent_node_id)
            if not candidates:
                return {"error": "not_found", "label": label}
            if len(candidates) > 1:
                return {"error": "ambiguous", "label": label, "candidates": candidates}
            return {"resolved": True, **candidates[0]}

        return {"error": "missing_resolution_input"}
    finally:
        manager.close()


def tool_structure_list_children(domain_id: str = None, node_id: str = None):
    manager = _structure_manager()
    try:
        if node_id:
            if not _validate_id(node_id, "node_"):
                return _id_error("node_id", node_id, "node_")
            parent = manager.store.get_node(node_id)
            if parent is None:
                return {"error": "not_found", "node_id": node_id}
            rows = manager.store.conn.execute(
                "SELECT node_id, domain_id, node_type, label, flavor FROM nodes WHERE parent_node_id = ? ORDER BY label",
                (node_id,),
            ).fetchall()
            return {"parent_node_id": node_id, "children": [dict(r) for r in rows]}

        if domain_id:
            if not _validate_id(domain_id, "dom_"):
                return _id_error("domain_id", domain_id, "dom_")
            root = manager.store.get_root_node(domain_id)
            if root is None:
                return {"error": "not_found", "domain_id": domain_id}
            rows = manager.store.conn.execute(
                "SELECT node_id, domain_id, node_type, label, flavor FROM nodes WHERE parent_node_id = ? ORDER BY label",
                (root.node_id,),
            ).fetchall()
            return {"domain_id": domain_id, "root_node_id": root.node_id, "children": [dict(r) for r in rows]}

        return {"error": "missing_scope", "message": "Provide domain_id or node_id"}
    finally:
        manager.close()


def tool_structure_create_gateway_anchor(domain_id: str, parent_node_id: str, label: str, flavor: str = None):
    if not _validate_id(domain_id, "dom_"):
        return _id_error("domain_id", domain_id, "dom_")
    if not _validate_id(parent_node_id, "node_"):
        return _id_error("parent_node_id", parent_node_id, "node_")
    manager = _structure_manager()
    try:
        return manager.create_gateway_anchor(domain_id=domain_id, parent_node_id=parent_node_id, label=label, flavor=flavor)
    except Exception as exc:
        return {"error": "creation_failed", "message": str(exc)}
    finally:
        manager.close()


def tool_structure_create_subdomain(parent_domain_id: str, entry_gateway_id: str, label: str):
    if not _validate_id(parent_domain_id, "dom_"):
        return _id_error("parent_domain_id", parent_domain_id, "dom_")
    if not _validate_id(entry_gateway_id, "gate_"):
        return _id_error("entry_gateway_id", entry_gateway_id, "gate_")
    manager = _structure_manager()
    try:
        return manager.create_subordinate_domain(parent_domain_id=parent_domain_id, entry_gateway_id=entry_gateway_id, label=label)
    except Exception as exc:
        return {"error": "creation_failed", "message": str(exc)}
    finally:
        manager.close()


def tool_structure_create_nested_subdomain(
    parent_domain_id: str,
    parent_node_id: str,
    gateway_label: str,
    subdomain_label: str,
    flavor: str = None,
):
    if not _validate_id(parent_domain_id, "dom_"):
        return _id_error("parent_domain_id", parent_domain_id, "dom_")
    if not _validate_id(parent_node_id, "node_"):
        return _id_error("parent_node_id", parent_node_id, "node_")
    manager = _structure_manager()
    try:
        return manager.create_nested_subordinate_domain(
            parent_domain_id=parent_domain_id,
            parent_node_id=parent_node_id,
            gateway_label=gateway_label,
            subdomain_label=subdomain_label,
            flavor=flavor,
        )
    except Exception as exc:
        return {"error": "creation_failed", "message": str(exc)}
    finally:
        manager.close()


# ==================== WRITE TOOLS ====================
def tool_add_drawer(
    wing: str,
    room: str,
    content: str,
    source_file: str = None,
    added_by: str = "mcp",
    domain_id: str = None,
    container_node_id: str = None,
):
    col = _get_collection(create=True)
    if not col:
        return _no_palace()

    dup = tool_check_duplicate(content, threshold=0.9)
    if dup.get("is_duplicate"):
        return {"success": False, "reason": "duplicate", "matches": dup["matches"]}

    placement = None
    manager = _structure_manager()
    try:
        if domain_id and container_node_id:
            placement = manager.file_drawer_to_node(domain_id=domain_id, container_node_id=container_node_id)
        else:
            placement = manager.resolve_ordinary_container(wing=wing, room=room)
    finally:
        manager.close()

    drawer_id = f"drawer_{wing}_{room}_{hashlib.md5((content[:100] + datetime.now().isoformat()).encode()).hexdigest()[:16]}"

    try:
        col.add(
            ids=[drawer_id],
            documents=[content],
            embeddings=[_deterministic_embedding(content)],
            metadatas=[
                {
                    "wing": wing,
                    "room": room,
                    "source_file": source_file or "",
                    "chunk_index": 0,
                    "domain_id": placement["domain_id"],
                    "container_node_id": placement["container_node_id"],
                    "added_by": added_by,
                    "filed_at": datetime.now().isoformat(),
                }
            ],
        )
        return {
            "success": True,
            "drawer_id": drawer_id,
            "wing": wing,
            "room": room,
            "domain_id": placement["domain_id"],
            "container_node_id": placement["container_node_id"],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def tool_delete_drawer(drawer_id: str):
    col = _get_collection()
    if not col:
        return _no_palace()
    existing = col.get(ids=[drawer_id], include=["metadatas"])
    if not existing.get("ids"):
        return {"success": False, "error": f"Drawer not found: {drawer_id}"}
    meta = existing["metadatas"][0]
    try:
        col.delete(ids=[drawer_id])
        return {
            "success": True,
            "drawer_id": drawer_id,
            "domain_id": meta.get("domain_id"),
            "container_node_id": meta.get("container_node_id"),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ==================== KNOWLEDGE GRAPH ====================
def tool_kg_query(entity: str, as_of: str = None, direction: str = "both"):
    results = _kg.query_entity(entity, as_of=as_of, direction=direction)
    return {"entity": entity, "as_of": as_of, "facts": results, "count": len(results)}


def tool_kg_add(subject: str, predicate: str, object: str, valid_from: str = None, source_closet: str = None):
    triple_id = _kg.add_triple(subject, predicate, object, valid_from=valid_from, source_closet=source_closet)
    return {"success": True, "triple_id": triple_id, "fact": f"{subject} → {predicate} → {object}"}


def tool_kg_invalidate(subject: str, predicate: str, object: str, ended: str = None):
    _kg.invalidate(subject, predicate, object, ended=ended)
    return {"success": True, "fact": f"{subject} → {predicate} → {object}", "ended": ended or "today"}


def tool_kg_timeline(entity: str = None):
    results = _kg.timeline(entity)
    return {"entity": entity or "all", "timeline": results, "count": len(results)}


def tool_kg_stats():
    return _kg.stats()


def tool_contradiction_check(subject: str, predicate: str, object: str):
    return _kg.check_contradiction(subject, predicate, object)


def tool_kg_add_safe(subject: str, predicate: str, object: str, valid_from: str = None, source_closet: str = None, auto_resolve: bool = True):
    result = _kg.add_triple_with_contradiction_check(subject, predicate, object, valid_from=valid_from, source_closet=source_closet, auto_resolve=auto_resolve)
    return {
        "success": True,
        "triple_id": result["triple_id"],
        "fact": f"{subject} → {predicate} → {object}",
        "contradictions_found": len(result["contradictions"]),
        "contradictions": result["contradictions"],
        "auto_resolved": result["auto_resolved"],
    }


# ==================== Diary / Layers / Closets ====================
def tool_diary_write(agent_name: str, entry: str, topic: str = "general"):
    wing = f"wing_{agent_name.lower().replace(' ', '_')}"
    room = "diary"
    col = _get_collection(create=True)
    if not col:
        return _no_palace()
    now = datetime.now()
    entry_id = f"diary_{wing}_{now.strftime('%Y%m%d_%H%M%S')}_{hashlib.md5(entry[:50].encode()).hexdigest()[:8]}"
    col.add(
        ids=[entry_id],
        documents=[entry],
        embeddings=[_deterministic_embedding(entry)],
        metadatas=[
            {
                "wing": wing,
                "room": room,
                "hall": "hall_diary",
                "topic": topic,
                "type": "diary_entry",
                "agent": agent_name,
                "filed_at": now.isoformat(),
                "date": now.strftime("%Y-%m-%d"),
            }
        ],
    )
    return {"success": True, "entry_id": entry_id, "agent": agent_name, "topic": topic, "timestamp": now.isoformat()}


def tool_diary_read(agent_name: str, last_n: int = 10):
    wing = f"wing_{agent_name.lower().replace(' ', '_')}"
    col = _get_collection()
    if not col:
        return _no_palace()
    results = col.get(where={"$and": [{"wing": wing}, {"room": "diary"}]}, include=["documents", "metadatas"])
    entries = [
        {
            "date": m.get("date", ""),
            "timestamp": m.get("filed_at", ""),
            "topic": m.get("topic", ""),
            "content": d,
        }
        for d, m in zip(results.get("documents", []), results.get("metadatas", []))
    ]
    entries.sort(key=lambda x: x["timestamp"], reverse=True)
    return {"agent": agent_name, "entries": entries[:last_n], "total": len(entries), "showing": min(last_n, len(entries))}


def tool_wake_up(wing: str = None):
    text = MemoryStack(palace_path=_config.palace_path).wake_up(wing=wing)
    return {"wake_up_text": text, "estimated_tokens": len(text) // 4}


def tool_recall(wing: str = None, room: str = None, n_results: int = 10):
    text = MemoryStack(palace_path=_config.palace_path).recall(wing=wing, room=room, n_results=n_results)
    return {"recall_text": text, "wing": wing, "room": room}


def tool_compress_text(text: str, wing: str = None, room: str = None):
    dialect = Dialect()
    compressed = dialect.compress(text, metadata={"wing": wing or "", "room": room or ""})
    stats = dialect.compression_stats(text, compressed)
    return {
        "compressed": compressed,
        "original_tokens": stats["original_tokens"],
        "compressed_tokens": stats["compressed_tokens"],
        "ratio": round(stats["ratio"], 1),
    }


def tool_generate_closets(wing: str = None):
    closets = generate_closets(_config.palace_path, wing=wing)
    return {"closets_generated": len(closets), "rooms": list(closets.keys())}


def tool_get_closet(wing: str = None, room: str = None):
    closets = get_closet(_config.palace_path, wing=wing, room=room)
    if not closets:
        return {"error": "No closets found. Run mempalace_generate_closets first."}
    return {"closets": closets, "count": len(closets)}


def tool_list_closets():
    closets = list_closets(_config.palace_path)
    return {"closets": closets, "count": len(closets)}


# ── static content
PALACE_PROTOCOL = """IMPORTANT — MemPalace Memory Protocol:
1. ON WAKE-UP: Call mempalace_status to load palace overview + AAAK spec.
2. BEFORE RESPONDING about any person, project, or past event: call mempalace_kg_query or mempalace_search FIRST. Never guess — verify.
3. IF UNSURE about a fact (name, gender, age, relationship): say \"let me check\" and query the palace. Wrong is worse than slow.
4. AFTER EACH SESSION: call mempalace_diary_write to record what happened, what you learned, what matters.
5. WHEN FACTS CHANGE: call mempalace_kg_invalidate on the old fact, mempalace_kg_add for the new one.

This protocol ensures the AI KNOWS before it speaks. Storage is not memory — but storage + this protocol = memory."""

AAAK_SPEC = """AAAK is a compressed memory dialect that MemPalace uses for efficient storage.
It is designed to be readable by both humans and LLMs without decoding."""


TOOLS = {
    "mempalace_status": {"description": "Palace overview", "input_schema": {"type": "object", "properties": {}}, "handler": tool_status},
    "mempalace_list_wings": {"description": "List wings", "input_schema": {"type": "object", "properties": {}}, "handler": tool_list_wings},
    "mempalace_list_rooms": {"description": "List rooms", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}}}, "handler": tool_list_rooms},
    "mempalace_get_taxonomy": {"description": "Wing/room taxonomy", "input_schema": {"type": "object", "properties": {}}, "handler": tool_get_taxonomy},
    "mempalace_search": {"description": "Semantic search", "input_schema": {"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}, "wing": {"type": "string"}, "room": {"type": "string"}}, "required": ["query"]}, "handler": tool_search},
    "mempalace_check_duplicate": {"description": "Check duplicate", "input_schema": {"type": "object", "properties": {"content": {"type": "string"}, "threshold": {"type": "number"}}, "required": ["content"]}, "handler": tool_check_duplicate},
    "mempalace_traverse": {"description": "Traverse graph", "input_schema": {"type": "object", "properties": {"start_room": {"type": "string"}, "max_hops": {"type": "integer"}}, "required": ["start_room"]}, "handler": tool_traverse_graph},
    "mempalace_find_tunnels": {"description": "Find tunnels", "input_schema": {"type": "object", "properties": {"wing_a": {"type": "string"}, "wing_b": {"type": "string"}}}, "handler": tool_find_tunnels},
    "mempalace_graph_stats": {"description": "Graph stats", "input_schema": {"type": "object", "properties": {}}, "handler": tool_graph_stats},
    "mempalace_add_drawer": {"description": "Add drawer", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}, "room": {"type": "string"}, "content": {"type": "string"}, "source_file": {"type": "string"}, "added_by": {"type": "string"}, "domain_id": {"type": "string"}, "container_node_id": {"type": "string"}}, "required": ["wing", "room", "content"]}, "handler": tool_add_drawer},
    "mempalace_delete_drawer": {"description": "Delete drawer", "input_schema": {"type": "object", "properties": {"drawer_id": {"type": "string"}}, "required": ["drawer_id"]}, "handler": tool_delete_drawer},
    "mempalace_structure_trace_node": {"description": "Trace canonical node", "input_schema": {"type": "object", "properties": {"node_id": {"type": "string"}}, "required": ["node_id"]}, "handler": tool_structure_trace_node},
    "mempalace_structure_trace_drawer": {"description": "Trace drawer by id", "input_schema": {"type": "object", "properties": {"drawer_id": {"type": "string"}, "memory_id": {"type": "string"}}}, "handler": tool_structure_trace_drawer},
    "mempalace_structure_validate": {"description": "Validate structure", "input_schema": {"type": "object", "properties": {}}, "handler": tool_structure_validate},
    "mempalace_structure_resolve": {"description": "Resolve labels to canonical ids", "input_schema": {"type": "object", "properties": {"node_id": {"type": "string"}, "wing": {"type": "string"}, "room": {"type": "string"}, "domain_id": {"type": "string"}, "label": {"type": "string"}, "node_type": {"type": "string"}, "parent_node_id": {"type": "string"}}}, "handler": tool_structure_resolve},
    "mempalace_structure_list_children": {"description": "List canonical children", "input_schema": {"type": "object", "properties": {"domain_id": {"type": "string"}, "node_id": {"type": "string"}}}, "handler": tool_structure_list_children},
    "mempalace_structure_create_gateway_anchor": {"description": "Create gateway anchor", "input_schema": {"type": "object", "properties": {"domain_id": {"type": "string"}, "parent_node_id": {"type": "string"}, "label": {"type": "string"}, "flavor": {"type": "string"}}, "required": ["domain_id", "parent_node_id", "label"]}, "handler": tool_structure_create_gateway_anchor},
    "mempalace_structure_create_subdomain": {"description": "Create subordinate domain", "input_schema": {"type": "object", "properties": {"parent_domain_id": {"type": "string"}, "entry_gateway_id": {"type": "string"}, "label": {"type": "string"}}, "required": ["parent_domain_id", "entry_gateway_id", "label"]}, "handler": tool_structure_create_subdomain},
    "mempalace_structure_create_nested_subdomain": {"description": "Create nested subordinate domain", "input_schema": {"type": "object", "properties": {"parent_domain_id": {"type": "string"}, "parent_node_id": {"type": "string"}, "gateway_label": {"type": "string"}, "subdomain_label": {"type": "string"}, "flavor": {"type": "string"}}, "required": ["parent_domain_id", "parent_node_id", "gateway_label", "subdomain_label"]}, "handler": tool_structure_create_nested_subdomain},
    "mempalace_get_aaak_spec": {"description": "AAAK spec", "input_schema": {"type": "object", "properties": {}}, "handler": tool_get_aaak_spec},
    "mempalace_kg_query": {"description": "KG query", "input_schema": {"type": "object", "properties": {"entity": {"type": "string"}, "as_of": {"type": "string"}, "direction": {"type": "string"}}, "required": ["entity"]}, "handler": tool_kg_query},
    "mempalace_kg_add": {"description": "KG add", "input_schema": {"type": "object", "properties": {"subject": {"type": "string"}, "predicate": {"type": "string"}, "object": {"type": "string"}, "valid_from": {"type": "string"}, "source_closet": {"type": "string"}}, "required": ["subject", "predicate", "object"]}, "handler": tool_kg_add},
    "mempalace_kg_invalidate": {"description": "KG invalidate", "input_schema": {"type": "object", "properties": {"subject": {"type": "string"}, "predicate": {"type": "string"}, "object": {"type": "string"}, "ended": {"type": "string"}}, "required": ["subject", "predicate", "object"]}, "handler": tool_kg_invalidate},
    "mempalace_kg_timeline": {"description": "KG timeline", "input_schema": {"type": "object", "properties": {"entity": {"type": "string"}}}, "handler": tool_kg_timeline},
    "mempalace_kg_stats": {"description": "KG stats", "input_schema": {"type": "object", "properties": {}}, "handler": tool_kg_stats},
    "mempalace_contradiction_check": {"description": "KG contradiction check", "input_schema": {"type": "object", "properties": {"subject": {"type": "string"}, "predicate": {"type": "string"}, "object": {"type": "string"}}, "required": ["subject", "predicate", "object"]}, "handler": tool_contradiction_check},
    "mempalace_kg_add_safe": {"description": "KG add safe", "input_schema": {"type": "object", "properties": {"subject": {"type": "string"}, "predicate": {"type": "string"}, "object": {"type": "string"}, "valid_from": {"type": "string"}, "source_closet": {"type": "string"}, "auto_resolve": {"type": "boolean"}}, "required": ["subject", "predicate", "object"]}, "handler": tool_kg_add_safe},
    "mempalace_diary_write": {"description": "Write diary", "input_schema": {"type": "object", "properties": {"agent_name": {"type": "string"}, "entry": {"type": "string"}, "topic": {"type": "string"}}, "required": ["agent_name", "entry"]}, "handler": tool_diary_write},
    "mempalace_diary_read": {"description": "Read diary", "input_schema": {"type": "object", "properties": {"agent_name": {"type": "string"}, "last_n": {"type": "integer"}}, "required": ["agent_name"]}, "handler": tool_diary_read},
    "mempalace_wake_up": {"description": "Wake up context", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}}}, "handler": tool_wake_up},
    "mempalace_recall": {"description": "Recall context", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}, "room": {"type": "string"}, "n_results": {"type": "integer"}}}, "handler": tool_recall},
    "mempalace_compress": {"description": "Compress text", "input_schema": {"type": "object", "properties": {"text": {"type": "string"}, "wing": {"type": "string"}, "room": {"type": "string"}}, "required": ["text"]}, "handler": tool_compress_text},
    "mempalace_generate_closets": {"description": "Generate closets", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}}}, "handler": tool_generate_closets},
    "mempalace_get_closet": {"description": "Get closet", "input_schema": {"type": "object", "properties": {"wing": {"type": "string"}, "room": {"type": "string"}}}, "handler": tool_get_closet},
    "mempalace_list_closets": {"description": "List closets", "input_schema": {"type": "object", "properties": {}}, "handler": tool_list_closets},
}


def handle_request(request):
    method = request.get("method", "")
    params = request.get("params", {})
    req_id = request.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "mempalace", "version": __version__},
            },
        }
    if method == "notifications/initialized":
        return None
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {"name": n, "description": t["description"], "inputSchema": t["input_schema"]}
                    for n, t in TOOLS.items()
                ]
            },
        }
    if method == "tools/call":
        tool_name = params.get("name")
        tool_args = params.get("arguments", {})
        if tool_name not in TOOLS:
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}}
        try:
            result = TOOLS[tool_name]["handler"](**tool_args)
            return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": json.dumps(result, sort_keys=True)}]}}
        except Exception as e:
            logger.error(f"Tool error in {tool_name}: {e}")
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(e)}}

    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown method: {method}"}}


def main():
    logger.info("MemPalace MCP Server starting...")
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            request = json.loads(line)
            response = handle_request(request)
            if response is not None:
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Server error: {e}")


if __name__ == "__main__":
    main()
