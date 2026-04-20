"""Structure-aware and backward-compatible graph traversal for MemPalace."""

from __future__ import annotations

from collections import Counter, defaultdict, deque

import chromadb

from .config import MempalaceConfig
from .structure_store import StructureStore
from .tracing import absolute_lineage


def _get_collection(config=None):
    config = config or MempalaceConfig()
    try:
        client = chromadb.PersistentClient(path=config.palace_path)
        return client.get_collection(config.collection_name)
    except Exception:
        return None


def _build_legacy_graph(col):
    room_data = defaultdict(lambda: {"wings": set(), "halls": set(), "count": 0, "dates": set()})
    total = col.count()
    offset = 0
    while offset < total:
        batch = col.get(limit=1000, offset=offset, include=["metadatas"])
        for meta in batch["metadatas"]:
            room = meta.get("room", "")
            wing = meta.get("wing", "")
            hall = meta.get("hall", "")
            date = meta.get("date", "")
            if room and room != "general" and wing:
                room_data[room]["wings"].add(wing)
                if hall:
                    room_data[room]["halls"].add(hall)
                if date:
                    room_data[room]["dates"].add(date)
                room_data[room]["count"] += 1
        if not batch["ids"]:
            break
        offset += len(batch["ids"])

    edges = []
    for room, data in room_data.items():
        wings = sorted(data["wings"])
        if len(wings) >= 2:
            for i, wa in enumerate(wings):
                for wb in wings[i + 1 :]:
                    for hall in data["halls"]:
                        edges.append(
                            {
                                "edge_type": "tunnel",
                                "room": room,
                                "wing_a": wa,
                                "wing_b": wb,
                                "hall": hall,
                                "count": data["count"],
                            }
                        )

    nodes = {}
    for room, data in room_data.items():
        nodes[room] = {
            "mode": "legacy",
            "label": room,
            "wings": sorted(data["wings"]),
            "halls": sorted(data["halls"]),
            "count": data["count"],
            "dates": sorted(data["dates"])[-5:] if data["dates"] else [],
        }

    return {"mode": "legacy", "nodes": nodes, "edges": edges}


def _build_structured_graph(col, config):
    store = StructureStore(config.structure_db_path)
    try:
        node_rows = store.conn.execute("SELECT * FROM nodes").fetchall()
        if not node_rows:
            return None

        drawer_counts = Counter()
        room_label_domains = defaultdict(set)
        room_to_nodes = defaultdict(set)

        total = col.count()
        offset = 0
        while offset < total:
            batch = col.get(limit=1000, offset=offset, include=["metadatas"])
            for meta in batch["metadatas"]:
                node_id = meta.get("container_node_id")
                if node_id:
                    drawer_counts[node_id] += 1
                    label = meta.get("room")
                    domain_id = meta.get("domain_id")
                    if label and domain_id:
                        room_label_domains[label].add(domain_id)
                        room_to_nodes[label].add(node_id)
            if not batch["ids"]:
                break
            offset += len(batch["ids"])

        nodes = {}
        for row in node_rows:
            node_id = row["node_id"]
            nodes[node_id] = {
                "mode": "structured",
                "node_id": node_id,
                "domain_id": row["domain_id"],
                "label": row["label"],
                "node_type": row["node_type"],
                "flavor": row["flavor"],
                "parent_node_id": row["parent_node_id"],
                "count": drawer_counts.get(node_id, 0),
            }

        edges = []
        for node in nodes.values():
            if node["parent_node_id"]:
                edges.append(
                    {
                        "edge_type": "ordinary_containment",
                        "from": node["parent_node_id"],
                        "to": node["node_id"],
                    }
                )

        domain_rows = store.conn.execute("SELECT * FROM domains WHERE parent_domain_id IS NOT NULL").fetchall()
        for d in domain_rows:
            child_root = store.get_root_node(d["domain_id"])
            if child_root is None:
                continue
            gateway = store._fetch_one("SELECT node_id FROM gateways WHERE gateway_id = ?", (d["entry_gateway_id"],))
            if gateway is None:
                continue
            edges.append(
                {
                    "edge_type": "gateway_domain_transition",
                    "from": gateway["node_id"],
                    "to": child_root.node_id,
                    "gateway_id": d["entry_gateway_id"],
                }
            )

        for room_label, node_ids in room_to_nodes.items():
            if len(node_ids) < 2:
                continue
            node_ids = sorted(node_ids)
            for i, a in enumerate(node_ids):
                for b in node_ids[i + 1 :]:
                    if nodes[a]["domain_id"] != nodes[b]["domain_id"]:
                        edges.append(
                            {
                                "edge_type": "tunnel",
                                "from": a,
                                "to": b,
                                "room_label": room_label,
                            }
                        )

        return {"mode": "structured", "nodes": nodes, "edges": edges}
    finally:
        store.close()


def build_graph(col=None, config=None):
    config = config or MempalaceConfig()
    if col is None:
        col = _get_collection(config)
    if not col:
        return {}, []

    structured = _build_structured_graph(col, config)
    if structured:
        return structured["nodes"], structured["edges"]

    legacy = _build_legacy_graph(col)
    return legacy["nodes"], legacy["edges"]


def _is_structured(nodes: dict) -> bool:
    if not nodes:
        return False
    first = next(iter(nodes.values()))
    return first.get("mode") == "structured"


def _adjacency(edges: list) -> dict:
    adj = defaultdict(list)
    for edge in edges:
        frm = edge.get("from")
        to = edge.get("to")
        if frm and to:
            adj[frm].append((to, edge))
            adj[to].append((frm, edge))
    return adj


def traverse(start_room: str, col=None, config=None, max_hops: int = 2):
    nodes, edges = build_graph(col, config)
    if not nodes:
        return []

    if not _is_structured(nodes):
        if start_room not in nodes:
            return {
                "error": f"Room '{start_room}' not found",
                "suggestions": _fuzzy_match(start_room, nodes),
            }

        start = nodes[start_room]
        visited = {start_room}
        results = [{"room": start_room, "wings": start["wings"], "count": start["count"], "hop": 0}]

        frontier = [(start_room, 0)]
        while frontier:
            current_room, depth = frontier.pop(0)
            if depth >= max_hops:
                continue
            current_wings = set(nodes.get(current_room, {}).get("wings", []))
            for room, data in nodes.items():
                if room in visited:
                    continue
                shared = current_wings & set(data.get("wings", []))
                if shared:
                    visited.add(room)
                    results.append(
                        {
                            "room": room,
                            "wings": data.get("wings", []),
                            "count": data.get("count", 0),
                            "hop": depth + 1,
                            "connected_via": sorted(shared),
                        }
                    )
                    frontier.append((room, depth + 1))

        results.sort(key=lambda x: (x["hop"], -x["count"]))
        return results[:50]

    start_candidates = []
    if start_room in nodes:
        start_candidates = [start_room]
    else:
        start_candidates = [
            nid
            for nid, node in nodes.items()
            if node.get("label", "").lower() == start_room.lower() and node.get("node_type") == "room"
        ]

    if not start_candidates:
        return {
            "error": f"Node/room '{start_room}' not found",
            "suggestions": [n["label"] for n in list(nodes.values())[:5]],
        }

    if len(start_candidates) > 1:
        return {
            "error": f"Ambiguous room label '{start_room}'",
            "candidates": [
                {
                    "node_id": nid,
                    "domain_id": nodes[nid]["domain_id"],
                    "label": nodes[nid]["label"],
                }
                for nid in start_candidates
            ],
        }

    start_node_id = start_candidates[0]
    adj = _adjacency(edges)
    queue = deque([(start_node_id, 0)])
    visited = {start_node_id}
    results = []

    while queue:
        current, depth = queue.popleft()
        node = nodes[current]
        results.append(
            {
                "node_id": current,
                "label": node["label"],
                "node_type": node["node_type"],
                "domain_id": node["domain_id"],
                "count": node.get("count", 0),
                "hop": depth,
            }
        )
        if depth >= max_hops:
            continue
        for nxt, edge in adj.get(current, []):
            if nxt in visited:
                continue
            visited.add(nxt)
            queue.append((nxt, depth + 1))
            results.append(
                {
                    "from": current,
                    "to": nxt,
                    "edge_type": edge["edge_type"],
                    "hop": depth + 1,
                }
            )

    return results[:100]


def find_tunnels(wing_a: str = None, wing_b: str = None, col=None, config=None):
    nodes, edges = build_graph(col, config)
    if not nodes:
        return []

    if not _is_structured(nodes):
        tunnels = []
        for room, data in nodes.items():
            wings = data["wings"]
            if len(wings) < 2:
                continue
            if wing_a and wing_a not in wings:
                continue
            if wing_b and wing_b not in wings:
                continue
            tunnels.append({"room": room, "wings": wings, "count": data["count"]})
        tunnels.sort(key=lambda x: -x["count"])
        return tunnels[:50]

    tunnels = []
    for edge in edges:
        if edge.get("edge_type") != "tunnel":
            continue
        a = nodes.get(edge["from"], {})
        b = nodes.get(edge["to"], {})
        if wing_a and a.get("label") != wing_a and b.get("label") != wing_a:
            continue
        if wing_b and a.get("label") != wing_b and b.get("label") != wing_b:
            continue
        tunnels.append(
            {
                "room": edge.get("room_label", "?"),
                "node_a": edge["from"],
                "node_b": edge["to"],
                "domain_a": a.get("domain_id"),
                "domain_b": b.get("domain_id"),
            }
        )
    return tunnels[:50]


def graph_stats(col=None, config=None):
    nodes, edges = build_graph(col, config)
    if not nodes:
        return {
            "total_rooms": 0,
            "tunnel_rooms": 0,
            "total_edges": 0,
            "rooms_per_wing": {},
            "top_tunnels": [],
        }

    if not _is_structured(nodes):
        tunnel_rooms = sum(1 for n in nodes.values() if len(n["wings"]) >= 2)
        wing_counts = Counter()
        for data in nodes.values():
            for w in data["wings"]:
                wing_counts[w] += 1
        return {
            "mode": "legacy",
            "total_rooms": len(nodes),
            "tunnel_rooms": tunnel_rooms,
            "total_edges": len(edges),
            "rooms_per_wing": dict(wing_counts.most_common()),
        }

    tunnel_edges = [e for e in edges if e.get("edge_type") == "tunnel"]
    per_domain = Counter(n["domain_id"] for n in nodes.values() if n.get("node_type") == "room")
    return {
        "mode": "structured",
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "gateway_edges": sum(1 for e in edges if e.get("edge_type") == "gateway_domain_transition"),
        "tunnel_edges": len(tunnel_edges),
        "rooms_per_domain": dict(per_domain),
    }


def trace_to_root(node_id: str, config=None):
    config = config or MempalaceConfig()
    store = StructureStore(config.structure_db_path)
    try:
        steps = absolute_lineage(store, node_id)
        return [
            {
                "domain_id": s.domain_id,
                "node_id": s.node_id,
                "label": s.label,
                "node_type": s.node_type,
                "link_type": s.link_type,
            }
            for s in steps
        ]
    finally:
        store.close()


def _fuzzy_match(query: str, nodes: dict, n: int = 5):
    query_lower = query.lower()
    scored = []
    for room in nodes:
        if query_lower in room:
            scored.append((room, 1.0))
        elif any(word in room for word in query_lower.split("-")):
            scored.append((room, 0.5))
    scored.sort(key=lambda x: -x[1])
    return [r for r, _ in scored[:n]]
