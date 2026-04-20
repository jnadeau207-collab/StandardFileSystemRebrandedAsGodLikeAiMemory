import tempfile
import os

import chromadb

from mempalace.layers import Layer2, Layer3
from mempalace.config import MempalaceConfig
from mempalace.miner import _deterministic_embedding
from mempalace.palace_graph import build_graph, trace_to_root, traverse
from mempalace.searcher import search_memories
from mempalace.structure import NodeType
from mempalace.structure_store import StructureStore


def _build_palace_with_structure(tmp: str):
    palace_path = f"{tmp}/palace"
    client = chromadb.PersistentClient(path=palace_path)
    col = client.get_or_create_collection("mempalace_drawers")

    store = StructureStore(StructureStore.default_db_path(palace_path))
    try:
        root_domain, root_node = store.ensure_main_domain()
        root_room = store.resolve_ordinary_container("wing_alpha", "room_common")

        gateway_anchor = store.create_gateway_anchor(
            domain_id=root_domain.domain_id,
            parent_node_id=root_node.node_id,
            label="wardrobe-a",
            flavor="wardrobe:rabbit_hole",
        )
        gateway = store.get_or_create_gateway(
            domain_id=root_domain.domain_id,
            node_id=gateway_anchor.node_id,
            label="wardrobe-a",
            flavor="wardrobe:rabbit_hole",
        )
        child_domain, child_root = store.get_or_create_subdomain(
            parent_domain_id=root_domain.domain_id,
            entry_gateway_id=gateway.gateway_id,
            label="child-space",
        )
        child_room = store.resolve_ordinary_container(
            wing="wing_child",
            room="room_common",
            domain_id=child_domain.domain_id,
        )

        # same flavor, different identity
        second_anchor = store.create_gateway_anchor(
            domain_id=root_domain.domain_id,
            parent_node_id=root_node.node_id,
            label="wardrobe-b",
            flavor="wardrobe:rabbit_hole",
        )
        second_gateway = store.get_or_create_gateway(
            domain_id=root_domain.domain_id,
            node_id=second_anchor.node_id,
            label="wardrobe-b",
            flavor="wardrobe:rabbit_hole",
        )

        col.add(
            ids=["d_root", "d_child", "d_legacy"],
            documents=["alpha root memory", "alpha child memory", "legacy memory"],
            embeddings=[
                _deterministic_embedding("alpha root memory"),
                _deterministic_embedding("alpha child memory"),
                _deterministic_embedding("legacy memory"),
            ],
            metadatas=[
                {
                    "wing": "wing_alpha",
                    "room": "room_common",
                    "source_file": "root.txt",
                    "chunk_index": 0,
                    "domain_id": root_room["domain_id"],
                    "container_node_id": root_room["container_node_id"],
                },
                {
                    "wing": "wing_child",
                    "room": "room_common",
                    "source_file": "child.txt",
                    "chunk_index": 0,
                    "domain_id": child_room["domain_id"],
                    "container_node_id": child_room["container_node_id"],
                },
                {
                    "wing": "wing_legacy",
                    "room": "room_legacy",
                    "source_file": "legacy.txt",
                    "chunk_index": 0,
                },
            ],
        )

        return {
            "palace_path": palace_path,
            "root_room": root_room,
            "child_room": child_room,
            "second_gateway": second_gateway,
            "child_root": child_root,
        }
    finally:
        store.close()


def test_search_structure_lineage_and_legacy_compatibility():
    with tempfile.TemporaryDirectory() as tmp:
        env = _build_palace_with_structure(tmp)
        result = search_memories("alpha", env["palace_path"], n_results=3)
        assert len(result["results"]) == 3

        child_hit = next(h for h in result["results"] if h["source_file"] == "child.txt")
        assert child_hit["domain_id"] == env["child_room"]["domain_id"]
        assert child_hit["container_node_id"] == env["child_room"]["container_node_id"]
        assert child_hit["local_breadcrumb"]
        assert child_hit["absolute_breadcrumb"]
        assert len(child_hit["gateway_crossings"]) >= 1

        legacy_hit = next(h for h in result["results"] if h["source_file"] == "legacy.txt")
        assert legacy_hit["wing"] == "wing_legacy"
        assert legacy_hit["room"] == "room_legacy"
        assert legacy_hit["domain_id"] is None
        assert legacy_hit["local_lineage"] == []


def test_graph_traversal_and_absolute_trace_through_gateway_domain():
    with tempfile.TemporaryDirectory() as tmp:
        env = _build_palace_with_structure(tmp)
        old_palace = os.environ.get("MEMPALACE_PALACE_PATH")
        old_struct = os.environ.get("MEMPALACE_STRUCTURE_DB_PATH")
        os.environ["MEMPALACE_PALACE_PATH"] = env["palace_path"]
        os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = str(StructureStore.default_db_path(env["palace_path"]))
        try:
            cfg = MempalaceConfig()
            col = chromadb.PersistentClient(path=env["palace_path"]).get_collection("mempalace_drawers")
            nodes, edges = build_graph(config=cfg, col=col)
            assert any(e.get("edge_type") == "gateway_domain_transition" for e in edges)

            ambiguous = traverse("room_common", config=cfg, col=col, max_hops=3)
            assert "Ambiguous" in ambiguous.get("error", "")

            node_results = traverse(env["child_room"]["container_node_id"], config=cfg, col=col, max_hops=3)
            assert any(r.get("edge_type") == "gateway_domain_transition" for r in node_results if isinstance(r, dict))

            lineage = trace_to_root(env["child_room"]["container_node_id"], config=cfg)
            link_types = [step["link_type"] for step in lineage]
            assert "gateway_domain_transition" in link_types
            assert lineage[-1]["node_type"] == NodeType.DOMAIN_ROOT.value
        finally:
            if old_palace is None:
                os.environ.pop("MEMPALACE_PALACE_PATH", None)
            else:
                os.environ["MEMPALACE_PALACE_PATH"] = old_palace
            if old_struct is None:
                os.environ.pop("MEMPALACE_STRUCTURE_DB_PATH", None)
            else:
                os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = old_struct


def test_layers_still_work_with_structure_metadata():
    with tempfile.TemporaryDirectory() as tmp:
        env = _build_palace_with_structure(tmp)
        l2 = Layer2(palace_path=env["palace_path"])
        l3 = Layer3(palace_path=env["palace_path"])

        recall = l2.retrieve(wing="wing_alpha", n_results=2)
        assert "[d:" in recall

        search_text = l3.search("alpha", n_results=2)
        assert "path:" in search_text

        raw_hits = l3.search_raw("alpha", n_results=2)
        assert "absolute_lineage" in raw_hits[0]


def test_repeated_gateway_flavors_remain_unambiguous():
    with tempfile.TemporaryDirectory() as tmp:
        env = _build_palace_with_structure(tmp)
        # same flavor, distinct gateway identities must remain distinct
        assert env["second_gateway"].gateway_id.startswith("gate_")
        old_struct = os.environ.get("MEMPALACE_STRUCTURE_DB_PATH")
        os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = str(StructureStore.default_db_path(env["palace_path"]))
        try:
            lineage = trace_to_root(env["child_room"]["container_node_id"], config=MempalaceConfig())
        finally:
            if old_struct is None:
                os.environ.pop("MEMPALACE_STRUCTURE_DB_PATH", None)
            else:
                os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = old_struct
        gateway_nodes = [s["node_id"] for s in lineage if s["link_type"] == "gateway_domain_transition"]
        assert len(set(gateway_nodes)) == len(gateway_nodes)
