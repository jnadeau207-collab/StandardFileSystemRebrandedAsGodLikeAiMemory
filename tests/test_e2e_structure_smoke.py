import importlib
import os
import tempfile

import chromadb
import yaml

from mempalace.migration import migrate_legacy_flat_drawers
from mempalace.miner import _deterministic_embedding, mine
from mempalace.palace_graph import trace_to_root, traverse
from mempalace.config import MempalaceConfig
from mempalace.searcher import search_memories
from mempalace.structure_store import StructureStore
from mempalace.structure_helpers import StructureManager


def test_e2e_recursive_structure_and_legacy_compatibility_smoke():
    with tempfile.TemporaryDirectory() as tmp:
        project_dir = os.path.join(tmp, "project")
        os.makedirs(os.path.join(project_dir, "backend"), exist_ok=True)
        with open(os.path.join(project_dir, "backend", "app.py"), "w") as f:
            f.write("def run():\n    return 'deep context'\n" * 20)
        with open(os.path.join(project_dir, "mempalace.yaml"), "w") as f:
            yaml.dump(
                {
                    "wing": "wing_project",
                    "rooms": [
                        {"name": "backend", "description": "Backend code"},
                        {"name": "general", "description": "General"},
                    ],
                },
                f,
            )

        palace_path = os.path.join(tmp, "palace")
        mine(project_dir, palace_path)
        os.environ["MEMPALACE_PALACE_PATH"] = palace_path
        os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = str(StructureStore.default_db_path(palace_path))
        cfg = MempalaceConfig()

        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_drawers")

        # Add one intentionally legacy/unstructured drawer
        col.add(
            ids=["drawer_legacy_only"],
            documents=["legacy-only drawer content"],
            embeddings=[_deterministic_embedding("legacy-only drawer content")],
            metadatas=[
                {
                    "wing": "wing_legacy",
                    "room": "legacy_room",
                    "source_file": "legacy.txt",
                    "chunk_index": 0,
                }
            ],
        )

        # migrate legacy flat drawers to canonical structure metadata
        migration_report = migrate_legacy_flat_drawers(palace_path)
        assert migration_report["processed"] >= 1

        store = StructureStore(StructureStore.default_db_path(palace_path))
        try:
            root_domain, root_node = store.ensure_main_domain()
            manager = StructureManager(store.db_path)
            try:
                gateway = manager.create_gateway_anchor(
                    domain_id=root_domain.domain_id,
                    parent_node_id=root_node.node_id,
                    label="entry-gateway",
                    flavor="wardrobe:narnia",
                )
                child = manager.create_subordinate_domain(
                    parent_domain_id=root_domain.domain_id,
                    entry_gateway_id=gateway["gateway_id"],
                    label="child-domain",
                )
                nested = manager.create_nested_subordinate_domain(
                    parent_domain_id=child["domain_id"],
                    parent_node_id=child["root_node_id"],
                    gateway_label="deep-gateway",
                    subdomain_label="nested-domain",
                    flavor="wardrobe:fractal",
                )
            finally:
                manager.close()

            deep_container = store.resolve_ordinary_container(
                wing="wing_deep",
                room="room_deep",
                domain_id=nested["domain_id"],
            )
        finally:
            store.close()

        col.add(
            ids=["drawer_deep_structured"],
            documents=["deep structured memory"],
            embeddings=[_deterministic_embedding("deep structured memory")],
            metadatas=[
                {
                    "wing": "wing_deep",
                    "room": "room_deep",
                    "source_file": "deep.txt",
                    "chunk_index": 0,
                    "domain_id": deep_container["domain_id"],
                    "container_node_id": deep_container["container_node_id"],
                }
            ],
        )

        results = search_memories("deep structured", palace_path=palace_path, n_results=3)
        deep_hit = next(h for h in results["results"] if h["source_file"] == "deep.txt")
        assert deep_hit["container_node_id"] == deep_container["container_node_id"]
        assert len(deep_hit["gateway_crossings"]) >= 2

        graph_walk = traverse(deep_container["container_node_id"], config=cfg, col=col, max_hops=4)
        assert any(item.get("edge_type") == "gateway_domain_transition" for item in graph_walk if isinstance(item, dict))

        lineage = trace_to_root(deep_container["container_node_id"], config=cfg)
        assert lineage[-1]["node_type"] == "domain_root"

        # MCP smoke path over same structure
        os.environ["MEMPALACE_PALACE_PATH"] = palace_path
        os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = str(StructureStore.default_db_path(palace_path))
        import mempalace.mcp_server as mcp_server

        mcp = importlib.reload(mcp_server)
        mcp_trace = mcp.tool_structure_trace_drawer(drawer_id="drawer_deep_structured")
        assert mcp_trace["container_node_id"] == deep_container["container_node_id"]
        mcp_validate = mcp.tool_structure_validate()
        assert mcp_validate["valid"] is True

        # legacy flat compatibility still works (even after migration)
        legacy_search = mcp.tool_search("legacy-only drawer content", limit=5)
        assert any(r["wing"] == "wing_legacy" for r in legacy_search["results"])
