import importlib
import json
import os
import tempfile

import chromadb

from mempalace.miner import _deterministic_embedding
from mempalace.structure_store import StructureStore


def _seed_palace(base: str):
    palace_path = os.path.join(base, "palace")
    client = chromadb.PersistentClient(path=palace_path)
    col = client.get_or_create_collection("mempalace_drawers")

    store = StructureStore(StructureStore.default_db_path(palace_path))
    try:
        domain, root = store.ensure_main_domain()
        room = store.resolve_ordinary_container("wing_a", "room_a")

        anchor = store.create_gateway_anchor(domain.domain_id, root.node_id, "door", "wardrobe:narnia")
        gateway = store.get_or_create_gateway(domain.domain_id, anchor.node_id, "door", "wardrobe:narnia")
        child, _ = store.get_or_create_subdomain(domain.domain_id, gateway.gateway_id, "child")
        child_room = store.resolve_ordinary_container("wing_child", "room_a", domain_id=child.domain_id)

        col.add(
            ids=["drawer_struct", "drawer_legacy"],
            documents=["alpha structured", "legacy text"],
            embeddings=[_deterministic_embedding("alpha structured"), _deterministic_embedding("legacy text")],
            metadatas=[
                {
                    "wing": "wing_a",
                    "room": "room_a",
                    "source_file": "a.txt",
                    "chunk_index": 0,
                    "domain_id": room["domain_id"],
                    "container_node_id": room["container_node_id"],
                },
                {
                    "wing": "wing_legacy",
                    "room": "legacy",
                    "source_file": "b.txt",
                    "chunk_index": 0,
                },
            ],
        )

        return {
            "palace_path": palace_path,
            "root_node": root.node_id,
            "room_node": room["container_node_id"],
            "child_room_node": child_room["container_node_id"],
            "child_domain_id": child.domain_id,
        }
    finally:
        store.close()


def _load_mcp_for_env(palace_path: str):
    os.environ["MEMPALACE_PALACE_PATH"] = palace_path
    os.environ["MEMPALACE_STRUCTURE_DB_PATH"] = str(StructureStore.default_db_path(palace_path))
    import mempalace.mcp_server as mcp_server

    return importlib.reload(mcp_server)


def test_mcp_trace_and_validation_tools():
    with tempfile.TemporaryDirectory() as tmp:
        env = _seed_palace(tmp)
        mcp = _load_mcp_for_env(env["palace_path"])

        node_trace = mcp.tool_structure_trace_node(env["child_room_node"])
        assert node_trace["container_node_id"] == env["child_room_node"]
        assert node_trace["absolute_breadcrumb"]
        assert "domain_chain" in node_trace

        drawer_trace = mcp.tool_structure_trace_drawer(drawer_id="drawer_struct")
        assert drawer_trace["drawer_id"] == "drawer_struct"
        assert drawer_trace["container_node_id"] == env["room_node"]

        validation = mcp.tool_structure_validate()
        assert validation["valid"] is True
        assert "errors" in validation


def test_mcp_resolution_ambiguity_and_malformed_ids():
    with tempfile.TemporaryDirectory() as tmp:
        env = _seed_palace(tmp)
        mcp = _load_mcp_for_env(env["palace_path"])

        # room_a exists in root + child domains => ambiguous label-only resolve
        ambiguous = mcp.tool_structure_resolve(label="room_a", node_type="room")
        assert ambiguous["error"] == "ambiguous"

        malformed = mcp.tool_structure_trace_node("node_BAD")
        assert malformed["error"] == "invalid_id"

        resolved_in_child = mcp.tool_structure_resolve(
            wing="wing_child",
            room="room_a",
            domain_id=env["child_domain_id"],
        )
        assert resolved_in_child["resolved"] is True
        assert resolved_in_child["domain_id"] == env["child_domain_id"]


def test_mcp_creation_and_children_listing():
    with tempfile.TemporaryDirectory() as tmp:
        env = _seed_palace(tmp)
        mcp = _load_mcp_for_env(env["palace_path"])

        root_trace = mcp.tool_structure_trace_node(env["room_node"])
        domain_id = root_trace["domain_id"]
        parent_node_id = root_trace["local_lineage"][-1]["node_id"]

        gateway = mcp.tool_structure_create_gateway_anchor(
            domain_id=domain_id,
            parent_node_id=parent_node_id,
            label="new-door",
            flavor="wardrobe:fractal",
        )
        assert gateway["gateway_id"].startswith("gate_")

        subdomain = mcp.tool_structure_create_subdomain(
            parent_domain_id=domain_id,
            entry_gateway_id=gateway["gateway_id"],
            label="new-sub",
        )
        assert subdomain["domain_id"].startswith("dom_")

        nested = mcp.tool_structure_create_nested_subdomain(
            parent_domain_id=domain_id,
            parent_node_id=parent_node_id,
            gateway_label="nested-door",
            subdomain_label="nested-sub",
            flavor="wardrobe:blackhole",
        )
        assert nested["domain_id"].startswith("dom_")

        children = mcp.tool_structure_list_children(node_id=parent_node_id)
        assert "children" in children
        assert any(c["node_type"] == "gateway_anchor" for c in children["children"])


def test_mcp_status_search_and_request_shape_stability():
    with tempfile.TemporaryDirectory() as tmp:
        env = _seed_palace(tmp)
        mcp = _load_mcp_for_env(env["palace_path"])

        status = mcp.tool_status()
        assert status["structured_drawers"] >= 1
        assert status["unstructured_drawers"] >= 1

        search = mcp.tool_search("alpha", limit=2)
        assert "results" in search
        assert "domain_id" in search["results"][0]

        # verify machine-stable JSON output keys are sorted in handle_request response text
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "mempalace_structure_validate", "arguments": {}},
        }
        response = mcp.handle_request(request)
        text = response["result"]["content"][0]["text"]
        parsed = json.loads(text)
        assert "valid" in parsed
