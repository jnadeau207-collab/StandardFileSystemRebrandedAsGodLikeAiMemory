import tempfile
from pathlib import Path

import pytest
import chromadb

from mempalace.structure import NodeType
from mempalace.structure_store import StructureStore
from mempalace.tracing import absolute_lineage, local_lineage
from mempalace.structure_helpers import StructureManager
from mempalace.migration import migrate_legacy_flat_drawers
from mempalace.miner import _deterministic_embedding


@pytest.fixture()
def store():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "structure.sqlite3"
        s = StructureStore(db_path)
        try:
            yield s
        finally:
            s.close()


def test_domain_creation(store):
    root = store.create_domain("root")
    assert root.domain_id.startswith("dom_")
    assert root.parent_domain_id is None


def test_node_creation(store):
    root_domain = store.create_domain("root")
    root_node = store.create_node(
        domain_id=root_domain.domain_id,
        label="root-node",
        node_type=NodeType.DOMAIN_ROOT.value,
        is_root=True,
    )
    child = store.create_node(
        domain_id=root_domain.domain_id,
        label="room-a",
        node_type=NodeType.ROOM.value,
        parent_node_id=root_node.node_id,
    )
    assert child.domain_id == root_domain.domain_id
    assert child.parent_node_id == root_node.node_id


def test_gateway_creation(store):
    root = store.create_domain("root")
    root_node = store.create_node(root.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    gateway_anchor = store.create_node(
        root.domain_id,
        "gateway-a",
        NodeType.GATEWAY_ANCHOR.value,
        parent_node_id=root_node.node_id,
        flavor="wardrobe:narnia",
    )
    gateway = store.create_gateway(
        domain_id=root.domain_id,
        node_id=gateway_anchor.node_id,
        label="entry",
        flavor="wardrobe:narnia",
    )
    assert gateway.gateway_id.startswith("gate_")


def test_subordinate_domain_creation(store):
    parent = store.create_domain("root")
    parent_root = store.create_node(parent.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    anchor = store.create_node(
        parent.domain_id,
        "wardrobe",
        NodeType.GATEWAY_ANCHOR.value,
        parent_node_id=parent_root.node_id,
    )
    gateway = store.create_gateway(parent.domain_id, anchor.node_id, label="wardrobe")
    child, child_root = store.create_subdomain(parent.domain_id, gateway.gateway_id, "sub-palace")
    assert child.parent_domain_id == parent.domain_id
    assert child.entry_gateway_id == gateway.gateway_id
    assert child_root.is_root is True


def test_local_lineage(store):
    d = store.create_domain("root")
    root = store.create_node(d.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    wing = store.create_node(d.domain_id, "wing-a", NodeType.WING.value, parent_node_id=root.node_id)
    room = store.create_node(d.domain_id, "room-a", NodeType.ROOM.value, parent_node_id=wing.node_id)

    lineage = local_lineage(store, room.node_id)
    assert [step.label for step in lineage] == ["room-a", "wing-a", "root"]


def test_absolute_lineage(store):
    root_domain = store.create_domain("root")
    root_node = store.create_node(root_domain.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)

    anchor = store.create_node(
        root_domain.domain_id,
        "wardrobe",
        NodeType.GATEWAY_ANCHOR.value,
        parent_node_id=root_node.node_id,
        flavor="wardrobe:fractal",
    )
    gateway = store.create_gateway(
        root_domain.domain_id,
        anchor.node_id,
        label="fractal-door",
        flavor="wardrobe:fractal",
    )
    child_domain, child_root = store.create_subdomain(root_domain.domain_id, gateway.gateway_id, "child")
    child_room = store.create_node(
        child_domain.domain_id,
        "same-label",
        NodeType.ROOM.value,
        parent_node_id=child_root.node_id,
    )

    lineage = absolute_lineage(store, child_room.node_id)
    labels = [step.label for step in lineage]
    assert labels == ["same-label", "child:root", "wardrobe", "root"]


def test_repeated_labels_across_domains(store):
    root = store.create_domain("root")
    root_node = store.create_node(root.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    room_a = store.create_node(root.domain_id, "shared", NodeType.ROOM.value, parent_node_id=root_node.node_id)

    anchor = store.create_node(root.domain_id, "door", NodeType.GATEWAY_ANCHOR.value, parent_node_id=root_node.node_id)
    gateway = store.create_gateway(root.domain_id, anchor.node_id, label="door")
    child, child_root = store.create_subdomain(root.domain_id, gateway.gateway_id, "child")
    room_b = store.create_node(child.domain_id, "shared", NodeType.ROOM.value, parent_node_id=child_root.node_id)

    assert room_a.label == room_b.label
    assert room_a.node_id != room_b.node_id


def test_repeated_gateway_flavors_across_domains(store):
    root = store.create_domain("root")
    root_node = store.create_node(root.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)

    anchor_1 = store.create_node(
        root.domain_id,
        "door-1",
        NodeType.GATEWAY_ANCHOR.value,
        parent_node_id=root_node.node_id,
        flavor="wardrobe:rabbit_hole",
    )
    g1 = store.create_gateway(root.domain_id, anchor_1.node_id, label="door-1", flavor="wardrobe:rabbit_hole")

    anchor_2 = store.create_node(
        root.domain_id,
        "door-2",
        NodeType.GATEWAY_ANCHOR.value,
        parent_node_id=root_node.node_id,
        flavor="wardrobe:rabbit_hole",
    )
    g2 = store.create_gateway(root.domain_id, anchor_2.node_id, label="door-2", flavor="wardrobe:rabbit_hole")

    assert g1.flavor == g2.flavor
    assert g1.gateway_id != g2.gateway_id


def test_cycle_rejection(store):
    domain = store.create_domain("root")
    root = store.create_node(domain.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    child = store.create_node(domain.domain_id, "child", NodeType.ROOM.value, parent_node_id=root.node_id)

    with pytest.raises(ValueError, match="Parent node must belong to same domain"):
        other_domain = store.create_domain("other")
        other_root = store.create_node(other_domain.domain_id, "other-root", NodeType.DOMAIN_ROOT.value, is_root=True)
        store.create_node(domain.domain_id, "bad", NodeType.ROOM.value, parent_node_id=other_root.node_id)

    # sanity to use variable and ensure normal local topology remains valid
    assert child.parent_node_id == root.node_id


def test_orphan_gateway_domain_rejection(store):
    root = store.create_domain("root")
    with pytest.raises(ValueError, match="Child domain must provide both"):
        store.create_domain("broken", parent_domain_id=root.domain_id)

    with pytest.raises(ValueError, match="Entry gateway does not exist"):
        store.create_domain(
            "broken2",
            parent_domain_id=root.domain_id,
            entry_gateway_id="gate_missing",
        )


def test_file_drawer_into_subordinate_domain(store):
    root = store.create_domain("root")
    root_node = store.create_node(root.domain_id, "root", NodeType.DOMAIN_ROOT.value, is_root=True)
    mgr = StructureManager(store.db_path)
    try:
        gw = mgr.create_gateway_anchor(
            domain_id=root.domain_id,
            parent_node_id=root_node.node_id,
            label="narnia-door",
            flavor="wardrobe:narnia",
        )
        child = mgr.create_subordinate_domain(
            parent_domain_id=root.domain_id,
            entry_gateway_id=gw["gateway_id"],
            label="narnia-domain",
        )
        child_room = store.resolve_ordinary_container(
            wing="wing_child",
            room="room_shared",
            domain_id=child["domain_id"],
        )
        placement = mgr.file_drawer_to_node(
            domain_id=child["domain_id"],
            container_node_id=child_room["container_node_id"],
        )
        assert placement["domain_id"] == child["domain_id"]
    finally:
        mgr.close()


def test_migration_idempotent_and_not_orphaned():
    with tempfile.TemporaryDirectory() as tmp:
        client = chromadb.PersistentClient(path=tmp)
        col = client.get_or_create_collection("mempalace_drawers")
        col.add(
            ids=["d1", "d2"],
            documents=["alpha", "beta"],
            embeddings=[_deterministic_embedding("alpha"), _deterministic_embedding("beta")],
            metadatas=[
                {"wing": "wing_a", "room": "room_x", "source_file": "a.txt", "chunk_index": 0},
                {"wing": "wing_a", "room": "room_x", "source_file": "b.txt", "chunk_index": 1},
            ],
        )

        report1 = migrate_legacy_flat_drawers(tmp)
        assert report1["processed"] == 2
        assert report1["updated"] == 2

        rows = col.get(include=["metadatas"])
        for meta in rows["metadatas"]:
            assert "domain_id" in meta
            assert "container_node_id" in meta

        report2 = migrate_legacy_flat_drawers(tmp)
        assert report2["processed"] == 2
        assert report2["updated"] == 0
        assert report2["already_structured"] == 2
