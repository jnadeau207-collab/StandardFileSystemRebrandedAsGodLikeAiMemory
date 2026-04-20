"""Executable migration utility for legacy flat wing/room drawers into structure metadata."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import chromadb

from .structure_helpers import StructureManager
from .structure_store import StructureStore
from .miner import _deterministic_embedding


@dataclass(frozen=True)
class MigrationStep:
    step: str
    detail: str


def build_flat_to_recursive_plan() -> list[MigrationStep]:
    return [
        MigrationStep("bootstrap_root_domain", "Create root domain + root node if missing."),
        MigrationStep("map_wings_to_nodes", "Create deterministic wing nodes in root domain."),
        MigrationStep("map_rooms_to_nodes", "Create deterministic room nodes under each wing."),
        MigrationStep("attach_drawers", "Write domain_id + container_node_id into legacy drawer metadata."),
    ]


def migrate_legacy_flat_drawers(
    palace_path: str,
    collection_name: str = "mempalace_drawers",
    batch_size: int = 500,
) -> dict:
    """Migrate legacy flat wing/room metadata to canonical structure IDs.

    Idempotent: already-migrated records are skipped if values are unchanged.
    """

    palace_path = str(Path(palace_path).expanduser().resolve())
    client = chromadb.PersistentClient(path=palace_path)
    collection = client.get_collection(collection_name)

    manager = StructureManager(StructureStore.default_db_path(palace_path))
    report = {
        "steps": [s.step for s in build_flat_to_recursive_plan()],
        "processed": 0,
        "updated": 0,
        "already_structured": 0,
        "skipped_missing_fields": 0,
        "errors": 0,
        "error_cases": [],
    }

    try:
        manager.store.ensure_main_domain()
        offset = 0
        while True:
            batch = collection.get(
                limit=batch_size,
                offset=offset,
                include=["documents", "metadatas"],
            )
            ids = batch.get("ids", [])
            if not ids:
                break

            for drawer_id, doc, meta in zip(ids, batch["documents"], batch["metadatas"]):
                report["processed"] += 1
                wing = meta.get("wing")
                room = meta.get("room")
                if not wing or not room:
                    report["skipped_missing_fields"] += 1
                    continue

                try:
                    placement = manager.resolve_ordinary_container(wing=wing, room=room)
                except Exception as exc:
                    report["errors"] += 1
                    report["error_cases"].append({"id": drawer_id, "error": str(exc)})
                    continue

                new_meta = dict(meta)
                new_meta["domain_id"] = placement["domain_id"]
                new_meta["container_node_id"] = placement["container_node_id"]

                if (
                    meta.get("domain_id") == new_meta["domain_id"]
                    and meta.get("container_node_id") == new_meta["container_node_id"]
                ):
                    report["already_structured"] += 1
                    continue

                collection.upsert(
                    ids=[drawer_id],
                    documents=[doc],
                    embeddings=[_deterministic_embedding(doc)],
                    metadatas=[new_meta],
                )
                report["updated"] += 1

            offset += len(ids)

    finally:
        manager.close()

    if report["errors"] > 0:
        raise RuntimeError(f"Migration failed with {report['errors']} errors", report)

    return report
