# mempalace/ — Core Package

The Python package that powers MemPalace.

## Modules

| Module | What it does |
|--------|-------------|
| `cli.py` | CLI entry point — routes to mine, search, init, compress, wake-up |
| `config.py` | Configuration loading (`~/.mempalace/config.json`, env vars, defaults) including `structure_db_path` |
| `normalize.py` | Converts multiple chat export formats into standard transcript format |
| `miner.py` | Project-file ingest — scans, chunks, files to ChromaDB with canonical `domain_id`/`container_node_id` |
| `convo_miner.py` | Conversation ingest — exchange/general extraction, files with canonical structure metadata |
| `searcher.py` | Structure-aware semantic search via ChromaDB; returns legacy fields plus lineage/breadcrumb metadata when present |
| `layers.py` | 4-layer memory stack (L0/L1/L2/L3), now consuming structure-aware search metadata for L2/L3 display |
| `dialect.py` | AAAK compression (entity codes, emotion markers, compact syntax) |
| `knowledge_graph.py` | Temporal entity-relationship graph (SQLite), time-filtered queries, invalidation |
| `palace_graph.py` | Structured + legacy graph traversal: canonical node identity, gateway transitions, tunnel semantics, legacy fallback |
| `mcp_server.py` | MCP server exposing read/write/graph/KG/closets/recall plus structure trace/resolve/validate/authoring tools |
| `onboarding.py` | Guided first-run setup — asks about people/projects, generates bootstrap |
| `entity_registry.py` | Entity code registry — maps names to AAAK codes |
| `entity_detector.py` | Auto-detect people/projects from content |
| `general_extractor.py` | Classifies text into decision/preference/milestone/problem/emotional memories |
| `room_detector_local.py` | Local folder-to-room mapping patterns |
| `spellcheck.py` | Name-aware spellcheck |
| `split_mega_files.py` | Splits concatenated transcript files into per-session files |
| `structure.py` | Structural primitives: node/domain/gateway dataclasses, enums, immutable ID helpers |
| `structure_store.py` | SQLite local-first canonical structure store (domains/nodes/gateways + constraints + deterministic resolution helpers) |
| `structure_helpers.py` | High-level helper APIs for canonical placement and gateway/subdomain authoring |
| `tracing.py` | Local and absolute lineage tracing across recursive domain transitions |
| `validators.py` | Invariant guards (cycle checks, structural assertions) |
| `migration.py` | Executable flat-to-canonical migration (`migrate_legacy_flat_drawers`) |

## Canonical structure model (implemented)

MemPalace now supports two parallel surfaces:

1. **Legacy compatibility surface**: `wing` / `room` metadata, still supported in ingest/search/MCP.
2. **Canonical structure surface**: recursive domains and nodes with immutable IDs.

Key concepts:

- **Domain** (`domain_id`): root or subordinate recursive domain.
- **Node** (`node_id`): immutable identity for wing/room/gateway/root containers.
- **Gateway anchor** (`gateway_id` + anchor node): explicit parent→child domain transition point.
- **Subordinate domain**: child domain with one `parent_domain_id` and one `entry_gateway_id`.
- **Local lineage**: ancestry within one domain.
- **Absolute lineage**: ancestry from any node to root across gateway/domain transitions.

`flavor` strings (e.g. `wardrobe:narnia`) are optional metadata only and never identity truth.

## Runtime architecture

```
Ingest (miner/convo_miner)
  -> ChromaDB drawers (verbatim docs + legacy metadata + canonical IDs)
  -> Structure SQLite (domains, nodes, gateways)
  -> Knowledge Graph SQLite (entity relations)

Read paths
  searcher / layers / palace_graph / MCP
    -> use canonical IDs + lineage when present
    -> fall back to legacy flat behavior when structure metadata is absent
```

This keeps existing flat workflows working while enabling deterministic recursive navigation for structured palaces.
