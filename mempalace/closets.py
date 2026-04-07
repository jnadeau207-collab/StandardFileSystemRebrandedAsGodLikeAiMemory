#!/usr/bin/env python3
"""
closets.py — Per-room AAAK compressed summaries.

A closet is a compressed summary of everything in one room.
Instead of searching 50 drawers, read one closet (~100-200 tokens).

Closets are auto-generated from the verbatim drawers using AAAK Dialect.
They're stored in the mempalace_closets ChromaDB collection.

Usage:
    from mempalace.closets import generate_closets, get_closet

    # Generate closets for all rooms (or one wing)
    generate_closets(palace_path, wing="my_app")

    # Retrieve a specific closet
    closet = get_closet(palace_path, wing="my_app", room="backend")
"""

import os
from collections import defaultdict
from datetime import datetime

import chromadb

from .dialect import Dialect


def generate_closets(palace_path, wing=None, entity_config_path=None, dry_run=False):
    """
    Generate AAAK closet summaries for each room.

    Groups all drawers by wing/room, compresses them into a single
    AAAK summary per room, and stores in mempalace_closets collection.

    Returns dict of {wing/room: closet_text}
    """
    try:
        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_drawers")
    except Exception:
        return {}

    # Load dialect
    if entity_config_path and os.path.exists(entity_config_path):
        dialect = Dialect.from_config(entity_config_path)
    else:
        dialect = Dialect()

    # Fetch all drawers (optionally filtered by wing)
    kwargs = {"include": ["documents", "metadatas"]}
    if wing:
        kwargs["where"] = {"wing": wing}
    results = col.get(**kwargs)

    # Group by wing/room
    rooms = defaultdict(list)
    for doc, meta in zip(results["documents"], results["metadatas"]):
        w = meta.get("wing", "unknown")
        r = meta.get("room", "general")
        rooms[(w, r)].append((doc, meta))

    if not rooms:
        return {}

    closets = {}
    closet_col = None
    if not dry_run:
        closet_col = client.get_or_create_collection("mempalace_closets")

    for (w, r), drawers in sorted(rooms.items()):
        # Compress each drawer, then combine into a room-level summary
        compressed_lines = []
        room_topics = set()

        for doc, meta in drawers:
            compressed = dialect.compress(doc, metadata=meta)
            # Parse out the content line (skip header)
            for line in compressed.split("\n"):
                if ":" in line.split("|")[0]:  # content line has ZID:entities
                    compressed_lines.append(line)
                    # Extract topics/emotions/flags for summary
                    parts = line.split("|")
                    if len(parts) >= 2:
                        room_topics.update(parts[1].split("_")[:2])

        # Build closet: room header + compressed content
        closet_lines = [
            f"=CLOSET[{w}/{r}]= ({len(drawers)} drawers, {datetime.now().strftime('%Y-%m-%d')})",
        ]

        if room_topics:
            topics_str = ", ".join(sorted(room_topics)[:8])
            closet_lines.append(f"TOPICS: {topics_str}")

        # Include up to 20 compressed entries (most rooms won't exceed this)
        for line in compressed_lines[:20]:
            closet_lines.append(line)

        if len(compressed_lines) > 20:
            closet_lines.append(f"... +{len(compressed_lines) - 20} more entries")

        closet_text = "\n".join(closet_lines)
        closets[f"{w}/{r}"] = closet_text

        if dry_run:
            stats = dialect.compression_stats(
                "\n".join(doc for doc, _ in drawers), closet_text
            )
            print(f"  [{w}/{r}] {len(drawers)} drawers → ~{stats['compressed_tokens']}t ({stats['ratio']:.1f}x)")
            print(f"    {closet_text[:120]}...")
            print()
        elif closet_col is not None:
            closet_id = f"closet_{w}_{r}"
            try:
                closet_col.upsert(
                    ids=[closet_id],
                    documents=[closet_text],
                    metadatas=[{
                        "wing": w,
                        "room": r,
                        "drawer_count": len(drawers),
                        "generated_at": datetime.now().isoformat(),
                        "type": "closet",
                    }],
                )
            except Exception:
                pass

    return closets


def get_closet(palace_path, wing=None, room=None):
    """
    Retrieve a closet summary.

    Args:
        wing: Filter by wing
        room: Filter by room

    Returns list of closet dicts with text, wing, room, drawer_count.
    """
    try:
        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_closets")
    except Exception:
        return []

    where = {}
    if wing and room:
        where = {"$and": [{"wing": wing}, {"room": room}]}
    elif wing:
        where = {"wing": wing}
    elif room:
        where = {"room": room}

    kwargs = {"include": ["documents", "metadatas"]}
    if where:
        kwargs["where"] = where

    try:
        results = col.get(**kwargs)
    except Exception:
        return []

    closets = []
    for doc, meta in zip(results["documents"], results["metadatas"]):
        closets.append({
            "text": doc,
            "wing": meta.get("wing", "?"),
            "room": meta.get("room", "?"),
            "drawer_count": meta.get("drawer_count", 0),
            "generated_at": meta.get("generated_at", ""),
        })

    return closets


def list_closets(palace_path):
    """List all available closets with stats."""
    try:
        client = chromadb.PersistentClient(path=palace_path)
        col = client.get_collection("mempalace_closets")
    except Exception:
        return []

    results = col.get(include=["metadatas"])
    closets = []
    for meta in results["metadatas"]:
        closets.append({
            "wing": meta.get("wing", "?"),
            "room": meta.get("room", "?"),
            "drawer_count": meta.get("drawer_count", 0),
            "generated_at": meta.get("generated_at", ""),
        })
    return closets
