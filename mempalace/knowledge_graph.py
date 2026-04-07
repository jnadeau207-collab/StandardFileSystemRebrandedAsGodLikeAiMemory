"""
knowledge_graph.py — Temporal Entity-Relationship Graph for MemPalace
=====================================================================

Real knowledge graph with:
  - Entity nodes (people, projects, tools, concepts)
  - Typed relationship edges (daughter_of, does, loves, works_on, etc.)
  - Temporal validity (valid_from → valid_to — knows WHEN facts are true)
  - Closet references (links back to the verbatim memory)

Storage: SQLite (local, no dependencies, no subscriptions)
Query: entity-first traversal with time filtering

This is what competes with Zep's temporal knowledge graph.
Zep uses Neo4j in the cloud ($25/mo+). We use SQLite locally (free).

Usage:
    from mempalace.knowledge_graph import KnowledgeGraph

    kg = KnowledgeGraph()
    kg.add_triple("Max", "child_of", "Alice", valid_from="2015-04-01")
    kg.add_triple("Max", "does", "swimming", valid_from="2025-01-01")
    kg.add_triple("Max", "loves", "chess", valid_from="2025-10-01")

    # Query: everything about Max
    kg.query_entity("Max")

    # Query: what was true about Max in January 2026?
    kg.query_entity("Max", as_of="2026-01-15")

    # Query: who is connected to Alice?
    kg.query_entity("Alice", direction="both")

    # Invalidate: Max's sports injury resolved
    kg.invalidate("Max", "has_issue", "sports_injury", ended="2026-02-15")
"""

import hashlib
import json
import os
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path


DEFAULT_KG_PATH = os.path.expanduser("~/.mempalace/knowledge_graph.sqlite3")


class KnowledgeGraph:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DEFAULT_KG_PATH
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT DEFAULT 'unknown',
                properties TEXT DEFAULT '{}',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS triples (
                id TEXT PRIMARY KEY,
                subject TEXT NOT NULL,
                predicate TEXT NOT NULL,
                object TEXT NOT NULL,
                valid_from TEXT,
                valid_to TEXT,
                confidence REAL DEFAULT 1.0,
                source_closet TEXT,
                source_file TEXT,
                extracted_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subject) REFERENCES entities(id),
                FOREIGN KEY (object) REFERENCES entities(id)
            );

            CREATE INDEX IF NOT EXISTS idx_triples_subject ON triples(subject);
            CREATE INDEX IF NOT EXISTS idx_triples_object ON triples(object);
            CREATE INDEX IF NOT EXISTS idx_triples_predicate ON triples(predicate);
            CREATE INDEX IF NOT EXISTS idx_triples_valid ON triples(valid_from, valid_to);
        """)
        conn.commit()
        conn.close()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=10)

    def _entity_id(self, name: str) -> str:
        return name.lower().replace(" ", "_").replace("'", "")

    # ── Write operations ──────────────────────────────────────────────────

    def add_entity(self, name: str, entity_type: str = "unknown", properties: dict = None):
        """Add or update an entity node."""
        eid = self._entity_id(name)
        props = json.dumps(properties or {})
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO entities (id, name, type, properties) VALUES (?, ?, ?, ?)",
            (eid, name, entity_type, props),
        )
        conn.commit()
        conn.close()
        return eid

    def add_triple(
        self,
        subject: str,
        predicate: str,
        obj: str,
        valid_from: str = None,
        valid_to: str = None,
        confidence: float = 1.0,
        source_closet: str = None,
        source_file: str = None,
    ):
        """
        Add a relationship triple: subject → predicate → object.

        Examples:
            add_triple("Max", "child_of", "Alice", valid_from="2015-04-01")
            add_triple("Max", "does", "swimming", valid_from="2025-01-01")
            add_triple("Alice", "worried_about", "Max injury", valid_from="2026-01", valid_to="2026-02")
        """
        sub_id = self._entity_id(subject)
        obj_id = self._entity_id(obj)
        pred = predicate.lower().replace(" ", "_")

        # Auto-create entities if they don't exist
        conn = self._conn()
        conn.execute("INSERT OR IGNORE INTO entities (id, name) VALUES (?, ?)", (sub_id, subject))
        conn.execute("INSERT OR IGNORE INTO entities (id, name) VALUES (?, ?)", (obj_id, obj))

        # Check for existing identical triple
        existing = conn.execute(
            "SELECT id FROM triples WHERE subject=? AND predicate=? AND object=? AND valid_to IS NULL",
            (sub_id, pred, obj_id),
        ).fetchone()

        if existing:
            conn.close()
            return existing[0]  # Already exists and still valid

        triple_id = f"t_{sub_id}_{pred}_{obj_id}_{hashlib.md5(f'{valid_from}{datetime.now().isoformat()}'.encode()).hexdigest()[:8]}"

        conn.execute(
            """INSERT INTO triples (id, subject, predicate, object, valid_from, valid_to, confidence, source_closet, source_file)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                triple_id,
                sub_id,
                pred,
                obj_id,
                valid_from,
                valid_to,
                confidence,
                source_closet,
                source_file,
            ),
        )
        conn.commit()
        conn.close()
        return triple_id

    def invalidate(self, subject: str, predicate: str, obj: str, ended: str = None):
        """Mark a relationship as no longer valid (set valid_to date)."""
        sub_id = self._entity_id(subject)
        obj_id = self._entity_id(obj)
        pred = predicate.lower().replace(" ", "_")
        ended = ended or date.today().isoformat()

        conn = self._conn()
        conn.execute(
            "UPDATE triples SET valid_to=? WHERE subject=? AND predicate=? AND object=? AND valid_to IS NULL",
            (ended, sub_id, pred, obj_id),
        )
        conn.commit()
        conn.close()

    # ── Query operations ──────────────────────────────────────────────────

    def query_entity(self, name: str, as_of: str = None, direction: str = "outgoing"):
        """
        Get all relationships for an entity.

        direction: "outgoing" (entity → ?), "incoming" (? → entity), "both"
        as_of: date string — only return facts valid at that time
        """
        eid = self._entity_id(name)
        conn = self._conn()

        results = []

        if direction in ("outgoing", "both"):
            query = "SELECT t.*, e.name as obj_name FROM triples t JOIN entities e ON t.object = e.id WHERE t.subject = ?"
            params = [eid]
            if as_of:
                query += " AND (t.valid_from IS NULL OR t.valid_from <= ?) AND (t.valid_to IS NULL OR t.valid_to >= ?)"
                params.extend([as_of, as_of])
            for row in conn.execute(query, params).fetchall():
                results.append(
                    {
                        "direction": "outgoing",
                        "subject": name,
                        "predicate": row[2],
                        "object": row[10],  # obj_name
                        "valid_from": row[4],
                        "valid_to": row[5],
                        "confidence": row[6],
                        "source_closet": row[7],
                        "current": row[5] is None,
                    }
                )

        if direction in ("incoming", "both"):
            query = "SELECT t.*, e.name as sub_name FROM triples t JOIN entities e ON t.subject = e.id WHERE t.object = ?"
            params = [eid]
            if as_of:
                query += " AND (t.valid_from IS NULL OR t.valid_from <= ?) AND (t.valid_to IS NULL OR t.valid_to >= ?)"
                params.extend([as_of, as_of])
            for row in conn.execute(query, params).fetchall():
                results.append(
                    {
                        "direction": "incoming",
                        "subject": row[10],  # sub_name
                        "predicate": row[2],
                        "object": name,
                        "valid_from": row[4],
                        "valid_to": row[5],
                        "confidence": row[6],
                        "source_closet": row[7],
                        "current": row[5] is None,
                    }
                )

        conn.close()
        return results

    def query_relationship(self, predicate: str, as_of: str = None):
        """Get all triples with a given relationship type."""
        pred = predicate.lower().replace(" ", "_")
        conn = self._conn()
        query = """
            SELECT t.*, s.name as sub_name, o.name as obj_name
            FROM triples t
            JOIN entities s ON t.subject = s.id
            JOIN entities o ON t.object = o.id
            WHERE t.predicate = ?
        """
        params = [pred]
        if as_of:
            query += " AND (t.valid_from IS NULL OR t.valid_from <= ?) AND (t.valid_to IS NULL OR t.valid_to >= ?)"
            params.extend([as_of, as_of])

        results = []
        for row in conn.execute(query, params).fetchall():
            results.append(
                {
                    "subject": row[10],
                    "predicate": pred,
                    "object": row[11],
                    "valid_from": row[4],
                    "valid_to": row[5],
                    "current": row[5] is None,
                }
            )
        conn.close()
        return results

    def timeline(self, entity_name: str = None):
        """Get all facts in chronological order, optionally filtered by entity."""
        conn = self._conn()
        if entity_name:
            eid = self._entity_id(entity_name)
            rows = conn.execute(
                """
                SELECT t.*, s.name as sub_name, o.name as obj_name
                FROM triples t
                JOIN entities s ON t.subject = s.id
                JOIN entities o ON t.object = o.id
                WHERE (t.subject = ? OR t.object = ?)
                ORDER BY t.valid_from ASC NULLS LAST
            """,
                (eid, eid),
            ).fetchall()
        else:
            rows = conn.execute("""
                SELECT t.*, s.name as sub_name, o.name as obj_name
                FROM triples t
                JOIN entities s ON t.subject = s.id
                JOIN entities o ON t.object = o.id
                ORDER BY t.valid_from ASC NULLS LAST
                LIMIT 100
            """).fetchall()

        conn.close()
        return [
            {
                "subject": r[10],
                "predicate": r[2],
                "object": r[11],
                "valid_from": r[4],
                "valid_to": r[5],
                "current": r[5] is None,
            }
            for r in rows
        ]

    # ── Stats ─────────────────────────────────────────────────────────────

    def stats(self):
        conn = self._conn()
        entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        triples = conn.execute("SELECT COUNT(*) FROM triples").fetchone()[0]
        current = conn.execute("SELECT COUNT(*) FROM triples WHERE valid_to IS NULL").fetchone()[0]
        expired = triples - current
        predicates = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT predicate FROM triples ORDER BY predicate"
            ).fetchall()
        ]
        conn.close()
        return {
            "entities": entities,
            "triples": triples,
            "current_facts": current,
            "expired_facts": expired,
            "relationship_types": predicates,
        }

    # ── Triple extraction from text ────────────────────────────────────────

    # Patterns that extract (subject, predicate, object) from prose
    _RELATION_PATTERNS = [
        # "X decided to use Y" → (X, decided_to_use, Y)
        (r"\b([A-Z][a-z]+)\s+decided\s+to\s+(?:use|go with|switch to)\s+(\w[\w\s]{1,30}?)(?:\.|,|\s+(?:because|instead|for|over))", "decided_to_use"),
        # "X switched from Y to Z" → (X, switched_to, Z) + (X, switched_from, Y)
        (r"\b([A-Z][a-z]+)\s+switched\s+from\s+(\w[\w\s]{1,20}?)\s+to\s+(\w[\w\s]{1,20}?)(?:\.|,|\s)", "switched"),
        # "X works on Y" / "X is working on Y"
        (r"\b([A-Z][a-z]+)\s+(?:works?|is working|worked)\s+on\s+(\w[\w\s]{1,30}?)(?:\.|,|\s+(?:and|but|with))", "works_on"),
        # "X loves Y" / "X likes Y"
        (r"\b([A-Z][a-z]+)\s+(?:loves?|likes?|enjoys?|prefers?)\s+(\w[\w\s]{1,25}?)(?:\.|,|\s+(?:and|but|because))", "loves"),
        # "X is Y's Z" — e.g. "Max is Alice's son"
        (r"\b([A-Z][a-z]+)\s+is\s+([A-Z][a-z]+)'s\s+(\w+)", "is_relative_of"),
        # "X uses Y" / "we use Y"
        (r"\b(?:[Ww]e|[A-Z][a-z]+)\s+(?:use|uses|used|are using)\s+(\w[\w\s]{1,25}?)(?:\s+(?:for|because|instead|as|to)\b|\.|,)", "uses"),
        # "X built Y" / "X created Y"
        (r"\b([A-Z][a-z]+)\s+(?:built|created|made|wrote|designed)\s+(\w[\w\s]{1,30}?)(?:\.|,|\s+(?:and|but|for|with))", "created"),
        # "X started Y" / "X founded Y"
        (r"\b([A-Z][a-z]+)\s+(?:started|founded|launched|began)\s+(\w[\w\s]{1,30}?)(?:\.|,|\s+(?:and|but|in|on))", "started"),
        # "X is a Y" — type/role
        (r"\b([A-Z][a-z]+)\s+is\s+(?:a|an|the)\s+(\w[\w\s]{1,25}?)(?:\.|,|\s+(?:who|that|and|but))", "is_a"),
        # "X has Y" — possession/attribute
        (r"\b([A-Z][a-z]+)\s+has\s+(?:a|an|the)?\s*(\w[\w\s]{1,25}?)(?:\.|,|\s+(?:and|but|that))", "has"),
    ]

    _TOOL_PATTERNS = [
        # "decided to use X because" / "we chose X" / "went with X"
        (r"\b(?:decided to use|chose|went with|picked|settled on|switched to)\s+(\w[\w\s./-]{1,30}?)(?:\s+(?:because|instead|for|over|as)\b|\.|,)", "tool_decision"),
        # "migrated from X to Y"
        (r"\b(?:migrated|moved|switched)\s+from\s+(\w[\w\s]{1,20}?)\s+to\s+(\w[\w\s]{1,20}?)(?:\.|,|\s)", "migration"),
        # "replaced X with Y"
        (r"\b(?:replaced|swapped)\s+(\w[\w\s]{1,20}?)\s+with\s+(\w[\w\s]{1,20}?)(?:\.|,|\s)", "replacement"),
    ]

    # Common non-entity words to skip
    _SKIP_SUBJECTS = {
        "the", "this", "that", "here", "there", "it", "they", "we", "i",
        "my", "your", "our", "just", "also", "still", "even", "well",
        "now", "then", "but", "and", "not", "yes", "no", "so",
    }

    def extract_triples_from_text(self, text, source_file=None):
        """
        Extract entity-relationship triples from plain text using pattern matching.

        Returns list of dicts: [{"subject": str, "predicate": str, "object": str}]
        Also auto-inserts them into the knowledge graph.
        """
        extracted = []
        text_clean = text.replace("\n", " ").strip()

        # Pass 1: Named entity relations
        for pattern, predicate in self._RELATION_PATTERNS:
            for match in re.finditer(pattern, text_clean):
                groups = match.groups()
                if predicate == "switched" and len(groups) == 3:
                    subj, old, new = [g.strip().rstrip(".,;:") for g in groups]
                    if subj.lower() in self._SKIP_SUBJECTS:
                        continue
                    extracted.append({"subject": subj, "predicate": "switched_from", "object": old})
                    extracted.append({"subject": subj, "predicate": "switched_to", "object": new})
                elif predicate == "is_relative_of" and len(groups) == 3:
                    subj, parent, role = [g.strip().rstrip(".,;:") for g in groups]
                    extracted.append({"subject": subj, "predicate": role + "_of", "object": parent})
                elif predicate == "uses" and len(groups) == 1:
                    obj = groups[0].strip().rstrip(".,;:")
                    if len(obj) > 1:
                        extracted.append({"subject": "project", "predicate": "uses", "object": obj})
                elif len(groups) >= 2:
                    subj = groups[0].strip().rstrip(".,;:")
                    obj = groups[1].strip().rstrip(".,;:")
                    if subj.lower() in self._SKIP_SUBJECTS:
                        continue
                    if len(subj) > 1 and len(obj) > 1:
                        extracted.append({"subject": subj, "predicate": predicate, "object": obj})

        # Pass 2: Tool/tech decisions (often don't have named subjects)
        for pattern, predicate in self._TOOL_PATTERNS:
            for match in re.finditer(pattern, text_clean, re.IGNORECASE):
                groups = match.groups()
                if predicate == "tool_decision" and len(groups) == 1:
                    tool = groups[0].strip().rstrip(".,;:")
                    if len(tool) > 1:
                        extracted.append({"subject": "project", "predicate": "uses", "object": tool})
                elif predicate in ("migration", "replacement") and len(groups) == 2:
                    old, new = [g.strip().rstrip(".,;:") for g in groups]
                    if len(old) > 1 and len(new) > 1:
                        extracted.append({"subject": "project", "predicate": "migrated_from", "object": old})
                        extracted.append({"subject": "project", "predicate": "migrated_to", "object": new})

        # Deduplicate
        seen = set()
        unique = []
        for t in extracted:
            key = (t["subject"].lower(), t["predicate"], t["object"].lower())
            if key not in seen:
                seen.add(key)
                unique.append(t)

        # Insert into KG
        for t in unique:
            self.add_triple(
                t["subject"], t["predicate"], t["object"],
                source_file=source_file,
            )

        return unique

    # ── Contradiction detection ──────────────────────────────────────────

    # Predicates where having two different current objects is a contradiction
    _EXCLUSIVE_PREDICATES = {
        "uses", "switched_to", "migrated_to", "lives_in", "works_at",
        "married_to", "is_a", "runs_on", "deployed_on", "hosted_on",
    }

    # Predicate pairs that are inherently contradictory
    _CONTRADICTORY_PAIRS = {
        ("loves", "hates"), ("hates", "loves"),
        ("uses", "abandoned"), ("abandoned", "uses"),
        ("started", "never_started"), ("never_started", "started"),
    }

    def check_contradiction(self, subject, predicate, obj):
        """
        Check if a new fact contradicts existing knowledge.

        Returns:
            dict with "has_contradiction" bool and "conflicts" list.
            Each conflict has the existing fact and the type of contradiction.
        """
        sub_id = self._entity_id(subject)
        pred = predicate.lower().replace(" ", "_")
        obj_id = self._entity_id(obj)
        conflicts = []

        conn = self._conn()

        # Check 1: Exclusive predicates — same subject+predicate but different object
        if pred in self._EXCLUSIVE_PREDICATES:
            rows = conn.execute(
                """SELECT t.*, o.name as obj_name FROM triples t
                   JOIN entities o ON t.object = o.id
                   WHERE t.subject = ? AND t.predicate = ? AND t.valid_to IS NULL
                   AND t.object != ?""",
                (sub_id, pred, obj_id),
            ).fetchall()
            for row in rows:
                conflicts.append({
                    "type": "exclusive_predicate",
                    "existing_fact": f"{subject} → {pred} → {row[10]}",
                    "new_fact": f"{subject} → {pred} → {obj}",
                    "explanation": f"'{pred}' is typically exclusive — {subject} can't {pred} both '{row[10]}' and '{obj}' simultaneously",
                    "existing_triple_id": row[0],
                })

        # Check 2: Contradictory predicate pairs
        for p1, p2 in self._CONTRADICTORY_PAIRS:
            if pred == p1:
                rows = conn.execute(
                    """SELECT t.*, o.name as obj_name FROM triples t
                       JOIN entities o ON t.object = o.id
                       WHERE t.subject = ? AND t.predicate = ? AND t.object = ?
                       AND t.valid_to IS NULL""",
                    (sub_id, p2, obj_id),
                ).fetchall()
                for row in rows:
                    conflicts.append({
                        "type": "contradictory_predicate",
                        "existing_fact": f"{subject} → {p2} → {row[10]}",
                        "new_fact": f"{subject} → {pred} → {obj}",
                        "explanation": f"'{pred}' contradicts existing '{p2}' for the same entities",
                        "existing_triple_id": row[0],
                    })

        # Check 3: Direct negation — same subject, same predicate, same object
        # but the existing one was invalidated (might indicate flip-flopping)
        rows = conn.execute(
            """SELECT t.valid_from, t.valid_to FROM triples t
               WHERE t.subject = ? AND t.predicate = ? AND t.object = ?
               AND t.valid_to IS NOT NULL
               ORDER BY t.valid_to DESC LIMIT 1""",
            (sub_id, pred, obj_id),
        ).fetchall()
        if rows:
            conflicts.append({
                "type": "previously_invalidated",
                "existing_fact": f"{subject} → {pred} → {obj} (was invalidated on {rows[0][1]})",
                "new_fact": f"{subject} → {pred} → {obj}",
                "explanation": "This fact was previously true but was invalidated. Re-adding it — verify this is intentional.",
            })

        conn.close()

        return {
            "has_contradiction": len(conflicts) > 0,
            "conflicts": conflicts,
        }

    def add_triple_with_contradiction_check(
        self, subject, predicate, obj,
        valid_from=None, valid_to=None, confidence=1.0,
        source_closet=None, source_file=None,
        auto_resolve=True,
    ):
        """
        Add a triple, but check for contradictions first.
        If auto_resolve=True, automatically invalidates conflicting exclusive predicates.

        Returns dict with triple_id, contradictions found, and any auto-resolved facts.
        """
        check = self.check_contradiction(subject, predicate, obj)
        resolved = []

        if check["has_contradiction"] and auto_resolve:
            for conflict in check["conflicts"]:
                if conflict["type"] == "exclusive_predicate":
                    # Auto-invalidate the old exclusive fact
                    old_triple_id = conflict.get("existing_triple_id")
                    if old_triple_id:
                        conn = self._conn()
                        conn.execute(
                            "UPDATE triples SET valid_to = ? WHERE id = ? AND valid_to IS NULL",
                            (date.today().isoformat(), old_triple_id),
                        )
                        conn.commit()
                        conn.close()
                        resolved.append(conflict["existing_fact"])

        triple_id = self.add_triple(
            subject, predicate, obj,
            valid_from=valid_from, valid_to=valid_to,
            confidence=confidence, source_closet=source_closet,
            source_file=source_file,
        )

        return {
            "triple_id": triple_id,
            "contradictions": check["conflicts"],
            "auto_resolved": resolved,
        }

    # ── Seed from known facts ─────────────────────────────────────────────

    def seed_from_entity_facts(self, entity_facts: dict):
        """
        Seed the knowledge graph from fact_checker.py ENTITY_FACTS.
        This bootstraps the graph with known ground truth.
        """
        for key, facts in entity_facts.items():
            name = facts.get("full_name", key.capitalize())
            etype = facts.get("type", "person")
            self.add_entity(
                name,
                etype,
                {
                    "gender": facts.get("gender", ""),
                    "birthday": facts.get("birthday", ""),
                },
            )

            # Relationships
            parent = facts.get("parent")
            if parent:
                self.add_triple(
                    name, "child_of", parent.capitalize(), valid_from=facts.get("birthday")
                )

            partner = facts.get("partner")
            if partner:
                self.add_triple(name, "married_to", partner.capitalize())

            relationship = facts.get("relationship", "")
            if relationship == "daughter":
                self.add_triple(
                    name,
                    "is_child_of",
                    facts.get("parent", "").capitalize() or name,
                    valid_from=facts.get("birthday"),
                )
            elif relationship == "husband":
                self.add_triple(name, "is_partner_of", facts.get("partner", name).capitalize())
            elif relationship == "brother":
                self.add_triple(name, "is_sibling_of", facts.get("sibling", name).capitalize())
            elif relationship == "dog":
                self.add_triple(name, "is_pet_of", facts.get("owner", name).capitalize())
                self.add_entity(name, "animal")

            # Interests
            for interest in facts.get("interests", []):
                self.add_triple(name, "loves", interest.capitalize(), valid_from="2025-01-01")
