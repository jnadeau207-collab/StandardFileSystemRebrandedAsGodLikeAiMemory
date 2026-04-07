import os
import tempfile
import shutil
from mempalace.knowledge_graph import KnowledgeGraph


def _make_kg():
    """Create a KG with a temp database."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_kg.sqlite3")
    return KnowledgeGraph(db_path=db_path), tmpdir


def test_extract_triples_from_text():
    kg, tmpdir = _make_kg()
    try:
        text = "Alice decided to use GraphQL instead of REST. Bob works on the backend service."
        triples = kg.extract_triples_from_text(text, source_file="test.txt")

        # Should extract at least some triples
        assert len(triples) > 0

        # Check that subjects are real entities
        subjects = {t["subject"] for t in triples}
        assert any(s in subjects for s in ("Alice", "Bob", "project"))

        # Verify they were inserted into the KG
        stats = kg.stats()
        assert stats["triples"] > 0
    finally:
        shutil.rmtree(tmpdir)


def test_extract_tool_decisions():
    kg, tmpdir = _make_kg()
    try:
        text = "We decided to use PostgreSQL because it handles JSON well. Later we switched to MongoDB."
        triples = kg.extract_triples_from_text(text)

        predicates = {t["predicate"] for t in triples}
        objects = {t["object"].lower() for t in triples}

        # Should detect tool decisions
        assert "uses" in predicates or "decided_to_use" in predicates
        assert any("postgresql" in o or "postgres" in o for o in objects)
    finally:
        shutil.rmtree(tmpdir)


def test_contradiction_exclusive_predicate():
    kg, tmpdir = _make_kg()
    try:
        # Add initial fact
        kg.add_triple("project", "uses", "PostgreSQL")

        # Check contradiction
        result = kg.check_contradiction("project", "uses", "MongoDB")

        assert result["has_contradiction"] is True
        assert len(result["conflicts"]) > 0
        assert result["conflicts"][0]["type"] == "exclusive_predicate"
    finally:
        shutil.rmtree(tmpdir)


def test_contradiction_contradictory_pairs():
    kg, tmpdir = _make_kg()
    try:
        # Add initial fact
        kg.add_triple("Alice", "loves", "Python")

        # Check contradictory predicate
        result = kg.check_contradiction("Alice", "hates", "Python")

        assert result["has_contradiction"] is True
        assert any(c["type"] == "contradictory_predicate" for c in result["conflicts"])
    finally:
        shutil.rmtree(tmpdir)


def test_contradiction_previously_invalidated():
    kg, tmpdir = _make_kg()
    try:
        # Add then invalidate a fact
        kg.add_triple("project", "uses", "Redis")
        kg.invalidate("project", "uses", "Redis", ended="2026-01-01")

        # Re-adding should warn
        result = kg.check_contradiction("project", "uses", "Redis")

        assert result["has_contradiction"] is True
        assert any(c["type"] == "previously_invalidated" for c in result["conflicts"])
    finally:
        shutil.rmtree(tmpdir)


def test_no_contradiction():
    kg, tmpdir = _make_kg()
    try:
        kg.add_triple("Alice", "loves", "Python")

        # Different predicate, same entities — not a contradiction
        result = kg.check_contradiction("Alice", "works_on", "Python")

        assert result["has_contradiction"] is False
    finally:
        shutil.rmtree(tmpdir)


def test_add_triple_with_auto_resolve():
    kg, tmpdir = _make_kg()
    try:
        # Add initial fact
        kg.add_triple("project", "uses", "PostgreSQL")

        # Add conflicting fact with auto-resolve
        result = kg.add_triple_with_contradiction_check(
            "project", "uses", "MongoDB", auto_resolve=True
        )

        assert result["triple_id"] is not None
        assert len(result["auto_resolved"]) > 0

        # Verify old fact was invalidated
        facts = kg.query_entity("project")
        current_uses = [f for f in facts if f["predicate"] == "uses" and f["current"]]
        assert len(current_uses) == 1
        assert current_uses[0]["object"] == "MongoDB"
    finally:
        shutil.rmtree(tmpdir)
