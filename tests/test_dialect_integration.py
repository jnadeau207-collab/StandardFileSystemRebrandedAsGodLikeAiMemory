import os
import tempfile
import shutil
import yaml
import chromadb
from mempalace.miner import mine
from mempalace.dialect import Dialect


def test_compression_runs_during_mining():
    """Verify that mining now auto-generates compressed drawers."""
    tmpdir = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmpdir, "backend"))
    with open(os.path.join(tmpdir, "backend", "app.py"), "w") as f:
        f.write("def main():\n    print('hello world')\n" * 20)
    with open(os.path.join(tmpdir, "mempalace.yaml"), "w") as f:
        yaml.dump(
            {
                "wing": "test_project",
                "rooms": [
                    {"name": "backend", "description": "Backend code"},
                    {"name": "general", "description": "General"},
                ],
            },
            f,
        )

    palace_path = os.path.join(tmpdir, "palace")
    mine(tmpdir, palace_path)

    # Check compressed collection exists and has entries
    client = chromadb.PersistentClient(path=palace_path)
    comp_col = client.get_collection("mempalace_compressed")
    assert comp_col.count() > 0

    # Check that compressed entries have metadata
    compressed = comp_col.get(include=["documents", "metadatas"])

    # At least some compression should have happened
    assert compressed["metadatas"][0].get("compression_ratio") is not None

    shutil.rmtree(tmpdir)


def test_dialect_compress_plain_text():
    """Test that Dialect.compress works on plain text."""
    dialect = Dialect()
    text = "We decided to use GraphQL instead of REST because it handles nested queries better. Alice loves the new API design."
    compressed = dialect.compress(text)

    assert len(compressed) > 0
    assert len(compressed) < len(text)
    assert "|" in compressed  # AAAK format uses pipe separators


def test_dialect_compress_with_metadata():
    """Test compression with metadata produces header line."""
    dialect = Dialect()
    text = "The backend service handles authentication and authorization."
    compressed = dialect.compress(text, metadata={
        "wing": "my_app",
        "room": "backend",
        "source_file": "auth.py",
    })

    assert "my_app" in compressed
    assert "backend" in compressed


def test_dialect_roundtrip_stats():
    """Test that compression stats are reasonable."""
    dialect = Dialect()
    text = "We decided to switch from PostgreSQL to MongoDB because our data model is document-oriented. " * 5
    compressed = dialect.compress(text)
    stats = dialect.compression_stats(text, compressed)

    assert stats["ratio"] > 1.0  # should actually compress
    assert stats["original_tokens"] > stats["compressed_tokens"]
