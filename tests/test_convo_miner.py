import os
import tempfile
import shutil
import chromadb
from mempalace.convo_miner import mine_convos


def test_convo_mining():
    tmpdir = tempfile.mkdtemp()
    with open(os.path.join(tmpdir, "chat.txt"), "w") as f:
        f.write(
            "> What is memory?\nMemory is persistence.\n\n> Why does it matter?\nIt enables continuity.\n\n> How do we build it?\nWith structured storage.\n"
        )

    palace_path = os.path.join(tmpdir, "palace")
    mine_convos(tmpdir, palace_path, wing="test_convos")

    client = chromadb.PersistentClient(path=palace_path)
    col = client.get_collection("mempalace_drawers")
    assert col.count() >= 2

    # Verify data is present
    results = col.get(limit=1, include=["documents"])
    assert len(results["documents"]) > 0
    row = col.get(limit=1, include=["metadatas"])
    meta = row["metadatas"][0]
    assert meta["wing"] == "test_convos"
    assert "room" in meta
    assert "source_file" in meta
    assert "chunk_index" in meta
    assert "domain_id" in meta
    assert "container_node_id" in meta

    shutil.rmtree(tmpdir)
