import os
import tempfile
import shutil
import yaml
import chromadb
from mempalace.miner import mine


def test_project_mining():
    tmpdir = tempfile.mkdtemp()
    # Create a mini project
    os.makedirs(os.path.join(tmpdir, "backend"))
    with open(os.path.join(tmpdir, "backend", "app.py"), "w") as f:
        f.write("def main():\n    print('hello world')\n" * 20)
    # Create config
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

    # Verify
    client = chromadb.PersistentClient(path=palace_path)
    col = client.get_collection("mempalace_drawers")
    assert col.count() > 0
    rows = col.get(limit=1, include=["metadatas"])
    meta = rows["metadatas"][0]
    assert meta["wing"] == "test_project"
    assert "room" in meta
    assert "source_file" in meta
    assert "chunk_index" in meta
    assert "domain_id" in meta
    assert "container_node_id" in meta

    shutil.rmtree(tmpdir)
