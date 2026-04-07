import os
import tempfile
import shutil
import yaml
from mempalace.miner import mine
from mempalace.closets import generate_closets, get_closet, list_closets


def _setup_palace():
    """Create a mini palace with some drawers."""
    tmpdir = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmpdir, "backend"))
    os.makedirs(os.path.join(tmpdir, "frontend"))

    with open(os.path.join(tmpdir, "backend", "app.py"), "w") as f:
        f.write("def main():\n    print('hello world')\n" * 20)

    with open(os.path.join(tmpdir, "frontend", "index.js"), "w") as f:
        f.write("function render() { return '<h1>Hello</h1>'; }\n" * 20)

    with open(os.path.join(tmpdir, "mempalace.yaml"), "w") as f:
        yaml.dump(
            {
                "wing": "test_project",
                "rooms": [
                    {"name": "backend", "description": "Backend code"},
                    {"name": "frontend", "description": "Frontend code"},
                    {"name": "general", "description": "General"},
                ],
            },
            f,
        )

    palace_path = os.path.join(tmpdir, "palace")
    mine(tmpdir, palace_path)
    return tmpdir, palace_path


def test_generate_closets():
    tmpdir, palace_path = _setup_palace()
    try:
        closets = generate_closets(palace_path)
        assert len(closets) > 0

        # Each closet should have the =CLOSET header
        for key, text in closets.items():
            assert "=CLOSET[" in text
            assert "/" in key  # wing/room format
    finally:
        shutil.rmtree(tmpdir)


def test_generate_closets_with_wing_filter():
    tmpdir, palace_path = _setup_palace()
    try:
        closets = generate_closets(palace_path, wing="test_project")
        assert len(closets) > 0

        # All closets should be in the filtered wing
        for key in closets:
            assert key.startswith("test_project/")
    finally:
        shutil.rmtree(tmpdir)


def test_get_closet():
    tmpdir, palace_path = _setup_palace()
    try:
        # Generate first
        generate_closets(palace_path)

        # Retrieve
        closets = get_closet(palace_path, wing="test_project")
        assert len(closets) > 0
        assert all("text" in c for c in closets)
        assert all("wing" in c for c in closets)
    finally:
        shutil.rmtree(tmpdir)


def test_list_closets():
    tmpdir, palace_path = _setup_palace()
    try:
        generate_closets(palace_path)
        closet_list = list_closets(palace_path)
        assert len(closet_list) > 0
        assert all("wing" in c for c in closet_list)
        assert all("room" in c for c in closet_list)
        assert all("drawer_count" in c for c in closet_list)
    finally:
        shutil.rmtree(tmpdir)
