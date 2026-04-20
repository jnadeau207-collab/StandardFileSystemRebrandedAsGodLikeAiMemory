"""MemPalace — Give your AI a memory. No API key required."""

__version__ = "3.0.0"

from .cli import main
from .structure_store import StructureStore
from .structure import NodeType, LinkType, new_memory_id
from .tracing import local_lineage, absolute_lineage
from .structure_helpers import StructureManager
from .migration import migrate_legacy_flat_drawers

__all__ = [
    "main",
    "__version__",
    "StructureStore",
    "NodeType",
    "LinkType",
    "new_memory_id",
    "local_lineage",
    "absolute_lineage",
    "StructureManager",
    "migrate_legacy_flat_drawers",
]
