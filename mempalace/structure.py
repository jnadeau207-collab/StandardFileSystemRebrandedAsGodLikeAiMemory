"""Structural primitives for recursive gateway-opened palace domains."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import uuid


class NodeType(str, Enum):
    """Canonical node types independent from optional semantic flavors."""

    DOMAIN_ROOT = "domain_root"
    WING = "wing"
    HALL = "hall"
    ROOM = "room"
    CLOSET = "closet"
    DRAWER = "drawer"
    MEMORY = "memory"
    GATEWAY_ANCHOR = "gateway_anchor"
    SUBORDINATE_DOMAIN_ROOT = "subordinate_domain_root"
    GENERIC = "generic"


class LinkType(str, Enum):
    """Explicit linkage semantics."""

    ORDINARY_CONTAINMENT = "ordinary_containment"
    GATEWAY_DOMAIN_TRANSITION = "gateway_domain_transition"


@dataclass(frozen=True)
class DomainRecord:
    domain_id: str
    label: str
    parent_domain_id: str | None
    entry_gateway_id: str | None
    created_at: str


@dataclass(frozen=True)
class NodeRecord:
    node_id: str
    domain_id: str
    node_type: str
    label: str
    flavor: str | None
    parent_node_id: str | None
    is_root: bool
    created_at: str


@dataclass(frozen=True)
class GatewayRecord:
    gateway_id: str
    domain_id: str
    node_id: str
    flavor: str | None
    label: str
    created_at: str


@dataclass(frozen=True)
class TraceStep:
    domain_id: str
    node_id: str
    label: str
    node_type: str
    flavor: str | None
    link_type: str


def new_domain_id() -> str:
    return f"dom_{uuid.uuid4().hex}"


def new_node_id() -> str:
    return f"node_{uuid.uuid4().hex}"


def new_gateway_id() -> str:
    return f"gate_{uuid.uuid4().hex}"


def new_memory_id() -> str:
    """Canonical opaque ID for memory/drawer records where needed."""

    return f"mem_{uuid.uuid4().hex}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
