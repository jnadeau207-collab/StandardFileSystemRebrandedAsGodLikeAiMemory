"""SQLite-backed durable structure metadata store for recursive palace domains."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .structure import (
    DomainRecord,
    GatewayRecord,
    NodeRecord,
    NodeType,
    new_domain_id,
    new_gateway_id,
    new_node_id,
    now_iso,
)
from .validators import ensure, ensure_no_domain_cycle, ensure_no_node_cycle


class StructureStore:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def close(self):
        self.conn.close()

    def _init_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS domains (
                domain_id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                parent_domain_id TEXT NULL REFERENCES domains(domain_id),
                entry_gateway_id TEXT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
                node_type TEXT NOT NULL,
                label TEXT NOT NULL,
                flavor TEXT NULL,
                parent_node_id TEXT NULL REFERENCES nodes(node_id) ON DELETE RESTRICT,
                is_root INTEGER NOT NULL CHECK(is_root IN (0,1)),
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS gateways (
                gateway_id TEXT PRIMARY KEY,
                domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
                node_id TEXT NOT NULL UNIQUE REFERENCES nodes(node_id) ON DELETE CASCADE,
                flavor TEXT NULL,
                label TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_domain ON nodes(domain_id);
            CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_node_id);
            CREATE INDEX IF NOT EXISTS idx_domains_parent ON domains(parent_domain_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_identity
                ON nodes(domain_id, ifnull(parent_node_id, ''), node_type, label);
            """
        )
        self.conn.commit()

    def _fetch_one(self, sql: str, params=()):
        return self.conn.execute(sql, params).fetchone()

    @staticmethod
    def default_db_path(base_path: str | Path) -> Path:
        return Path(base_path).expanduser().resolve() / "structure.sqlite3"

    def create_domain(
        self,
        label: str,
        domain_id: str | None = None,
        parent_domain_id: str | None = None,
        entry_gateway_id: str | None = None,
    ) -> DomainRecord:
        ensure(label.strip() != "", "Domain label cannot be empty")
        ensure(
            (parent_domain_id is None) == (entry_gateway_id is None),
            "Child domain must provide both parent_domain_id and entry_gateway_id",
        )

        if parent_domain_id:
            ensure(self.domain_exists(parent_domain_id), "Parent domain does not exist")
            ensure(self.gateway_exists(entry_gateway_id), "Entry gateway does not exist")
            ensure_no_domain_cycle(self, parent_domain_id)

            row = self._fetch_one(
                "SELECT domain_id FROM gateways WHERE gateway_id = ?", (entry_gateway_id,)
            )
            ensure(row and row[0] == parent_domain_id, "Entry gateway must belong to parent domain")

        domain_id = domain_id or new_domain_id()
        created_at = now_iso()
        self.conn.execute(
            """
            INSERT INTO domains(domain_id, label, parent_domain_id, entry_gateway_id, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (domain_id, label, parent_domain_id, entry_gateway_id, created_at),
        )
        self.conn.commit()
        return DomainRecord(domain_id, label, parent_domain_id, entry_gateway_id, created_at)

    def get_or_create_domain(
        self,
        label: str,
        parent_domain_id: str | None = None,
        entry_gateway_id: str | None = None,
    ) -> DomainRecord:
        row = self._fetch_one(
            """
            SELECT * FROM domains
            WHERE label = ? AND parent_domain_id IS ? AND entry_gateway_id IS ?
            """,
            (label, parent_domain_id, entry_gateway_id),
        )
        if row is not None:
            return self.get_domain(row["domain_id"])
        return self.create_domain(
            label=label,
            parent_domain_id=parent_domain_id,
            entry_gateway_id=entry_gateway_id,
        )

    def create_node(
        self,
        domain_id: str,
        label: str,
        node_type: str,
        parent_node_id: str | None = None,
        flavor: str | None = None,
        is_root: bool = False,
        node_id: str | None = None,
    ) -> NodeRecord:
        ensure(self.domain_exists(domain_id), "Domain does not exist")
        ensure(label.strip() != "", "Node label cannot be empty")

        if parent_node_id is not None:
            row = self._fetch_one("SELECT domain_id FROM nodes WHERE node_id = ?", (parent_node_id,))
            ensure(row is not None, "Parent node does not exist")
            ensure(row[0] == domain_id, "Parent node must belong to same domain")
            ensure(not is_root, "Root node cannot have parent")
        else:
            ensure(is_root, "Node without parent must be a root node")

        ensure_no_node_cycle(self, domain_id, parent_node_id)

        if is_root:
            existing_root = self._fetch_one(
                "SELECT node_id FROM nodes WHERE domain_id = ? AND is_root = 1",
                (domain_id,),
            )
            ensure(existing_root is None, "Domain already has a root node")

        node_id = node_id or new_node_id()
        created_at = now_iso()
        try:
            self.conn.execute(
                """
                INSERT INTO nodes(node_id, domain_id, node_type, label, flavor, parent_node_id, is_root, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    domain_id,
                    node_type,
                    label,
                    flavor,
                    parent_node_id,
                    int(is_root),
                    created_at,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"Node creation failed: {exc}") from exc
        self.conn.commit()
        return NodeRecord(node_id, domain_id, node_type, label, flavor, parent_node_id, is_root, created_at)

    def get_or_create_node(
        self,
        domain_id: str,
        label: str,
        node_type: str,
        parent_node_id: str | None,
        flavor: str | None = None,
        is_root: bool = False,
    ) -> NodeRecord:
        row = self._fetch_one(
            """
            SELECT * FROM nodes
            WHERE domain_id = ? AND node_type = ? AND label = ? AND parent_node_id IS ?
            """,
            (domain_id, node_type, label, parent_node_id),
        )
        if row is not None:
            if flavor is not None and row["flavor"] not in (None, flavor):
                raise ValueError("Ambiguous node flavor for deterministic container")
            return self.get_node(row["node_id"])
        return self.create_node(
            domain_id=domain_id,
            label=label,
            node_type=node_type,
            parent_node_id=parent_node_id,
            flavor=flavor,
            is_root=is_root,
        )

    def create_gateway(
        self,
        domain_id: str,
        node_id: str,
        label: str,
        flavor: str | None = None,
        gateway_id: str | None = None,
    ) -> GatewayRecord:
        ensure(self.domain_exists(domain_id), "Domain does not exist")
        node = self._fetch_one("SELECT domain_id, node_type FROM nodes WHERE node_id = ?", (node_id,))
        ensure(node is not None, "Gateway anchor node does not exist")
        ensure(node[0] == domain_id, "Gateway anchor must belong to the same domain")
        ensure(node[1] == NodeType.GATEWAY_ANCHOR.value, "Gateway must anchor to gateway_anchor node")

        gateway_id = gateway_id or new_gateway_id()
        created_at = now_iso()
        self.conn.execute(
            """
            INSERT INTO gateways(gateway_id, domain_id, node_id, flavor, label, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (gateway_id, domain_id, node_id, flavor, label, created_at),
        )
        self.conn.commit()
        return GatewayRecord(gateway_id, domain_id, node_id, flavor, label, created_at)

    def get_or_create_gateway(
        self,
        domain_id: str,
        node_id: str,
        label: str,
        flavor: str | None = None,
    ) -> GatewayRecord:
        row = self._fetch_one("SELECT * FROM gateways WHERE node_id = ?", (node_id,))
        if row is not None:
            if flavor is not None and row["flavor"] not in (None, flavor):
                raise ValueError("Ambiguous gateway flavor for deterministic gateway")
            return GatewayRecord(
                row["gateway_id"],
                row["domain_id"],
                row["node_id"],
                row["flavor"],
                row["label"],
                row["created_at"],
            )
        return self.create_gateway(domain_id=domain_id, node_id=node_id, label=label, flavor=flavor)

    def create_subdomain(
        self,
        parent_domain_id: str,
        entry_gateway_id: str,
        label: str,
    ) -> tuple[DomainRecord, NodeRecord]:
        domain = self.create_domain(
            label=label,
            parent_domain_id=parent_domain_id,
            entry_gateway_id=entry_gateway_id,
        )
        root = self.create_node(
            domain_id=domain.domain_id,
            label=f"{label}:root",
            node_type=NodeType.SUBORDINATE_DOMAIN_ROOT.value,
            is_root=True,
        )
        return domain, root

    def get_or_create_subdomain(
        self,
        parent_domain_id: str,
        entry_gateway_id: str,
        label: str,
    ) -> tuple[DomainRecord, NodeRecord]:
        row = self._fetch_one(
            """
            SELECT domain_id FROM domains
            WHERE parent_domain_id = ? AND entry_gateway_id = ?
            """,
            (parent_domain_id, entry_gateway_id),
        )
        if row is not None:
            domain = self.get_domain(row["domain_id"])
            root = self.get_root_node(domain.domain_id)
            return domain, root
        return self.create_subdomain(parent_domain_id, entry_gateway_id, label)

    def ensure_main_domain(self, label: str = "main") -> tuple[DomainRecord, NodeRecord]:
        roots = self.conn.execute(
            "SELECT * FROM domains WHERE parent_domain_id IS NULL"
        ).fetchall()
        if len(roots) > 1:
            raise ValueError("Ambiguous root domains; cannot resolve main domain deterministically")
        if len(roots) == 1:
            domain = self.get_domain(roots[0]["domain_id"])
        else:
            domain = self.create_domain(label=label)

        root = self.get_root_node(domain.domain_id)
        if root is None:
            root = self.create_node(
                domain_id=domain.domain_id,
                label=f"{domain.label}:root",
                node_type=NodeType.DOMAIN_ROOT.value,
                is_root=True,
            )
        return domain, root

    def get_root_node(self, domain_id: str) -> NodeRecord | None:
        row = self._fetch_one(
            "SELECT node_id FROM nodes WHERE domain_id = ? AND is_root = 1", (domain_id,)
        )
        return self.get_node(row["node_id"]) if row else None

    def resolve_ordinary_container(self, wing: str, room: str, domain_id: str | None = None) -> dict:
        if domain_id is None:
            domain, domain_root = self.ensure_main_domain()
        else:
            domain = self.get_domain(domain_id)
            ensure(domain is not None, "Domain does not exist")
            domain_root = self.get_root_node(domain_id)
            ensure(domain_root is not None, "Domain has no root node")

        wing_node = self.get_or_create_node(
            domain_id=domain.domain_id,
            label=wing,
            node_type=NodeType.WING.value,
            parent_node_id=domain_root.node_id,
        )
        room_node = self.get_or_create_node(
            domain_id=domain.domain_id,
            label=room,
            node_type=NodeType.ROOM.value,
            parent_node_id=wing_node.node_id,
        )
        return {
            "domain_id": domain.domain_id,
            "root_node_id": domain_root.node_id,
            "wing_node_id": wing_node.node_id,
            "container_node_id": room_node.node_id,
        }

    def create_gateway_anchor(
        self,
        domain_id: str,
        parent_node_id: str,
        label: str,
        flavor: str | None = None,
    ) -> NodeRecord:
        return self.get_or_create_node(
            domain_id=domain_id,
            label=label,
            node_type=NodeType.GATEWAY_ANCHOR.value,
            parent_node_id=parent_node_id,
            flavor=flavor,
        )

    def file_drawer_to_node(
        self,
        domain_id: str,
        container_node_id: str,
    ) -> dict:
        node = self.get_node(container_node_id)
        ensure(node is not None, "Container node does not exist")
        ensure(node.domain_id == domain_id, "Container/domain mismatch")
        return {
            "domain_id": domain_id,
            "container_node_id": container_node_id,
        }

    def domain_exists(self, domain_id: str | None) -> bool:
        if domain_id is None:
            return False
        return self._fetch_one("SELECT 1 FROM domains WHERE domain_id = ?", (domain_id,)) is not None

    def gateway_exists(self, gateway_id: str | None) -> bool:
        if gateway_id is None:
            return False
        return self._fetch_one("SELECT 1 FROM gateways WHERE gateway_id = ?", (gateway_id,)) is not None

    def get_node(self, node_id: str) -> NodeRecord | None:
        row = self._fetch_one("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        if row is None:
            return None
        return NodeRecord(
            row["node_id"],
            row["domain_id"],
            row["node_type"],
            row["label"],
            row["flavor"],
            row["parent_node_id"],
            bool(row["is_root"]),
            row["created_at"],
        )

    def get_domain(self, domain_id: str) -> DomainRecord | None:
        row = self._fetch_one("SELECT * FROM domains WHERE domain_id = ?", (domain_id,))
        if row is None:
            return None
        return DomainRecord(
            row["domain_id"],
            row["label"],
            row["parent_domain_id"],
            row["entry_gateway_id"],
            row["created_at"],
        )
