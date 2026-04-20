"""High-level helper APIs for structure-aware filing and recursive domain authoring."""

from __future__ import annotations

from pathlib import Path

from .structure_store import StructureStore


class StructureManager:
    """Compatibility and authoring helpers layered on top of StructureStore."""

    def __init__(self, structure_db_path: str | Path):
        self.store = StructureStore(structure_db_path)

    def close(self):
        self.store.close()

    def resolve_ordinary_container(self, wing: str, room: str, domain_id: str | None = None) -> dict:
        return self.store.resolve_ordinary_container(wing=wing, room=room, domain_id=domain_id)

    def create_gateway_anchor(
        self,
        domain_id: str,
        parent_node_id: str,
        label: str,
        flavor: str | None = None,
    ) -> dict:
        anchor = self.store.create_gateway_anchor(
            domain_id=domain_id,
            parent_node_id=parent_node_id,
            label=label,
            flavor=flavor,
        )
        gateway = self.store.get_or_create_gateway(
            domain_id=domain_id,
            node_id=anchor.node_id,
            label=label,
            flavor=flavor,
        )
        return {
            "gateway_id": gateway.gateway_id,
            "gateway_node_id": anchor.node_id,
            "domain_id": domain_id,
            "flavor": flavor,
        }

    def create_subordinate_domain(
        self,
        parent_domain_id: str,
        entry_gateway_id: str,
        label: str,
    ) -> dict:
        domain, root = self.store.get_or_create_subdomain(
            parent_domain_id=parent_domain_id,
            entry_gateway_id=entry_gateway_id,
            label=label,
        )
        return {
            "domain_id": domain.domain_id,
            "root_node_id": root.node_id,
            "parent_domain_id": domain.parent_domain_id,
            "entry_gateway_id": domain.entry_gateway_id,
        }

    def create_nested_subordinate_domain(
        self,
        parent_domain_id: str,
        parent_node_id: str,
        gateway_label: str,
        subdomain_label: str,
        flavor: str | None = None,
    ) -> dict:
        gateway = self.create_gateway_anchor(
            domain_id=parent_domain_id,
            parent_node_id=parent_node_id,
            label=gateway_label,
            flavor=flavor,
        )
        return self.create_subordinate_domain(
            parent_domain_id=parent_domain_id,
            entry_gateway_id=gateway["gateway_id"],
            label=subdomain_label,
        )

    def file_drawer_to_node(self, domain_id: str, container_node_id: str) -> dict:
        return self.store.file_drawer_to_node(domain_id=domain_id, container_node_id=container_node_id)
