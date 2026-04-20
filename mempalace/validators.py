"""Invariant validation helpers for recursive palace structure."""


def ensure(condition: bool, message: str):
    if not condition:
        raise ValueError(message)


def ensure_no_domain_cycle(store, candidate_parent_domain_id: str | None):
    """Reject cycles in domain ancestry."""

    seen = set()
    current = candidate_parent_domain_id
    while current is not None:
        ensure(current not in seen, "Domain ancestry cycle detected")
        seen.add(current)
        parent = store._fetch_one(
            "SELECT parent_domain_id FROM domains WHERE domain_id = ?",
            (current,),
        )
        current = parent[0] if parent else None


def ensure_no_node_cycle(store, domain_id: str, candidate_parent_node_id: str | None):
    """Reject cycles in node parentage within a single domain."""

    seen = set()
    current = candidate_parent_node_id
    while current is not None:
        ensure(current not in seen, "Node parentage cycle detected")
        seen.add(current)
        parent = store._fetch_one(
            "SELECT parent_node_id FROM nodes WHERE domain_id = ? AND node_id = ?",
            (domain_id, current),
        )
        current = parent[0] if parent else None
