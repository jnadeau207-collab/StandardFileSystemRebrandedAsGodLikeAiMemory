"""Tracing helpers for local and absolute lineage across recursive domains."""

from .structure import LinkType, TraceStep


def local_lineage(store, node_id: str) -> list[TraceStep]:
    """Return ancestry inside a domain from node -> ... -> domain root."""

    node = store.get_node(node_id)
    if node is None:
        raise ValueError("Node does not exist")

    steps = [
        TraceStep(
            domain_id=node.domain_id,
            node_id=node.node_id,
            label=node.label,
            node_type=node.node_type,
            flavor=node.flavor,
            link_type=LinkType.ORDINARY_CONTAINMENT.value,
        )
    ]

    current_parent = node.parent_node_id
    while current_parent is not None:
        parent = store.get_node(current_parent)
        if parent is None:
            raise ValueError("Broken parent linkage")
        steps.append(
            TraceStep(
                domain_id=parent.domain_id,
                node_id=parent.node_id,
                label=parent.label,
                node_type=parent.node_type,
                flavor=parent.flavor,
                link_type=LinkType.ORDINARY_CONTAINMENT.value,
            )
        )
        current_parent = parent.parent_node_id
    return steps


def absolute_lineage(store, node_id: str) -> list[TraceStep]:
    """Trace node -> local root -> parent gateway -> ... -> root domain."""

    path = local_lineage(store, node_id)
    current_domain_id = path[-1].domain_id

    while True:
        domain = store.get_domain(current_domain_id)
        if domain is None:
            raise ValueError("Broken domain linkage")
        if domain.parent_domain_id is None:
            break

        gateway_row = store._fetch_one(
            """
            SELECT g.gateway_id, g.node_id, n.label, n.node_type, n.flavor, n.domain_id
            FROM gateways g
            JOIN nodes n ON n.node_id = g.node_id
            WHERE g.gateway_id = ?
            """,
            (domain.entry_gateway_id,),
        )
        if gateway_row is None:
            raise ValueError("Broken entry gateway linkage")

        path.append(
            TraceStep(
                domain_id=gateway_row["domain_id"],
                node_id=gateway_row["node_id"],
                label=gateway_row["label"],
                node_type=gateway_row["node_type"],
                flavor=gateway_row["flavor"],
                link_type=LinkType.GATEWAY_DOMAIN_TRANSITION.value,
            )
        )

        # Continue from gateway up within the parent domain.
        parent_branch = local_lineage(store, gateway_row["node_id"])
        path.extend(parent_branch[1:])
        current_domain_id = domain.parent_domain_id

    return path
