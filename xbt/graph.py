import networkx as nx


def get_selected(dag, selection=None):
    if not selection:
        return set(dag.nodes())
    selected = set()
    and_descendants = False
    and_ancestors = False
    for s in selection:
        if s.endswith("+"):
            s = s[:-1]
            and_descendants = True
        if s.startswith("+"):
            s = s[1:]
            and_ancestors = True

        selected.add(s)
        if and_descendants:
            selected |= set(nx.descendants(dag, s))
        if and_ancestors:
            selected |= set(nx.ancestors(dag, s))

    return selected
