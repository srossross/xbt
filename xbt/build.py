import yaml


def save_model(env, name, model):
    template = env.get_template(model["template"])
    ty = model["type"]
    output = model["output"]
    fn = f"build/{output}"
    print(f"render {ty} {fn}")
    env.globals.update(current_node=name)
    rendered = template.render(model["data"])
    with open(fn, "w") as fd:
        fd.write(rendered)


def save_graph(dag, connections):
    with open("build/graph.yml", "w") as fd:
        yaml.safe_dump(
            {
                "edges": list(dag.edges()),
                "nodes": {n: dag.nodes[n]["conn"] for n in dag.nodes()},
                "connections": connections,
            },
            default_flow_style=False,
            stream=fd,
        )
