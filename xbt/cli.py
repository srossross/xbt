import os
import click
import yaml
from jinja2 import Environment, FileSystemLoader
import networkx as nx
from .models import load_all_models, load_all_scripts
from .macros import (
    load_macros,
    load_macros_from_dir,
    load_macros_from_config,
    load_vars,
)
from .connections import build_connection
from .graph import get_selected
from .build import save_model, save_graph
from dotenv import load_dotenv

env = Environment(loader=FileSystemLoader(["flow"]))


@click.group(chain=True, invoke_without_command=True)
@click.pass_context
def main(ctx):
    load_dotenv(".env")
    if ctx.invoked_subcommand is None:
        ctx.invoke(build)
        ctx.invoke(run)


@main.command("build")
@click.option(
    "-v",
    "--var",
    multiple=True,
)
def build(var):
    with open("build.yml") as fd:
        build_info = yaml.safe_load(fd)

    load_vars(env, build_info, var)
    dag = nx.DiGraph()

    macros = load_macros(env, dag, build_info)
    env.globals.update(macros)
    macros = load_macros_from_dir(env)
    env.globals.update(macros)
    macros = load_macros_from_config(build_info)
    env.globals.update(macros)

    os.makedirs("build", exist_ok=True)

    models = {}
    models.update(load_all_models(env, dag, build_info))
    models.update(load_all_scripts(env, dag, build_info))

    for name, model in sorted(models.items()):
        save_model(env, name, model)

    connections_str = yaml.safe_dump(build_info["connections"])
    connections_t = env.from_string(connections_str)
    connections = yaml.safe_load(connections_t.render())

    save_graph(dag, connections)


@main.command("run")
@click.option(
    "-s",
    "--select",
    multiple=True,
    default=(),
)
@click.option("--dry-run/--no-dry-run", "-d/-D", default=False)
def run(select, dry_run):

    with open("build/graph.yml") as fd:
        graph = yaml.safe_load(fd)

    connections = {}
    for info in graph["connections"]:
        connections[info["name"]] = build_connection(info)

    dag = nx.DiGraph()
    for node, conn in graph["nodes"].items():
        dag.add_node(node, conn=conn)
    for a, b in graph["edges"]:
        dag.add_edge(a, b)

    selected = get_selected(dag, select)

    for node in nx.topological_sort(dag):
        if node not in selected:
            print(f"Skip {node} (not selected)")
            continue
        conn_name = dag.nodes[node]["conn"]
        conn = connections[conn_name]
        print(f"Execute {node} using connection {conn_name}")
        with open(f"build/{node}.sql") as fd:
            query = fd.read()
            if not dry_run:
                conn.execute(query)


if __name__ == "__main__":
    main()
