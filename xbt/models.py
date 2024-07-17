import os
from jinja2 import Environment
import networkx as nx


EXTS = [".jinja2", ".j2", ".sql"]


def is_ty(ty: str):
    def is_script(tname: str) -> bool:
        ext = os.path.splitext(tname)[1]
        if ext not in EXTS:
            return False
        if tname.startswith(f"{ty}/"):
            return True
        return False

    return is_script


def load_all(env: Environment, dag: nx.DiGraph, config, ty: str, field: str):
    scripts = {}

    first_connection = config["connections"][0]["name"]

    for tname in env.list_templates(filter_func=is_ty(field)):
        name = os.path.basename(tname)[:-4]
        scripts[name] = {
            "name": name,
            "type": ty,
            "template": tname,
            "data": {},
            "output": f"{name}.sql",
            "connection": first_connection,
        }

    for name, overrides in config.get(field, {}).items():
        data = scripts.setdefault(name, {})
        data.update(overrides)
        data.setdefault("data", {})
        data.setdefault("type", ty)
        data.setdefault("output", f"{name}.sql")
        data.setdefault("connection", first_connection)
        scripts[name] = data

    for name, data in scripts.items():
        dag.add_node(name, conn=data["connection"])

    return scripts


def load_all_scripts(env: Environment, dag, config):
    return load_all(env, dag, config, "script", "scripts")


def load_all_models(env: Environment, dag, config):
    return load_all(env, dag, config, "model", "models")
