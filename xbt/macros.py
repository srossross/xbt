import os
import sys
import yaml

from werkzeug.utils import import_string
from jinja2 import Environment


def is_macro(tname: str) -> bool:
    exts = [".jinja2", ".j2", ".sql", ".py"]
    ext = os.path.splitext(tname)[1]
    if ext not in exts:
        return False
    if tname.startswith("macros/"):
        return True
    return False


def load_macros_from_dir(env: Environment):
    macros = {}
    sys.path.insert(0, "./flow/macros")
    for tname in env.list_templates(filter_func=is_macro):
        template = env.get_template(tname)
        assert template.filename is not None
        if template.filename.endswith(".py"):
            name = os.path.basename(template.filename)[:-3]
            mod = __import__(name, fromlist=["*"])
            all_names = getattr(mod, "__all__", None) or dir(mod)
            public_names = [name for name in all_names if not name.startswith("_")]
            macros[name] = {attr: getattr(mod, attr) for attr in public_names}

        module = template.make_module()
        for name, value in module.__dict__.items():
            if callable(value) and not name.startswith("_"):
                macros[name] = value
    sys.path.pop(0)
    return macros


def load_macros(env: Environment, dag, config):
    def source(name):
        s = config["sources"][name]
        return env.from_string(s).render()

    def env_var(name, *default):
        if not default and name not in os.environ:
            raise ValueError(f"Environment variable {name} is not set")
        return os.environ.get(name, *default)

    def ref(other):
        dag.add_edge(other, env.globals["current_node"])
        return dag.nodes[other].get("ref_name", other)

    def resource(ref_name):
        current_name = env.globals["current_node"]
        dag.add_node(current_name, ref_name=ref_name)
        return ref_name

    def create_table():
        current_name = env.globals["current_node"]
        ref_name = f'"main"."{current_name}"'
        dag.add_node(current_name, ref_name=ref_name)
        return f"""
    DROP TABLE IF EXISTS {ref_name};
    CREATE TABLE {ref_name} AS
    """

    def xprint(*args):
        print(args)
        return ""

    return dict(
        source=source,
        env_var=env_var,
        ref=ref,
        create_table=create_table,
        resource=resource,
        print=xprint,
    )


def load_macros_from_config(config):
    macros = {}
    sys.path.append(".")
    for item in config.get("macros", []):
        name = item.split(":", 1)[-1]
        macro = import_string(item)
        macros[name] = macro
    sys.path.pop()

    return macros


def load_vars(env: Environment, config, var):
    env.globals.update(config.get("data", {}))
    cli_vars = {}
    for v in var:
        if "=" not in v:
            raise ValueError(f"Invalid var {v} (should be key=value)")
        k, v = v.split("=", 1)
        cli_vars[k] = yaml.safe_load(v)

    env.globals.update(cli_vars)
