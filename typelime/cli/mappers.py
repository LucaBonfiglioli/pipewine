import inspect
from collections.abc import Callable
from functools import partial
from typing import Annotated, Optional
from uuid import uuid1

from typer import Option, Typer

from typelime.cli.ops import (
    op_callback,
    _single_op_workflow,
    _param_to_str,
)
from typelime.mappers import *
from typelime.operators.functional import MapOp


def _generate_command[
    **T, V: Mapper
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    cmd_name = name or fn.__name__
    params = inspect.signature(fn).parameters
    fn_args_code: list[str] = []
    symbols: set[type] = set()
    for k in params:
        code, subsymbols = _param_to_str(params[k], f"locals()['params']['{k}']")
        symbols.update(subsymbols)
        fn_args_code.append(code)

    gen_fn_name = f"_generated_fn_{name}_{uuid1().hex}"
    gen_cls_name = f"_generated_cls_{name}_{uuid1().hex}"
    code = f"""
from typing import Annotated
from typer import Context, Option, Argument

class {gen_cls_name}:
    fn = None
    param_names = None

def {gen_fn_name}(
    ctx: Context,
    input: Annotated[str, Option(..., "-i", "--input", help="The input dataset.")],
    output: Annotated[str, Option(..., "-o", "--output", help="The output dataset.")],
    {",\n    ".join(fn_args_code)}
) -> None:
    params = ctx.params
    mapper_kw = {{k: v for k, v in params.items() if k in {gen_cls_name}.param_names}}
    op = {gen_cls_name}.fn(**mapper_kw)
    _single_op_workflow(ctx, op, **params)
"""
    for symbol in symbols:
        globals()[symbol.__name__] = symbol
    exec(code)
    gen_cls, gen_fn = locals()[gen_cls_name], locals()[gen_fn_name]

    def to_map_op(*args: T.args, **kwargs: T.kwargs) -> MapOp:
        return MapOp(fn(*args, **kwargs))

    gen_cls.fn = to_map_op
    gen_cls.param_names = set(params.keys())
    globals()[gen_cls_name] = gen_cls
    globals()[gen_fn_name] = gen_fn
    ctx_settings = {"allow_extra_args": True, "ignore_unknown_options": True}
    helpstr = fn.__doc__
    map_app.command(cmd_name, context_settings=ctx_settings, help=helpstr)(gen_fn)
    return fn


def map_cli[T](name: str | None = None) -> Callable[[T], T]:
    return partial(_generate_command, name=name)  # type: ignore


map_app = Typer(
    callback=op_callback,
    name="map",
    help="Run a typelime dataset mapper.",
    invoke_without_command=True,
    no_args_is_help=True,
)


algo_help = "Hashing algorithm, see 'hashlib' documentation for a full list."
keys_help = "List of keys on which to compute the hash, None for all keys."


@map_cli(name="hash")
def hash_(
    algorithm: Annotated[
        str, Option(..., "-a", "--algorithm", help=algo_help)
    ] = "sha256",
    keys: Annotated[list[str], Option(..., "-k", "--keys", help=keys_help)] = [],
) -> HashMapper:
    """Compute an hash of each sample."""
    return HashMapper(algorithm=algorithm, keys=None if len(keys) == 0 else keys)
