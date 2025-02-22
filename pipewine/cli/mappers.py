"""CLI commands for dataset mappers."""

import inspect
from collections.abc import Callable
from functools import partial
from typing import Annotated, Optional
from uuid import uuid1

from typer import Option, Typer

from pipewine.cli.ops import _param_to_str, _single_op_workflow, _op_callback
from pipewine.mappers import *
from pipewine.operators import MapOp
from pipewine.parsers import Parser, ParserRegistry


def _generate_command[
    **T, V: Mapper
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    cmd_name = name or fn.__name__.replace("_", "-")
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
from pipewine.operators import MapOp

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
    # op = {gen_cls_name}.fn(**mapper_kw)
    op = MapOp({gen_cls_name}.fn(**mapper_kw))
    _single_op_workflow(ctx, op, **params)
"""
    for symbol in symbols:
        globals()[symbol.__name__] = symbol
    exec(code)
    gen_cls, gen_fn = locals()[gen_cls_name], locals()[gen_fn_name]
    gen_cls.fn = fn
    gen_cls.param_names = set(params.keys())
    globals()[gen_cls_name] = gen_cls
    globals()[gen_fn_name] = gen_fn
    ctx_settings = {"allow_extra_args": True, "ignore_unknown_options": True}
    map_app.command(cmd_name, context_settings=ctx_settings, help=fn.__doc__)(gen_fn)
    return fn


def map_cli[T](name: str | None = None) -> Callable[[T], T]:
    """Decorator to generate a CLI command for a dataset mapper.

    Decorated functions must follow the rules of Typer CLI commands, returning a
    `Mapper` object.

    The decorated function must be correctly annotated with the type of mapper it
    returns.

    Args:
        name (str, optional): The name of the command. Defaults to None, in which case
            the function name is used.
    """
    return partial(_generate_command, name=name)  # type: ignore


map_app = Typer(
    callback=_op_callback,
    name="map",
    help="Run a pipewine dataset mapper.",
    invoke_without_command=True,
    no_args_is_help=True,
)
"""Typer app for the Pipewine map CLI."""


algo_help = "Hashing algorithm, see 'hashlib' documentation for a full list."
keys_help = "List of keys on which to compute the hash, None for all keys."


@map_cli(name="hash")
def hash_(
    algorithm: Annotated[
        str, Option(..., "-a", "--algorithm", help=algo_help)
    ] = "sha256",
    keys: Annotated[list[str], Option(..., "-k", "--keys", help=keys_help)] = [],
) -> HashMapper:
    """Apply a hashing function to each sample."""
    return HashMapper(algorithm=algorithm, keys=None if len(keys) == 0 else keys)


conversion_help = (
    "Mapping from each key to data format in the form KEY=FORMAT e.g. '-c image=jpeg'"
)


@map_cli()
def convert(
    conversion: Annotated[
        list[str], Option(..., "-c", "--conversion", help=conversion_help)
    ],
) -> ConvertMapper:
    """Change the format of individual items."""
    parsers: dict[str, Parser] = {}
    for k_eq_fmt in conversion:
        k, _, fmt = k_eq_fmt.partition("=")
        ptype = ParserRegistry.get(fmt)
        assert ptype is not None
        parsers[k] = ptype()
    return ConvertMapper(parsers)


share_help = "Set listed items as shared."
unshare_help = "Set listed items as not shared."


@map_cli()
def share(
    share: Annotated[list[str], Option(..., "-s", "--share", help=share_help)] = [],
    unshare: Annotated[
        list[str], Option(..., "-u", "--unshare", help=unshare_help)
    ] = [],
) -> ShareMapper:
    """Change the sharedness of individual items."""
    return ShareMapper(share, unshare)


source_key_help = "The key of the item to copy."
destination_key_help = "The key that will contain the copied item."


@map_cli()
def duplicate(
    source_key: Annotated[
        str, Option(..., "-s", "--src", "--source-key", help=source_key_help)
    ],
    destination_key: Annotated[
        str, Option(..., "-d", "--dst", "--destination-key", help=destination_key_help)
    ],
) -> DuplicateItemMapper:
    """Create a copy of an item with a different name."""
    return DuplicateItemMapper(source_key, destination_key)


keys_help = "The list of keys to rename applying the format string."
format_string_help = (
    "A string optionally containing the '*' character where any occurrence of '*' is "
    "replaced by the original key. E.g. Applying 'my_*_key' to a set of keys ['image', "
    "'mask'], results in ['my_image_key', 'my_mask_'key']."
)


@map_cli()
def format_keys(
    keys: Annotated[list[str], Option(..., "-k", "--keys", help=keys_help)],
    format_string: Annotated[
        str, Option(..., "-f", "--format-string", help=format_string_help)
    ] = "*",
) -> FormatKeysMapper:
    """Rename items according to a custom format rule."""
    return FormatKeysMapper(keys=keys, format_string=format_string)


renaming_help = "Mapping from old to new keys in the form OLD=NEW. E.g. '-r img=image'."
exclude_help = (
    "If true, remove from the dataset all items not included in the renaming mapping."
)


@map_cli()
def rename(
    renaming: Annotated[list[str], Option(..., "-r", "--renaming", help=renaming_help)],
    exclude: Annotated[bool, Option(..., "-e", "--exclude", help=exclude_help)] = False,
) -> RenameMapper:
    """Rename items with a custom mapping from old to new names."""
    renaming_map: dict[str, str] = {}
    for old_eq_new in renaming:
        old, _, new = old_eq_new.partition("=")
        renaming_map[old] = new
    return RenameMapper(renaming_map, exclude=exclude)


keys_help = "The set of keys to keep/remove. Missing keys will be ignored."
negate_help = "If true, remove the listed keys instead."


@map_cli()
def filter_keys(
    keys: Annotated[list[str], Option(..., "-k", "--keys", help=keys_help)],
    negate: Annotated[bool, Option(..., "-n", "--negate", help=negate_help)] = False,
) -> FilterKeysMapper:
    """Keep only or remove a subset of items."""
    return FilterKeysMapper(keys, negate=negate)
