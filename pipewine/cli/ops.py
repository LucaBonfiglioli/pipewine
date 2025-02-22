"""CLI commands for dataset operators."""

import inspect
import random
import sys
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import partial
from types import GenericAlias
from typing import Annotated, Any, Literal, Optional, cast
from uuid import uuid1

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from typer import Context, Option, Typer

from pipewine._op_typing import (
    get_sample_type_from_dataset_annotation,
    get_sample_type_from_sample_annotation,
    origin_type,
)
from pipewine.bundle import Bundle
from pipewine.cli.sinks import SinkCLIRegistry
from pipewine.cli.sources import SourceCLIRegistry
from pipewine.cli.utils import (
    deep_get,
    parse_grabber,
    parse_sink,
    parse_source,
    run_cli_workflow,
)
from pipewine.dataset import Dataset
from pipewine.grabber import Grabber
from pipewine.operators import *
from pipewine.parsers import ParserRegistry
from pipewine.sample import Sample, TypelessSample
from pipewine.sinks import DatasetSink
from pipewine.sources import DatasetSource
from pipewine.workflows import Workflow


class _DictBundle[T](Bundle[T]):
    def __init__(self, /, **kwargs: T) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


@dataclass
class _OpInfo:
    input_format: str
    output_format: str
    grabber: Grabber
    tui: bool


# This gets called by generated code
def _single_op_workflow(
    ctx: Context, operator: DatasetOperator, *args, **kwargs
) -> None:
    opinfo = cast(_OpInfo, ctx.obj)
    i_hint = inspect.get_annotations(operator.__call__, eval_str=True).get("x")
    o_hint = inspect.get_annotations(operator.__call__, eval_str=True).get("return")
    i_orig, o_orig = origin_type(i_hint), origin_type(o_hint)

    i_name, o_name = "input", "output"
    i_name_short, o_name_short = i_name[0], o_name[0]

    input_dict, output_dict = {}, {}
    maybe_in_seq, maybe_out_seq = kwargs.get(i_name), kwargs.get(o_name)
    i_curr, o_curr = 0, 0
    extra_queue = deque(ctx.args)
    for token in sys.argv:
        if isinstance(maybe_in_seq, Sequence) and (
            token.startswith(f"-{i_name_short}.") or token.startswith(f"--{i_name}.")
        ):
            input_dict[maybe_in_seq[i_curr][1:]] = extra_queue.popleft()
            i_curr += 1
        if isinstance(maybe_out_seq, Sequence) and (
            token.startswith(f"-{o_name_short}.") or token.startswith(f"--{o_name}.")
        ):
            output_dict[maybe_out_seq[o_curr][1:]] = extra_queue.popleft()
            o_curr += 1

    def _parse_source(text: str, sample_type: type[Sample]) -> DatasetSource:
        return parse_source(opinfo.input_format, text, opinfo.grabber, sample_type)

    def _parse_sink(text: str) -> DatasetSink:
        return parse_sink(opinfo.output_format, text, opinfo.grabber)

    wf = Workflow()
    if issubclass(i_orig, Dataset):
        if isinstance(operator, MapOp):
            sample_hint = inspect.get_annotations(
                operator._mapper.__call__, eval_str=True
            ).get("x")
            sample_type = get_sample_type_from_sample_annotation(sample_hint)
        else:
            sample_type = get_sample_type_from_dataset_annotation(i_hint)
        input_ = wf.node(_parse_source(kwargs[i_name], sample_type))()
    elif issubclass(i_orig, (Sequence, tuple)):
        if (
            issubclass(i_orig, tuple)
            and isinstance(i_hint, GenericAlias)
            and not (len(i_hint.__args__) == 2 and i_hint.__args__[1] is ...)
        ):
            input_ = [
                wf.node(
                    _parse_source(x, get_sample_type_from_dataset_annotation(hint)),
                )()
                for x, hint in zip(kwargs[i_name], i_hint.__args__)
            ]
        elif isinstance(i_hint, GenericAlias) and len(i_hint.__args__) > 0:
            sample_type = get_sample_type_from_dataset_annotation(i_hint.__args__[0])
            input_ = [wf.node(_parse_source(x, sample_type))() for x in kwargs[i_name]]
        else:
            sample_type = TypelessSample
            input_ = [wf.node(_parse_source(x, sample_type))() for x in kwargs[i_name]]
    elif issubclass(i_orig, Mapping):
        if isinstance(i_hint, GenericAlias) and len(i_hint.__args__) == 2:
            sample_type = get_sample_type_from_dataset_annotation(i_hint.__args__[1])
        else:
            sample_type = TypelessSample
        input_ = {
            k: wf.node(_parse_source(v, sample_type))() for k, v in input_dict.items()
        }
    elif issubclass(i_orig, Bundle):
        data = {
            k: wf.node(
                _parse_source(
                    kwargs[f"{i_name}_{k}"], get_sample_type_from_dataset_annotation(v)
                )
            )()
            for k, v in inspect.get_annotations(i_orig).items()
        }
        input_ = _DictBundle(**data)
    else:
        raise NotImplementedError(i_orig)
    output = wf.node(operator)(input_)  # type: ignore
    if issubclass(o_orig, Dataset):
        wf.node(_parse_sink(kwargs[o_name]))(output)
    elif issubclass(o_orig, (Sequence, tuple)):
        for i, x in enumerate(kwargs[o_name]):
            wf.node(_parse_sink(x))(output[i])
    elif issubclass(o_orig, Mapping):
        for k, v in output_dict.items():
            wf.node(_parse_sink(v))(output[k])
    elif issubclass(o_orig, Bundle):
        for k in o_orig.__annotations__:
            wf.node(_parse_sink(kwargs[f"{o_name}_{k}"]))(getattr(output, k))
    else:
        raise NotImplementedError(o_orig)

    run_cli_workflow(wf, tui=opinfo.tui)


def _annotation_to_str(annotation: Any) -> tuple[str, set[type]]:
    symbols = set()
    if isinstance(annotation, GenericAlias):
        args: list[str] = []
        for arg in annotation.__args__:
            argstr, sub_symbols = _annotation_to_str(arg)
            args.append(argstr)
            symbols.update(sub_symbols)
        annstr = f"{annotation.__origin__.__name__}[{', '.join(args)}]"
    elif isinstance(annotation, type):
        annstr = annotation.__name__
        symbols.add(annotation)
    else:
        raise NotImplementedError(annotation)
    return annstr, symbols


def _param_to_str(
    param: inspect.Parameter, param_obj_code: str
) -> tuple[str, set[type]]:
    annotation = param.annotation
    defaultstr = (
        f" = {param_obj_code}.default" if param.default is not inspect._empty else ""
    )
    symbols: set[type] = set()
    if annotation is inspect._empty:
        annstr = ""
    elif annotation.__name__ == "Annotated":
        annmeta = [
            f"{param_obj_code}.annotation.__metadata__[{i}]"
            for i in range(len(annotation.__metadata__))
        ]
        annsubstr, sub_symbols = _annotation_to_str(annotation.__origin__)
        symbols.update(sub_symbols)
        annstr = f"Annotated[{annsubstr}, {', '.join(annmeta)}]"
    else:
        annstr, sub_symbols = _annotation_to_str(annotation)
        symbols.update(sub_symbols)

    annstr = f": {annstr}" if annstr else ""
    return param.name + annstr + defaultstr, symbols


def _generate_op_command[
    **T, V: DatasetOperator
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    cmd_name = name or fn.__name__.replace("_", "-")
    params = inspect.signature(fn).parameters
    fn_args_code: list[str] = []
    symbols: set[type] = set()
    grabber_param: str | None = None
    param_names: set[str] = set(params.keys())
    for k in params:
        if params[k].annotation is Grabber:
            if grabber_param is not None:
                raise ValueError("Passing grabber argument multiple times.")
            if params[k].default is not inspect._empty:
                raise ValueError("Grabber argument must not have a default value.")
            grabber_param = k
            param_names.remove(k)
            continue

        code, subsymbols = _param_to_str(params[k], f"locals()['params']['{k}']")
        symbols.update(subsymbols)
        fn_args_code.append(code)

    optype: type[DatasetOperator] = origin_type(fn.__annotations__["return"])
    assert issubclass(optype, DatasetOperator)

    i_hint = optype.__call__.__annotations__["x"]
    o_hint = optype.__call__.__annotations__["return"]
    i_orig = origin_type(i_hint)
    o_orig = origin_type(o_hint)

    def make_option(
        io: str, type_: Literal["str", "list", "dict"] | int, name: str = ""
    ) -> str:
        if type_ == "str":
            help = f'The "{name}" {io} dataset.' if name else f"The {io} dataset."
            hint = "str"
        elif type_ == "list":
            help = f"List of {io} datasets. Use multiple times: --{io} DATA1 --{io} DATA2 ..."
            hint = "list[str]"
        elif type_ == "dict":
            help = f"Dict of {io} datasets. Use multiple times: --{io[0]}.key_a DATA1 --{io[0]}.key_b DATA2 ..."
            hint = "list[str]"
        else:
            hint = f"tuple[{', '.join(['str'] * type_)}]"
            help = f"Tuple of {type_} {io} datasets."
        param = io if not name else f"{io}_{name}"
        if type_ == "dict" and not name:
            decl = f"'-{io[0]}'"
        else:
            decl = (
                f"'-{io[0]}.{name}', '--{io}.{name}'"
                if name
                else f"'-{io[0]}', '--{io}'"
            )
        code = f"{param}: Annotated[{hint}, Option(..., {decl}, help='{help}')]"
        return code

    added_args_code: list[str] = []
    for hint, orig, io in [[i_hint, i_orig, "input"], [o_hint, o_orig, "output"]]:
        if issubclass(orig, Dataset):
            added_args_code.append(make_option(io, "str"))
        elif issubclass(orig, tuple):
            if hint is tuple or hint.__args__[-1] is Ellipsis:
                added_args_code.append(make_option(io, "list"))
            else:
                added_args_code.append(make_option(io, len(hint.__args__)))
            pass
        elif issubclass(orig, Sequence):
            added_args_code.append(make_option(io, "list"))
        elif issubclass(orig, Mapping):
            added_args_code.append(make_option(io, "dict"))
        elif issubclass(orig, Bundle):
            for k in orig.__annotations__:
                added_args_code.append(make_option(io, "str", name=k))
        else:
            raise NotImplementedError("Not supported")

    gen_fn_name = f"_generated_fn_{name}_{uuid1().hex}"
    gen_cls_name = f"_generated_cls_{name}_{uuid1().hex}"
    code = f"""
from typing import Annotated
from typer import Context, Option, Argument

class {gen_cls_name}:
    fn = None
    param_names = None
    grabber_param = None

def {gen_fn_name}(
    ctx: Context,
    {",\n    ".join(added_args_code + fn_args_code)}
) -> None:
    params = ctx.params
    op_kwargs = {{k: v for k, v in params.items() if k in {gen_cls_name}.param_names}}
    if {gen_cls_name}.grabber_param is not None:
        op_kwargs[{gen_cls_name}.grabber_param] = ctx.obj.grabber
    operator = {gen_cls_name}.fn(**op_kwargs)
    _single_op_workflow(ctx, operator, **params)
"""
    for symbol in symbols:
        globals()[symbol.__name__] = symbol
    exec(code)
    gen_cls, gen_fn = locals()[gen_cls_name], locals()[gen_fn_name]
    gen_cls.fn = fn
    gen_cls.param_names = param_names
    gen_cls.grabber_param = grabber_param
    globals()[gen_cls_name] = gen_cls
    globals()[gen_fn_name] = gen_fn
    ctx_settings = {"allow_extra_args": True, "ignore_unknown_options": True}
    op_app.command(cmd_name, context_settings=ctx_settings, help=fn.__doc__)(gen_fn)
    return fn


def op_cli[T](name: str | None = None) -> Callable[[T], T]:
    """Decorator to generate a CLI command for a dataset operator.

    Decorated functions must follow the rules of Typer CLI commands, returning a
    `DatasetOperator` object.

    The decorated function must be correctly annotated with the type of operator it
    returns.

    Args:
        name (str, optional): The name of the command. Defaults to None, in which case
            the function name is used.
    """

    return partial(_generate_op_command, name=name)  # type: ignore


def _print_format_help_panel() -> None:
    console = Console()
    titles = ["Input Formats", "Output Formats"]
    grid_expand = False
    grid_padding = (0, 3)
    key_style = "bold cyan"
    panel_border_style = "dim"
    for title, registry in zip(titles, (SourceCLIRegistry, SinkCLIRegistry)):
        grid = Table.grid(expand=grid_expand, padding=grid_padding)
        grid.add_column(style=key_style)
        grid.add_column()
        for name, fn in registry.registered.items():  # type: ignore
            grid.add_row(name, fn.__doc__ or "")
        panel = Panel(
            grid, title=title, title_align="left", border_style=panel_border_style
        )
        console.print(panel)
    grid = Table.grid(expand=grid_expand, padding=grid_padding)
    grid.add_column(style=key_style)
    grid.add_column()
    grid.add_column()
    for k in ParserRegistry.keys():
        ptype = ParserRegistry.get(k)
        assert ptype is not None
        grid.add_row(k, ptype.__module__, ptype.__name__)
    panel = Panel(
        grid,
        title="Data Formats",
        title_align="left",
        border_style=panel_border_style,
    )
    console.print(panel)


input_format_help = "The format of the input dataset/s."
output_format_help = "The format of the output dataset/s."
grabber_help = (
    "Multi-processing options WORKERS[,PREFETCH] (e.g. '-g 4' will spawn 4 workers"
    " with default prefetching, '-g 8,20' will spawn 8 workers with prefetch 20)."
)
tui_help = "Show workflow progress in a TUI while executing the command."
format_help_help = "Show a help message on data input/output formats and exit."


def _op_callback(
    ctx: Context,
    input_format: Annotated[
        str, Option(..., "-I", "--input-format", help=input_format_help)
    ] = "underfolder",
    output_format: Annotated[
        str, Option(..., "-O", "--output-format", help=output_format_help)
    ] = "underfolder",
    format_help: Annotated[bool, Option(help=format_help_help, is_eager=True)] = False,
    grabber: Annotated[
        Optional[Grabber],
        Option(..., "-g", "--grabber", help=grabber_help, parser=parse_grabber),
    ] = None,
    tui: Annotated[bool, Option(..., help=tui_help)] = True,
) -> None:
    if format_help:
        _print_format_help_panel()
        exit()
    ctx.obj = _OpInfo(
        input_format,
        output_format,
        grabber or Grabber(),
        tui,
    )


op_app = Typer(
    callback=_op_callback,
    name="op",
    help="Run a pipewine dataset operator.",
    invoke_without_command=True,
    no_args_is_help=True,
)
"""Typer app for the Pipewine op CLI."""


key_help = "Filter by the value of key (e.g. metadata.mylist.12.foo)."
compare_help = "How to compare with the target."
target_help = "The target value (gets autocasted to the key value)."
negate_help = "Invert the filtering criterion."


@op_cli()
def clone() -> IdentityOp:
    """Copy a dataset, applying no changes to any sample."""
    return IdentityOp()


class Compare(str, Enum):
    eq = "eq"
    neq = "neq"
    gt = "gt"
    lt = "lt"
    ge = "ge"
    le = "le"


@op_cli(name="filter")
def filter_(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
    compare: Annotated[Compare, Option(..., "--compare", "-c", help=compare_help)],
    target: Annotated[str, Option(..., "--target", "-t", help=target_help)],
    negate: Annotated[bool, Option(..., "--negate", "-n", help=negate_help)] = False,
) -> FilterOp:
    """Keep only the samples that satisfy a certain logical comparison with a target."""

    def _filter_fn(idx: int, sample: Sample) -> bool:
        value = deep_get(sample, key)
        target_ = (
            type(value)(target)
            if type(value) != bool
            else str(target).lower() in ["yes", "true", "y", "ok", "t", "1"]
        )
        if compare == Compare.eq:
            result = value == target_
        elif compare == Compare.neq:
            result = value != target_
        elif compare == Compare.gt:
            result = value > target_
        elif compare == Compare.lt:
            result = value < target_
        elif compare == Compare.ge:
            result = value >= target_
        else:
            result = value <= target_
        return result

    return FilterOp(_filter_fn, negate=negate, grabber=grabber)


key_help = "Group by the value of the key (e.g. metadata.mylist.12.foo)."


@op_cli()
def groupby(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
) -> GroupByOp:
    """Group together samples with the same value associated to the specified key."""

    def _groupby_fn(idx: int, sample: Sample) -> str:
        return str(deep_get(sample, key))

    return GroupByOp(_groupby_fn, grabber=grabber)


key_help = "Sorting key (e.g. metadata.mylist.12.foo)."
reverse_help = "Sort instead by non-increasing values."


@op_cli()
def sort(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
    reverse: Annotated[bool, Option(..., "--reverse", "-r", help=reverse_help)] = False,
) -> SortOp:
    """Sort samples by non-decreasing values associated with the specified key."""

    def _sort_fn(idx: int, sample: Sample) -> Any:
        return deep_get(sample, key)

    return SortOp(_sort_fn, reverse=reverse, grabber=grabber)


@op_cli(name="slice")
def slice_(
    start: Annotated[int, Option(help="Start index.")] = None,  # type: ignore
    stop: Annotated[int, Option(help="Stop index.")] = None,  # type: ignore
    step: Annotated[int, Option(help="Slice step size.")] = None,  # type: ignore
) -> SliceOp:
    """Slice a dataset as you would do with any Python sequence."""
    return SliceOp(start=start, stop=stop, step=step)


times_help = "The number of times to repeat the dataset."
interleave_help = "Instead of ABCABCABCABC, do AAAABBBBCCCC."


@op_cli()
def repeat(
    times: Annotated[int, Option(..., "--times", "-t", help=times_help)],
    interleave: Annotated[
        bool, Option(..., "--interleave", "-I", help=interleave_help)
    ] = False,
) -> RepeatOp:
    """Repeat a dataset N times replicating the samples."""
    return RepeatOp(times, interleave=interleave)


@op_cli()
def cycle(
    length: Annotated[int, Option(..., "--n", "-n", help="Desired number of samples.")]
) -> CycleOp:
    """Repeat the samples until a certain number of samples is reached."""
    return CycleOp(length)


@op_cli()
def reverse() -> ReverseOp:
    """Reverse the order of the samples."""
    return ReverseOp()


length_help = "Desired number of samples."
pad_with_help = "Index of the sample (within the dataset) to use as padding."


@op_cli()
def pad(
    length: Annotated[int, Option(..., "--length", "-l", help=length_help)],
    pad_with: Annotated[int, Option(..., "--pad-width", "-p", help=pad_with_help)] = -1,
) -> PadOp:
    """Pad a dataset until it reaches a specified length."""
    return PadOp(length, pad_with=pad_with)


@op_cli()
def cat() -> CatOp:
    """Concatenate two or more datasets into a single dataset."""
    return CatOp()


@op_cli(name="zip")
def zip_() -> ZipOp:
    """Zip two or more datasets of the same length by merging the individual samples."""
    return ZipOp()


@op_cli()
def shuffle(
    seed: Annotated[int, Option(..., "--seed", "-s", help="Random seed.")] = -1
) -> ShuffleOp:
    """Shuffle the samples of a dataset in random order."""
    if seed >= 0:
        random.seed(seed)
    return ShuffleOp()


batch_size_help = "The number of samples per batch."


@op_cli()
def batch(
    batch_size: Annotated[int, Option(..., "--batch-size", "-b", help=batch_size_help)],
) -> BatchOp:
    """Split a dataset into batches of the specified size."""
    return BatchOp(batch_size)


@op_cli()
def chunk(
    chunks: Annotated[int, Option(..., "--chunk", "-c", help="The number of chunks.")]
) -> ChunkOp:
    """Split a dataset into N chunks."""
    return ChunkOp(chunks)


splits_help = (
    "The size of each dataset, either as exact values (int) or fraction"
    " (float). You can set at most one value to 'null' to mean 'all the"
    " remaining samples'."
)


@op_cli()
def split(
    sizes: Annotated[list[str], Option(..., "-s", "--sizes", help=splits_help)]
) -> SplitOp:
    """Split a dataset into parts with custom size."""
    parsed_sizes = []
    for x in sizes:
        if x == "null":
            parsed = None
        elif "." in x:
            parsed = float(x)
        else:
            parsed = int(x)
        parsed_sizes.append(parsed)
    return SplitOp(parsed_sizes)
