import inspect
import sys
import traceback
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from types import GenericAlias
from typing import Annotated, Any, Literal, Optional, cast
from uuid import uuid1

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from typer import Context, Option, Typer

from typelime._op_typing import origin_type
from typelime.bundle import Bundle
from typelime.cli.sinks import SinkCLIRegistry
from typelime.cli.sources import SourceCLIRegistry
from typelime.dataset import Dataset
from typelime.grabber import Grabber
from typelime.operators import DatasetOperator
from typelime.sinks import DatasetSink
from typelime.sources import DatasetSource
from typelime.workflows import CursesTracker, NoTracker, Workflow, run_workflow


class _DictBundle[T](Bundle[T]):
    def __init__(self, /, **kwargs: T) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


def _print_workflow_panel(
    start_time: datetime, text: str, style: str, body: str | None = None
) -> None:
    end_time = datetime.now()
    duration = end_time - start_time
    console = Console()
    message = Text(text, style=style)
    if body is not None:
        message.append(f"\n\n{body}", style="white not bold")
    message.append(f"\nStarted:  ")
    message.append(start_time.strftime("%Y-%m-%d %H:%M:%S"), style="white not bold")
    message.append(f"\nFinished: ")
    message.append(end_time.strftime("%Y-%m-%d %H:%M:%S"), style="white not bold")
    message.append(f"\nTotal duration: ")
    message.append(str(duration), style="white not bold")
    panel = Panel(
        message, title="Workflow Status", title_align="left", expand=False, style=style
    )
    console.print(panel)


def _run_cli_workflow(workflow: Workflow, tui: bool = True) -> None:
    start_time = datetime.now()
    try:
        run_workflow(workflow, tracker=CursesTracker() if tui else NoTracker())
    except KeyboardInterrupt:
        _print_workflow_panel(start_time, "Workflow canceled.", "bold bright_black")
        exit(1)
    except Exception:
        _print_workflow_panel(
            start_time, "Workflow failed.", "bold red", body=traceback.format_exc()
        )
        exit(1)

    _print_workflow_panel(start_time, "Workflow completed successfully.", "bold green")


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


# This gets called by generated code
def _generated_fn_impl(
    ctx: Context, operator: DatasetOperator, *args, **kwargs
) -> None:
    opinfo = cast(OpInfo, ctx.obj)
    i_hint = operator.__call__.__annotations__["x"]
    o_hint = operator.__call__.__annotations__["return"]
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

    def _parse_source_or_sink(
        text: str, reg: type[SourceCLIRegistry] | type[SinkCLIRegistry]
    ) -> DatasetSource | DatasetSink:
        format_ = opinfo.input_format
        if format_ not in reg.registered:
            print(
                f"Format '{format_}' not found, use "
                "'typelime op --format-help' to print available i/o formats."
            )
            exit(1)
        try:
            result = reg.registered[format_](text, opinfo.grabber)
        except:
            print(
                f"Failed to parse string '{text}' into a '{format_}' format, use "
                "'typelime op --format-help' to print available i/o formats and their "
                "usage."
            )
            exit(1)
        return result

    def _parse_source(text: str) -> DatasetSource:
        return cast(DatasetSource, _parse_source_or_sink(text, SourceCLIRegistry))

    def _parse_sink(text: str) -> DatasetSink:
        return cast(DatasetSink, _parse_source_or_sink(text, SinkCLIRegistry))

    wf = Workflow()
    if issubclass(i_orig, Dataset):
        input_ = wf.node(_parse_source(kwargs[i_name]))()
    elif issubclass(i_orig, (list, tuple)):
        input_ = [wf.node(_parse_source(x))() for x in kwargs[i_name]]
    elif issubclass(i_orig, dict):
        input_ = {k: wf.node(_parse_source(v))() for k, v in input_dict.items()}
    elif issubclass(i_orig, Bundle):
        data = {
            k: wf.node(_parse_source(kwargs[f"{i_name}_{k}"]))()
            for k in i_orig.__annotations__
        }
        input_ = _DictBundle(**data)
    else:
        raise NotImplementedError(i_orig)
    output = wf.node(operator)(input_)
    if issubclass(o_orig, Dataset):
        wf.node(_parse_sink(kwargs[o_name]))(output)
    elif issubclass(o_orig, (list, tuple)):
        for i, x in enumerate(kwargs[o_name]):
            wf.node(_parse_sink(x))(output[i])
    elif issubclass(o_orig, dict):
        for k, v in output_dict.items():
            wf.node(_parse_sink(v))(output[k])
    elif issubclass(o_orig, Bundle):
        for k in o_orig.__annotations__:
            wf.node(_parse_sink(kwargs[f"{o_name}_{k}"]))(getattr(output, k))
    else:
        raise NotImplementedError(o_orig)

    _run_cli_workflow(wf)


# This is a hack to inject stuff into a typer command. Do not try this at home!
def _op_cli_inner[
    **T, V: DatasetOperator
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    cmd_name = name or fn.__name__
    params = inspect.signature(fn).parameters
    fn_args_code: list[str] = []
    symbols: set[type] = set()
    grabber_param: str | None = None
    param_names: set[str] = set()
    for k in params:
        defaultstr = (
            f" = locals()['params']['{k}'].default"
            if params[k].default is not inspect._empty
            else ""
        )
        annotation = params[k].annotation
        if annotation is Grabber:
            if grabber_param is not None:
                raise ValueError("Passing grabber argument multiple times.")
            if params[k].default is not inspect._empty:
                raise ValueError("Grabber argument must not have a default value.")
            grabber_param = k
            continue
        elif annotation is inspect._empty:
            annstr = ""
        elif annotation.__name__ == "Annotated":
            annmeta = [
                f"locals()['params']['{k}'].annotation.__metadata__[{i}]"
                for i in range(len(annotation.__metadata__))
            ]
            annsubstr, sub_symbols = _annotation_to_str(annotation.__origin__)
            symbols.update(sub_symbols)
            annstr = f"Annotated[{annsubstr}, {', '.join(annmeta)}]"
        else:
            annstr, sub_symbols = _annotation_to_str(annotation)
            symbols.update(sub_symbols)
        annstr = f": {annstr}" if annstr else ""
        fn_args_code.append(k + annstr + defaultstr)
        param_names.add(k)

    for symbol in symbols:
        globals()[symbol.__name__] = symbol

    optype: type[DatasetOperator] = origin_type(fn.__annotations__["return"])
    assert issubclass(optype, DatasetOperator)

    i_hint = optype.__call__.__annotations__["x"]
    o_hint = optype.__call__.__annotations__["return"]
    i_orig = origin_type(i_hint)
    o_orig = origin_type(o_hint)

    added_args_code: list[str] = []

    def make_option(
        io: str, type_: Literal["str", "list", "dict"] | int, name: str = ""
    ) -> str:
        if type_ == "str":
            help_str = f'The "{name}" {io} dataset.' if name else f"The {io} dataset."
            hint = "str"
        elif type_ == "list":
            help_str = f"List of {io} datasets. Use multiple times: --{io} DATA1 --{io} DATA2 ..."
            hint = "list[str]"
        elif type_ == "dict":
            help_str = f"Dict of {io} datasets. Use multiple times: --{io}.key_a DATA1 --{io}.key_b DATA2 ..."
            hint = "list[str]"
        else:
            hint = f"tuple[{', '.join(['str'] * type_)}]"
            help_str = f"Tuple of {type_} {io} datasets."
        param = io if not name else f"{io}_{name}"
        decl = (
            f"'-{io[0]}.{name}', '--{io}.{name}'" if name else f"'-{io[0]}', '--{io}'"
        )
        return f"{param}: Annotated[{hint}, Option(..., {decl}, help='{help_str}')]"

    for hint, orig, io in [[i_hint, i_orig, "input"], [o_hint, o_orig, "output"]]:
        if issubclass(orig, Dataset):
            added_args_code.append(make_option(io, "str"))
        elif issubclass(orig, tuple):
            if hint is tuple or hint.__args__[-1] is Ellipsis:
                added_args_code.append(make_option(io, "list"))
            else:
                added_args_code.append(make_option(io, len(hint.__args__)))
            pass
        elif issubclass(orig, list):
            added_args_code.append(make_option(io, "list"))
        elif issubclass(orig, dict):
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
    _generated_fn_impl(ctx, operator, **params)
"""
    exec(code)
    gen_cls, gen_fn = locals()[gen_cls_name], locals()[gen_fn_name]
    gen_cls.fn = fn
    gen_cls.param_names = param_names
    gen_cls.grabber_param = grabber_param
    globals()[gen_cls_name] = gen_cls
    globals()[gen_fn_name] = gen_fn
    ctx_settings = {"allow_extra_args": True, "ignore_unknown_options": True}
    helpstr = fn.__doc__
    op_app.command(cmd_name, context_settings=ctx_settings, help=helpstr)(gen_fn)
    return fn


def op_cli[T](name: str | None = None) -> Callable[[T], T]:
    return partial(_op_cli_inner, name=name)  # type: ignore


def parse_grabber(value: str) -> Grabber:
    sep = ","
    if sep in value:
        worker_str, _, prefetch_str = value.partition(sep)
        return Grabber(num_workers=int(worker_str), prefetch=int(prefetch_str))
    else:
        return Grabber(num_workers=int(value))


@dataclass
class OpInfo:
    input_format: str
    output_format: str
    grabber: Grabber
    tui: bool


def _print_format_help_panel() -> None:
    console = Console()
    titles = ["Input Formats", "Output Formats"]
    registries = [SourceCLIRegistry, SinkCLIRegistry]
    for title, registry in zip(titles, registries):
        grid = Table.grid(expand=False, padding=(0, 3))
        grid.add_column(style="bold cyan")
        grid.add_column()
        for name, fn in registry.registered.items():
            grid.add_row(name, fn.__doc__ or "")
        panel = Panel(grid, title=title, title_align="left", border_style="dim")
        console.print(panel)


input_format_help = "The format of the input dataset/s."
output_format_help = "The format of the output dataset/s."
grabber_help = (
    "Multi-processing options WORKERS[,PREFETCH] (e.g. '-g 4' will spawn 4 workers"
    " with default prefetching, '-g 8,20' will spawn 8 workers with prefetch 20)."
)
tui_help = "Show workflow progress in a TUI while executing the command."
format_help_help = "Show a help message on data input/output formats and exit."


def op_callback(
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
    tui: Annotated[bool, Option(..., "-t", "--tui", help=tui_help)] = True,
) -> None:
    if format_help:
        _print_format_help_panel()
        exit()
    ctx.obj = OpInfo(
        input_format,
        output_format,
        grabber or Grabber(),
        tui,
    )


main_app = Typer(
    invoke_without_command=False,
    pretty_exceptions_enable=False,
    add_completion=False,
    no_args_is_help=True,
)
op_app = Typer()
main_app.add_typer(
    op_app,
    name="op",
    help="Run a typelime dataset operation.",
    callback=op_callback,
    invoke_without_command=True,
    no_args_is_help=True,
)


def main() -> None:
    main_app()


if __name__ == "__main__":
    main()
