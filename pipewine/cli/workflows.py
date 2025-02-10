from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Optional

from typer import Context, Option, Typer

from pipewine.cli.utils import (
    deep_get,
    parse_grabber,
    parse_sink,
    parse_source,
    run_cli_workflow,
)
from pipewine.cli.ops import _print_format_help_panel
from pipewine.grabber import Grabber


@dataclass
class WfInfo:
    input_format: str
    output_format: str
    grabber: Grabber
    tui: bool
    draw: bool


input_format_help = "The format of the input dataset/s."
output_format_help = "The format of the output dataset/s."
format_help_help = "Show a help message on data input/output formats and exit."
grabber_help = (
    "Multi-processing options WORKERS[,PREFETCH] (e.g. '-g 4' will spawn 4 workers"
    " with default prefetching, '-g 8,20' will spawn 8 workers with prefetch 20)."
)
tui_help = "Show workflow progress in a TUI while executing the command."
draw_help = "Draw workflow to a SVG file and exit."


def wf_callback(
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
    draw: Annotated[bool, Option(..., help=draw_help)] = False,
) -> None:
    if format_help:
        _print_format_help_panel()
        exit()
    ctx.obj = WfInfo(
        input_format,
        output_format,
        grabber or Grabber(),
        tui,
        draw,
    )


wf_app = Typer(
    callback=wf_callback,
    name="wf",
    help="Run a pipewine workflow.",
    invoke_without_command=True,
    no_args_is_help=True,
)


def wf_cli[T](name: str | None = None) -> Callable[[T], T]:
    return partial(_generate_wf_command, name=name)  # type: ignore
