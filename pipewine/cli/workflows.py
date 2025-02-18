from collections.abc import Callable
from dataclasses import dataclass
import functools
from pathlib import Path
from typing import Annotated, Optional

from typer import Context, Option, Typer

from pipewine.cli.utils import (
    deep_get,
    parse_grabber,
    parse_sink,
    parse_source,
    run_cli_workflow,
)
from pipewine.workflows import Workflow, draw_workflow


@dataclass
class WfInfo:
    tui: bool
    draw: Optional[Path]


global_wf_info: list[WfInfo] = []

tui_help = "Show workflow progress in a TUI while executing the command."
draw_help = "Draw workflow to a SVG file and exit."


def wf_callback(
    ctx: Context,
    tui: Annotated[bool, Option(..., help=tui_help)] = True,
    draw: Annotated[Optional[Path], Option(..., help=draw_help)] = None,
) -> None:
    global_wf_info.append(WfInfo(tui, draw))


def _generate_wf_command[
    **T, V: Workflow
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    @functools.wraps(fn)
    def decorated(*args, **kwargs):
        wf = fn(*args, **kwargs)
        wf_info = global_wf_info[-1]
        if wf_info.draw is not None:
            draw_workflow(wf, wf_info.draw)
            return
        run_cli_workflow(wf, tui=wf_info.tui)

    wf_app.command(name=name)(decorated)

    return fn


wf_app = Typer(
    callback=wf_callback,
    name="wf",
    help="Run a pipewine workflow.",
    invoke_without_command=True,
    no_args_is_help=True,
)


def wf_cli[T](name: str | None = None) -> Callable[[T], T]:
    return functools.partial(_generate_wf_command, name=name)  # type: ignore
