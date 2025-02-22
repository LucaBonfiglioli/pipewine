"""CLI for running workflows."""

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
class _WfInfo:
    tui: bool
    draw: Optional[Path]


_global_wf_info: list[_WfInfo] = []

tui_help = "Show workflow progress in a TUI while executing the command."
draw_help = "Draw workflow to a SVG file and exit."


def _wf_callback(
    ctx: Context,
    tui: Annotated[bool, Option(..., help=tui_help)] = True,
    draw: Annotated[Optional[Path], Option(..., help=draw_help)] = None,
) -> None:
    _global_wf_info.append(_WfInfo(tui, draw))


def _generate_wf_command[
    **T, V: Workflow
](fn: Callable[T, V], name: str | None = None) -> Callable[T, V]:
    @functools.wraps(fn)
    def decorated(*args, **kwargs):
        wf = fn(*args, **kwargs)
        wf_info = _global_wf_info[-1]
        if wf_info.draw is not None:
            draw_workflow(wf, wf_info.draw)
            return
        run_cli_workflow(wf, tui=wf_info.tui)

    wf_app.command(name=name)(decorated)

    return fn


wf_app = Typer(
    callback=_wf_callback,
    name="wf",
    help="Run a pipewine workflow.",
    invoke_without_command=True,
    no_args_is_help=True,
)
"""The main CLI application for workflows."""


def wf_cli[T](name: str | None = None) -> Callable[[T], T]:
    """Decorator to generate a CLI command for a dataset workflow.

    Decorated functions must follwo the rules of Typer CLI commands, returning a
    `Workflow` object to be run.

    Args:
        name (str, optional): The name of the command. Defaults to None, in which case
            the function name is used.
    """
    return functools.partial(_generate_wf_command, name=name)  # type: ignore
