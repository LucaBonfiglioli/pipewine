"""Main module for Pipewine CLI, containing the main entry point and the main CLI app."""

import sys
from typing import Annotated

from typer import Option, Typer

from pipewine.cli.extension import import_module
from pipewine.cli.mappers import map_app
from pipewine.cli.ops import op_app
from pipewine.cli.workflows import wf_app

version_help = "Show the current Pipewine installation version."
module_help = (
    "Add an extension module with custom CLI commands. Can be either a path to a "
    "python script or a python classpath."
)


def _main_callback(
    version: Annotated[bool, Option(help=version_help, is_eager=True)] = False,
    module: Annotated[list[str], Option(..., "-m", "--module", help=module_help)] = [],
) -> None:
    from pipewine import __version__

    if version:
        print(__version__)
        exit(0)


pipewine_app = Typer(
    invoke_without_command=True,
    pretty_exceptions_enable=False,
    add_completion=False,
    no_args_is_help=True,
    callback=_main_callback,
)
"""Typer app for the main Pipewine CLI."""
pipewine_app.add_typer(op_app)
pipewine_app.add_typer(map_app)
pipewine_app.add_typer(wf_app)


def main() -> None:  # pragma: no cover
    """Pipewine CLI entry point."""
    command_names = [x.name for x in pipewine_app.registered_commands]
    for i, token in enumerate(sys.argv):
        if token in command_names:
            break
        if token in ["-m" or "--module"]:
            import_module(sys.argv[i + 1])
    pipewine_app()


if __name__ == "__main__":  # pragma: no cover
    main()
