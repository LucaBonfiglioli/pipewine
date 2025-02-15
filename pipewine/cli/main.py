from typing import Annotated

from typer import Option, Typer

from pipewine.cli.mappers import map_app
from pipewine.cli.ops import op_app
from pipewine.cli.workflows import wf_app
from pipewine.cli.extension import import_module

version_help = "Show the current Pipewine installation version."
module_help = (
    "Add an extension module: path to python script, module path, or plain python code."
)


def main_callback(
    version: Annotated[bool, Option(help=version_help, is_eager=True)] = False,
    module: Annotated[list[str], Option(..., "-m", "--module", help=module_help)] = [],
) -> None:
    from pipewine import __version__

    if version:
        print(__version__)
        exit(0)

    for m in module:
        import_module(m)


pipewine_app = Typer(
    invoke_without_command=True,
    pretty_exceptions_enable=False,
    add_completion=False,
    no_args_is_help=True,
    callback=main_callback,
)
pipewine_app.add_typer(op_app)
pipewine_app.add_typer(map_app)
pipewine_app.add_typer(wf_app)


def main() -> None:  # pragma: no cover
    pipewine_app()


if __name__ == "__main__":  # pragma: no cover
    main()
