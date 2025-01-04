from typer import Typer, Option
from typing import Annotated
from pipewine.cli.ops import op_app
from pipewine.cli.mappers import map_app


def main_callback(
    version: Annotated[
        bool, Option(help="Show pipewine version", is_eager=True)
    ] = False,
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
    callback=main_callback,
)
pipewine_app.add_typer(op_app)
pipewine_app.add_typer(map_app)


def main() -> None:  # pragma: no cover
    pipewine_app()


if __name__ == "__main__":  # pragma: no cover
    main()
