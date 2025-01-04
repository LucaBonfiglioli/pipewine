from typer import Typer

from pipewine.cli.ops import op_app
from pipewine.cli.mappers import map_app

pipewine_app = Typer(
    invoke_without_command=False,
    pretty_exceptions_enable=False,
    add_completion=False,
    no_args_is_help=True,
)
pipewine_app.add_typer(op_app)
pipewine_app.add_typer(map_app)


def main() -> None:  # pragma: no cover
    pipewine_app()


if __name__ == "__main__":  # pragma: no cover
    main()
