from collections.abc import Callable
from pathlib import Path

from typelime.grabber import Grabber
from typelime.sources import DatasetSource, UnderfolderSource


class SourceCLIRegistry:
    registered: dict[str, Callable[[str, Grabber], DatasetSource]] = {}


def source_cli[
    T: Callable[[str, Grabber], DatasetSource]
](name: str | None = None) -> Callable[[T], T]:
    def inner(fn: T) -> T:
        fn_name = name or fn.__name__
        SourceCLIRegistry.registered[fn_name] = fn
        return fn

    return inner


@source_cli()
def underfolder(text: str, grabber: Grabber) -> UnderfolderSource:
    """PATH: Path to the dataset folder."""
    return UnderfolderSource(Path(text))
