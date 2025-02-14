from collections.abc import Callable
from pathlib import Path

from pipewine.grabber import Grabber
from pipewine.sample import Sample
from pipewine.sources import DatasetSource, UnderfolderSource


class SourceCLIRegistry:
    registered: dict[str, Callable[[str, Grabber, type[Sample]], DatasetSource]] = {}


def source_cli[
    T: Callable[[str, Grabber, type[Sample]], DatasetSource]
](name: str | None = None) -> Callable[[T], T]:
    def inner(fn: T) -> T:
        fn_name = name or fn.__name__
        SourceCLIRegistry.registered[fn_name] = fn
        return fn

    return inner


@source_cli()
def underfolder(
    text: str, grabber: Grabber, sample_type: type[Sample]
) -> UnderfolderSource:
    """PATH: Path to the dataset folder."""
    return UnderfolderSource(Path(text), sample_type=sample_type)
