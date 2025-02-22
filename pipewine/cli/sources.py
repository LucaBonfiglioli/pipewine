"""CLI for dataset sources."""

from collections.abc import Callable
from pathlib import Path

from pipewine.grabber import Grabber
from pipewine.sample import Sample
from pipewine.sources import DatasetSource, UnderfolderSource


class SourceCLIRegistry:
    """Registry for known types of dataset sources."""

    registered: dict[str, Callable[[str, Grabber, type[Sample]], DatasetSource]] = {}


def source_cli[
    T: Callable[[str, Grabber, type[Sample]], DatasetSource]
](name: str | None = None) -> Callable[[T], T]:
    """Decorator to register a type of dataset source to the CLI.

    The decorated function must take a string, a grabber, and a sample type and return a
    dataset source that can be called with a single dataset.

    The decorated function must be correctly annotated with the type of dataset source it
    returns.

    Args:
        name (str, optional): The name of the source. Defaults to None, in which case
            the function name is used.
    """

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
