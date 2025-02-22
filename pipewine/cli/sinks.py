"""CLI for dataset sinks."""

from collections import deque
from collections.abc import Callable
from pathlib import Path

from pipewine.grabber import Grabber
from pipewine.sinks import CopyPolicy, DatasetSink, OverwritePolicy, UnderfolderSink


class SinkCLIRegistry:
    """Registry for known types of dataset sinks."""

    registered: dict[str, Callable[[str, Grabber], DatasetSink]] = {}


def sink_cli[
    T: Callable[[str, Grabber], DatasetSink]
](name: str | None = None) -> Callable[[T], T]:
    """Decorator to register a type of dataset sink to the CLI.

    The decorated function must take a string and a grabber and return a dataset sink
    that can be called with a single dataset.

    The decorated function must be correctly annotated with the type of dataset sink it
    returns.

    Args:
        name (str, optional): The name of the sink. Defaults to None, in which case
            the function name is used.
    """

    def inner(fn: T) -> T:
        fn_name = name or fn.__name__
        SinkCLIRegistry.registered[fn_name] = fn
        return fn

    return inner


def _split_and_parse_underfolder_text(
    text: str,
) -> tuple[Path, OverwritePolicy, CopyPolicy]:
    splits = deque(text.split(","))
    path = splits.popleft()
    if len(splits) > 0:
        ow_policy = OverwritePolicy(splits.popleft().upper())
    else:
        ow_policy = OverwritePolicy.FORBID
    if len(splits) > 0:
        copy_policy = CopyPolicy(splits.popleft().upper())
    else:
        copy_policy = CopyPolicy.HARD_LINK
    return Path(path), ow_policy, copy_policy


@sink_cli()
def underfolder(text: str, grabber: Grabber) -> UnderfolderSink:
    """PATH[,OVERWRITE=forbid[,COPY_POLICY=hard_link]]

    PATH: Path to the dataset to write.
    OVERWRITE: What happens if the destination path is not empty. One of:
        - "forbid" - Fail if the folder already exists.
        - "allow_if_empty" - Allow overwrite if the folder exists but it is empty.
        - "allow_new_files" - Only allow the creation of new files.
        - "overwrite_files" - If a file already exists, delete it before writing.
        - "overwrite" - If the folder already exists, delete if before writing.
    COPY_POLICY: What happens if the library detects replication of existing data. One of:
        - "rewrite" - Do as if no copy was detected. Serialize the data and write.
        - "replicate" - Avoid the serialization but copy the original file contents.
        - "symbolic_link" - Create a symlink to the original file.
        - "hard_link" - Create a link to the same inode of the original file.
    """
    path, ow_policy, copy_policy = _split_and_parse_underfolder_text(text)
    return UnderfolderSink(
        Path(path), grabber=grabber, overwrite_policy=ow_policy, copy_policy=copy_policy
    )
