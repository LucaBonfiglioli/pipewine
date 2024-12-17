from collections import deque
from collections.abc import Callable
from pathlib import Path

from typelime.grabber import Grabber
from typelime.sinks import CopyPolicy, DatasetSink, OverwritePolicy, UnderfolderSink


class SinkCLIRegistry:
    registered: dict[str, Callable[[str, Grabber], DatasetSink]] = {}


def sink_cli[
    T: Callable[[str, Grabber], DatasetSink]
](name: str | None = None) -> Callable[[T], T]:
    def inner(fn: T) -> T:
        fn_name = name or fn.__name__
        SinkCLIRegistry.registered[fn_name] = fn
        return fn

    return inner


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

    return UnderfolderSink(
        Path(path), grabber=grabber, overwrite_policy=ow_policy, copy_policy=copy_policy
    )
