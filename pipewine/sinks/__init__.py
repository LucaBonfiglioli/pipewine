"""Package for all Pipewine built-in dataset sinks."""

from pipewine.sinks.base import DatasetSink
from pipewine.sinks.underfolder import UnderfolderSink, OverwritePolicy, CopyPolicy
from pipewine.sinks.fs_utils import CopyPolicy, write_item_to_file
