"""Pipewine root package, containing all the core classes and functions of the library.

Everything except the `pipewine.workflows` and `pipewine.cli` modules is imported here, 
so that the user can conveniently access the most important classes and functions
directly from the `pipewine` package. 

The Pipewine API reference documentation is available as docstrings in every public
module, class, function and attribute. This form of documentation assumes is intended to
be used with an interactive Python environment, such as IPython or Jupyter, or through
the static documentation website.

The API Reference assumes that the developer is familiar with the basic concepts of
the Pipewine library, available in the "Usage" section of the documentation.
"""

__version__ = "0.1.0"
"""Pipewine package version."""

from pipewine.bundle import Bundle, BundleMeta
from pipewine.dataset import Dataset, LazyDataset, ListDataset
from pipewine.grabber import Grabber
from pipewine.item import CachedItem, Item, MemoryItem, StoredItem
from pipewine.mappers.base import Mapper
from pipewine.mappers.cache import CacheMapper
from pipewine.mappers.compose import ComposeMapper
from pipewine.mappers.crypto import HashedSample, HashMapper
from pipewine.mappers.item_transform import ConvertMapper, ShareMapper
from pipewine.mappers.key_transform import (
    DuplicateItemMapper,
    FilterKeysMapper,
    FormatKeysMapper,
    RenameMapper,
)
from pipewine.operators.base import DatasetOperator, IdentityOp
from pipewine.operators.cache import (
    Cache,
    CacheOp,
    FIFOCache,
    ItemCacheOp,
    LIFOCache,
    LRUCache,
    MemoCache,
    MemorizeEverythingOp,
    MRUCache,
    RRCache,
)
from pipewine.operators.functional import FilterOp, GroupByOp, MapOp, SortOp
from pipewine.operators.iter import (
    CycleOp,
    IndexOp,
    PadOp,
    RepeatOp,
    ReverseOp,
    SliceOp,
)
from pipewine.operators.merge import CatOp, ZipOp
from pipewine.operators.rand import ShuffleOp
from pipewine.operators.split import BatchOp, ChunkOp, SplitOp
from pipewine.parsers.base import Parser, ParserRegistry
from pipewine.parsers.image_parser import (
    BmpParser,
    ImageParser,
    JpegParser,
    PngParser,
    TiffParser,
)
from pipewine.parsers.metadata_parser import JSONParser, YAMLParser
from pipewine.parsers.numpy_parser import NumpyNpyParser
from pipewine.parsers.pickle_parser import PickleParser
from pipewine.reader import LocalFileReader, Reader
from pipewine.sample import Sample, TypedSample, TypelessSample
from pipewine.sinks.base import DatasetSink
from pipewine.sinks.fs_utils import CopyPolicy, write_item_to_file
from pipewine.sinks.underfolder import CopyPolicy, OverwritePolicy, UnderfolderSink
from pipewine.sources.base import DatasetSource
from pipewine.sources.underfolder import UnderfolderSource
