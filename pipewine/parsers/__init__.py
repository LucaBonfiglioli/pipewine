"""Package for all Pipewine built-in parsers."""

from pipewine.parsers.base import Parser, ParserRegistry
from pipewine.parsers.metadata_parser import JSONParser, YAMLParser
from pipewine.parsers.numpy_parser import NumpyNpyParser
from pipewine.parsers.pickle_parser import PickleParser
from pipewine.parsers.image_parser import (
    ImageParser,
    BmpParser,
    JpegParser,
    PngParser,
    TiffParser,
)
