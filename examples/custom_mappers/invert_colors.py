from pathlib import Path
from tempfile import gettempdir

import numpy as np
import pydantic

from pipewine import (
    Item,
    MapOp,
    Mapper,
    TypedSample,
    UnderfolderSink,
    UnderfolderSource,
)


class LetterMetadata(pydantic.BaseModel):
    letter: str
    color: str


class SharedMetadata(pydantic.BaseModel):
    vowels: list[str]
    consonants: list[str]


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    shared: Item[SharedMetadata]


class InvertRGBMapper(Mapper[LetterSample, LetterSample]):
    def __call__(self, idx: int, x: LetterSample) -> LetterSample:
        x = x.with_value("image", 255 - x.image())
        return x


if __name__ == "__main__":
    # Read a dataset with an underfolder source
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(input_path, sample_type=LetterSample)
    dataset = source()

    # Apply the invert rgb mapper
    op = MapOp(InvertRGBMapper())
    inv_dataset = op(dataset)

    # Write the dataset with an underfolder sink
    output_path = Path(gettempdir()) / "invert_colors_example"
    sink = UnderfolderSink(output_path)
    sink(inv_dataset)
