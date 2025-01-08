from pathlib import Path

import numpy as np
import pydantic

from pipewine import Item, TypedSample, UnderfolderSource


class LetterMetadata(pydantic.BaseModel):
    letter: str
    color: str


class SharedMetadata(pydantic.BaseModel):
    vowels: list[str]
    consonants: list[str]


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[LetterMetadata]
    shared: Item[SharedMetadata]


if __name__ == "__main__":
    # Create the source object from an existing directory Path.
    path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(path, sample_type=LetterSample)

    # Call the source object to create a new Dataset instance.
    dataset = source()

    # Do stuff with the dataset
    sample = dataset[4]
    print(sample.image().reshape(-1, 3).mean(0))  # >>> [244.4, 231.4, 221.7]
    print(sample.metadata().color)  #               >>> "orange"
    print(sample.shared().vowels)  #                >>> ["a", "e", "i", "o", "u"]
