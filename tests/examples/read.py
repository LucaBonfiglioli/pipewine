from pathlib import Path

import numpy as np

from typelime.item import Item
from typelime.sample import TypedSample
from typelime.sources.underfolder import UnderfolderSource
from pydantic import BaseModel


class MyMetadata(BaseModel):
    username: str
    email: str


class MySample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]


source = UnderfolderSource(
    Path("tests/sample_data/underfolder_0"), sample_type=MySample
)

dataset = source.generate()


print(dataset[0].metadata().email)
