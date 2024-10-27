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


folder = Path("tests/sample_data/underfolder_0")

dataset = UnderfolderSource[MySample](folder).generate()

print(dataset[0].metadata().username)
print(dataset[0].metadata().email)
print(dataset[1].metadata().username)
print(dataset[1].metadata().email)
print(dataset[2].metadata().username)
print(dataset[2].metadata().email)
