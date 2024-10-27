from pathlib import Path

import numpy as np
from pydantic import BaseModel

from typelime.item import Item
from typelime.mappers import Mapper
from typelime.operators import CacheOp, MapOp
from typelime.sample import TypedSample
from typelime.sources import UnderfolderSource


class MyMetadata(BaseModel):
    username: str
    email: str


class MySample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]


class VeryLongMapper(Mapper[MySample, MySample]):
    def apply(self, x: MySample) -> MySample:
        import time

        print("LONG CALL")
        time.sleep(2)
        return x


folder = Path("tests/sample_data/underfolder_0")

dataset = UnderfolderSource[MySample](folder).generate()

new_dataset = MapOp(VeryLongMapper())(dataset)
new_dataset = CacheOp[MySample]()(new_dataset)
print(new_dataset[0].metadata().email)
print(new_dataset[0].metadata().email)
print(new_dataset[0].metadata().email)
