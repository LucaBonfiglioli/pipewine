from pathlib import Path

import numpy as np
from pydantic import BaseModel

from typelime.item import Item
from typelime.mappers import Mapper
from typelime.operators import CacheOp, MapOp, CatOp, MemoCache
from typelime.sample import TypedSample
from typelime.sources import UnderfolderSource
from typelime.sinks import UnderfolderSink, OverwritePolicy
from typelime.grabber import Grabber


class MyMetadata(BaseModel):
    username: str
    email: str


class MySample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]
    shared: Item


class VeryLongMapper(Mapper[MySample, MySample]):
    def __call__(self, idx: int, x: MySample) -> MySample:
        print(f"LONG CALL {idx}")
        return x


def myfunc(x):
    return x


if __name__ == "__main__":
    folder = Path("tests/sample_data/underfolder_0")

    dataset = UnderfolderSource(folder, sample_type=MySample)()
    dataset = MapOp(VeryLongMapper())(dataset)
    dataset = CacheOp(MemoCache)(dataset)
    dataset[1]
    print(dataset[0].metadata().email)
    print(dataset[0].metadata().email)
    print(dataset[0].metadata().email)
    print(dataset[0].shared())

    dataset = CatOp()([dataset, dataset[:2], dataset[:1]])

    UnderfolderSink(
        Path("/tmp/cursed"),
        grabber=Grabber(8, 1),
        overwrite_policy=OverwritePolicy.OVERWRITE_FOLDER,
    )(dataset)
