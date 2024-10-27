from pathlib import Path

import numpy as np
from pydantic import BaseModel

from typelime.item import Item, MemoryItem
from typelime.mappers import ComposeMapper, Mapper
from typelime.parsers import YAMLParser
from typelime.sample import TypedSample
from typelime.sources import UnderfolderSource


class MyMetadata(BaseModel):
    username: str
    email: str


class MySample1(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]


class MySample2(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[MyMetadata]
    abc: Item[int]


class MySample3(TypedSample):
    image: Item[np.ndarray]
    abc: Item[int]


class MySample4(TypedSample):
    abc: Item[int]


class MySample5(TypedSample):
    abc: Item[int]
    hello: Item[str]


class Mapper1To2(Mapper[MySample1, MySample2]):
    def apply(self, idx: int, x: MySample1) -> MySample2:
        abc = MemoryItem(len(x.metadata().email), YAMLParser())
        return MySample2(image=x.image, metadata=x.metadata, abc=abc)


class Mapper2To3(Mapper[MySample2, MySample3]):
    def apply(self, idx: int, x: MySample2) -> MySample3:
        return MySample3(image=x.image, abc=x.abc)


class Mapper3To4(Mapper[MySample3, MySample4]):
    def apply(self, idx: int, x: MySample3) -> MySample4:
        return MySample4(abc=x.abc)


class Mapper4To5(Mapper[MySample4, MySample5]):
    def apply(self, idx: int, x: MySample4) -> MySample5:
        hello = MemoryItem(str(x.abc()), YAMLParser())
        return MySample5(abc=x.abc, hello=hello)


composite_mapper = ComposeMapper(
    (
        Mapper1To2(),
        Mapper2To3(),
        Mapper3To4(),
        Mapper4To5(),
    )
)

my_sample = UnderfolderSource[MySample1](
    Path("tests/sample_data/underfolder_0")
).generate()[0]

out = composite_mapper(0, my_sample)
print(out.abc(), out.hello())
