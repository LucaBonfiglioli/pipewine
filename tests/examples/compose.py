from pathlib import Path

import numpy as np
from pydantic import BaseModel

from typelime.item import Item, MemoryItem
from typelime.parsers import YAMLParser
from typelime.mappers import Mapper, ComposeMapper
from typelime.operators import CacheOp, MapOp
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


class Mapper12(Mapper[MySample1, MySample2]):
    def apply(self, x: MySample1) -> MySample2:
        abc = MemoryItem(len(x.metadata().email), YAMLParser())
        return MySample2(image=x.image, metadata=x.metadata, abc=abc)


class Mapper23(Mapper[MySample2, MySample3]):
    def apply(self, x: MySample2) -> MySample3:
        return MySample3(image=x.image, abc=x.abc)


class Mapper34(Mapper[MySample3, MySample4]):
    def apply(self, x: MySample3) -> MySample4:
        return MySample4(abc=x.abc)


class Mapper45(Mapper[MySample4, MySample5]):
    def apply(self, x: MySample4) -> MySample5:
        hello = MemoryItem(str(x.abc()), YAMLParser())
        return MySample5(abc=x.abc, hello=hello)


mapper_one = ComposeMapper(Mapper12())
mapper_two = ComposeMapper((Mapper12(), Mapper23()))
mapper_three = ComposeMapper((Mapper12(), Mapper23(), Mapper34(), Mapper45()))

my_sample = UnderfolderSource[MySample1](
    Path("tests/sample_data/underfolder_0")
).generate()[0]

out = mapper_three(my_sample)
print(out.abc(), out.hello())
