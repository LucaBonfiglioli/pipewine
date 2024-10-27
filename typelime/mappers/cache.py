from typelime.item import MemoryItem
from typelime.mappers.base import Mapper
from typelime.sample import Sample


class CacheMapper[T: Sample](Mapper[T, T]):
    def apply(self, x: T) -> T:
        new_items = {}
        for k, v in x.items():
            new_items[k] = MemoryItem(v(), v.get_parser())
        return x.with_items(**new_items)
