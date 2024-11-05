from typelime.item import CachedItem
from typelime.mappers.base import Mapper
from typelime.sample import Sample


class CacheMapper[T: Sample](Mapper[T, T]):
    def apply(self, idx: int, x: T) -> T:
        return x.with_items(
            **{
                k: v if isinstance(v, CachedItem) else CachedItem(v)
                for k, v in x.items()
            }
        )
