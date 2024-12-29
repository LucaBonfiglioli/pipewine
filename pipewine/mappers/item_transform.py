from collections.abc import Iterable, Mapping

from pipewine.item import Item
from pipewine.mappers.base import Mapper
from pipewine.parsers import Parser
from pipewine.sample import Sample


class ChangeFormatMapper[T: Sample](Mapper[T, T]):
    def __init__(self, parsers: Mapping[str, Parser]) -> None:
        super().__init__()
        self._parsers = parsers

    def __call__(self, idx: int, x: T) -> T:
        to_modify: dict[str, Item] = {}
        for k, parser in self._parsers.items():
            if k in x:
                to_modify[k] = x[k].with_parser(parser)
        return x.with_items(**to_modify)


class ShareMapper[T: Sample](Mapper[T, T]):
    def __init__(self, share: Iterable[str], unshare: Iterable[str]) -> None:
        super().__init__()
        if set(share) & set(unshare):
            raise ValueError("The keys in 'share' and 'unshare' must be disjoint.")
        self._share = share
        self._unshare = unshare

    def __call__(self, idx: int, x: T) -> T:
        to_modify: dict[str, Item] = {}
        for k, item in x.items():
            if not item.is_shared and k in self._share:
                to_modify[k] = item.with_sharedness(True)
            elif item.is_shared and k in self._unshare:
                to_modify[k] = item.with_sharedness(False)
        return x.with_items(**to_modify)
