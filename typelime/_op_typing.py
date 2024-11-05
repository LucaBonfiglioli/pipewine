from collections.abc import Mapping, Sequence

from typelime.bundle import Bundle
from typelime.dataset import Dataset


AnyDataset = (
    Dataset
    | tuple[Dataset, ...]
    | Sequence[Dataset]
    | Mapping[str, Dataset]
    | Bundle[Dataset]
)


class _RetrieveGeneric:
    @property
    def _genargs(self) -> tuple[type, ...]:
        if not hasattr(self, "__orig_class__"):
            return tuple()
        return self.__orig_class__.__args__  # type: ignore
