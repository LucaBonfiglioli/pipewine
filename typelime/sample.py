import typing as t
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping

from typing_extensions import Self

from typelime.item import Item


class Sample(ABC, Mapping[str, Item]):
    @abstractmethod
    def get_item(self, key: str) -> Item:
        pass

    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def keys(self) -> Iterator[str]:  # type: ignore
        pass

    @abstractmethod
    def with_items(self, **items: Item) -> Self:
        pass

    def with_data(self, **data: t.Any) -> Self:
        dict_data = {k: self.get_item(k).with_data(v) for k, v in data.items()}
        return self.with_items(**dict_data)

    def without(self, *keys: str) -> "Sample":
        data = {k: self.get_item(k) for k in self.keys() if k not in keys}
        return TypelessSample(data)

    def with_only(self, *keys: str) -> "Sample":
        data = {k: self.get_item(k) for k in self.keys() if k in keys}
        return TypelessSample(data)

    def remapped(
        self, remapping: t.Mapping[str, str], exclude: bool = False
    ) -> "Sample":
        if exclude:
            data = {}
        else:
            data = {k: self.get_item(k) for k in self.keys()}
        for k_from, k_to in remapping.items():
            if k_from in data:
                data[k_to] = data.pop(k_from)
        return TypelessSample(data)

    def __getitem__(self, key: str) -> Item:
        return self.get_item(key)

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    def __len__(self) -> int:
        return self.size()


class TypelessSample(Sample):
    def __init__(self, data: Mapping[str, Item]) -> None:
        self._data = data

    def get_item(self, key: str) -> Item:
        return self._data[key]

    def size(self) -> int:
        return len(self._data)

    def keys(self) -> Iterator[str]:
        return iter(self._data)

    def with_items(self, **items: Item) -> Self:
        return self.__class__({**self._data, **items})


class TypedSample(Sample):
    def get_item(self, key: str) -> Item:
        return getattr(self, key)

    def size(self) -> int:
        return len(self.__dict__)

    def keys(self) -> Iterator[str]:
        return iter(self.__dict__)

    def with_items(self, **items: Item) -> Self:
        return self.__class__(**{**self.__dict__, **items})
