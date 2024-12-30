import typing as t
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping, KeysView

from typing_extensions import Self

from pipewine.bundle import Bundle
from pipewine.item import Item


class Sample(ABC, Mapping[str, Item]):
    @abstractmethod
    def _get_item(self, key: str) -> Item:
        pass

    @abstractmethod
    def _size(self) -> int:
        pass

    @abstractmethod
    def keys(self) -> KeysView[str]:
        pass

    @abstractmethod
    def with_items(self, **items: Item) -> Self:
        pass

    def with_item(self, key: str, item: Item) -> Self:
        return self.with_items(**{key: item})

    def with_values(self, **values: t.Any) -> Self:
        dict_values = {k: self._get_item(k).with_value(v) for k, v in values.items()}
        return self.with_items(**dict_values)

    def with_value(self, key: str, value: t.Any) -> Self:
        return self.with_values(**{key: value})

    def without(self, *keys: str) -> "TypelessSample":
        items = {k: self._get_item(k) for k in self.keys() if k not in keys}
        return TypelessSample(**items)

    def with_only(self, *keys: str) -> "TypelessSample":
        items = {k: self._get_item(k) for k in self.keys() if k in keys}
        return TypelessSample(**items)

    def typeless(self) -> "TypelessSample":
        return TypelessSample(**self)

    def remap(
        self, fromto: t.Mapping[str, str], exclude: bool = False
    ) -> "TypelessSample":
        if exclude:
            items = {k: self._get_item(k) for k in self.keys() if k in fromto}
        else:
            items = {k: self._get_item(k) for k in self.keys()}
        for k_from, k_to in fromto.items():
            if k_from in items:
                items[k_to] = items.pop(k_from)
        return TypelessSample(**items)

    def __getitem__(self, key: str) -> Item:
        return self._get_item(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return self._size()


class TypelessSample(Sample):
    def __init__(self, **items: Item) -> None:
        self._items = items

    def _get_item(self, key: str) -> Item:
        return self._items[key]

    def _size(self) -> int:
        return len(self._items)

    def keys(self) -> KeysView[str]:
        return self._items.keys()

    def with_items(self, **items: Item) -> Self:
        return self.__class__(**{**self._items, **items})


class TypedSample(Bundle[Item], Sample):
    def _get_item(self, key: str) -> Item:
        return getattr(self, key)

    def _size(self) -> int:
        return len(self.as_dict())

    def keys(self) -> KeysView[str]:
        return self.as_dict().keys()

    def with_items(self, **items: Item) -> Self:
        return type(self).from_dict(**{**self.__dict__, **items})
