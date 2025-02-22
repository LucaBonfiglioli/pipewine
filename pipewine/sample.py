"""`Sample` base class and its implementations to represent data samples in Pipewine."""

from abc import ABC, abstractmethod
from collections.abc import Iterator, KeysView, Mapping
from typing import Any, Mapping, Self

from pipewine.bundle import Bundle
from pipewine.item import Item


class Sample(ABC, Mapping[str, Item]):
    """Base class for all Pipewine samples. A sample is a collection of items, each of
    which references a single, serializable unit of data.

    Samples are immutable mappings from item keys (strings) to item instances. They
    provide methods to access the items, keys, and values, as well as to create new
    samples with different keys and values.

    Subclasses must implement the `_get_item`, `_size`, `keys`, and `with_items`
    methods.
    """

    @abstractmethod
    def _get_item(self, key: str) -> Item:
        """Return the item corresponding to the given key.

        Args:
            key (str): Key of the item to return.

        Returns:
            Item: The item object.
        """
        pass

    @abstractmethod
    def _size(self) -> int:
        """Return the number of items in the sample."""
        pass

    @abstractmethod
    def keys(self) -> KeysView[str]:
        """Return a keys view of the items in the sample."""
        pass

    @abstractmethod
    def with_items(self, **items: Item) -> Self:
        """Return a new sample with the given items added or replaced. The new sample
        is guaranteed to be of the same type as the original sample.

        Args:
            **items (Item): Items to add or replace in the sample.

        Returns:
            Self: The new sample with the given items added or replaced.
        """
        pass

    def with_item(self, key: str, item: Item) -> Self:
        """Return a new sample with the given item added or replaced. The new sample is
        guaranteed to be of the same type as the original sample.

        Args:
            key (str): Key of the item to add or replace.
            item (Item): Item to add or replace.

        Returns:
            Self: The new sample with the given item added or replaced.
        """
        return self.with_items(**{key: item})

    def with_values(self, **values: Any) -> Self:
        """Return a new sample with the values of the items with the given keys changed
        to the given values. The new sample is guaranteed to be of the same type as the
        original sample.

        This method differs from `with_items` in that it only changes the values of the
        items, preserving their type and parser.

        Returns:
            Self: The new sample with the modified values.
        """
        dict_values = {k: self._get_item(k).with_value(v) for k, v in values.items()}
        return self.with_items(**dict_values)

    def with_value(self, key: str, value: Any) -> Self:
        """Return a new sample with the value of the item with the given key changed to
        the given value. The new sample is guaranteed to be of the same type as the
        original sample.

        This method differs from `with_item` in that it only changes the value of the
        item, preserving its type and parser.

        Args:
            key (str): Key of the item to change.
            value (Any): New value to set.

        Returns:
            Self: The new sample with the modified value.
        """
        return self.with_values(**{key: value})

    def without(self, *keys: str) -> "TypelessSample":
        """Return a new sample with the items with the given keys removed. The new
        sample will always be an instance of `TypelessSample`, because removing an item
        always implies that the resulting sample cannot be of the same type as the
        original sample.

        Args:
            *keys (str): Keys of the items to remove.

        Returns:
            TypelessSample: The new sample with the items removed.
        """
        items = {k: self._get_item(k) for k in self.keys() if k not in keys}
        return TypelessSample(**items)

    def with_only(self, *keys: str) -> "TypelessSample":
        """Return a new sample with only the items with the given keys. The new sample
        will always be an instance of `TypelessSample`, because keeping only a subset of
        the items always implies that the resulting sample cannot be of the same type as
        the original sample.

        Args:
            *keys (str): Keys of the items to keep.

        Returns:
            TypelessSample: The new sample with only the items with the given keys.
        """
        items = {k: self._get_item(k) for k in self.keys() if k in keys}
        return TypelessSample(**items)

    def remap(
        self, fromto: Mapping[str, str], exclude: bool = False
    ) -> "TypelessSample":
        """Return a new sample with the items remapped according to the given mapping.
        The new sample will always be an instance of `TypelessSample`, because remapping
        the items always implies that the resulting sample cannot be of the same type as
        the original sample.

        Args:
            fromto (Mapping[str, str]): Mapping from old keys to new keys.
            exclude (bool): Whether to exclude items not in the mapping.

        Returns:
            TypelessSample: The new sample with the items remapped.
        """
        if exclude:
            items = {k: self._get_item(k) for k in self.keys() if k in fromto}
        else:
            items = {k: self._get_item(k) for k in self.keys()}
        for k_from, k_to in fromto.items():
            if k_from in items:
                items[k_to] = items.pop(k_from)
        return TypelessSample(**items)

    def typeless(self) -> "TypelessSample":
        """Return a new sample with the same items as the original sample, but as an
        instance of `TypelessSample`. This method is useful when needing to explicitly
        convert a sample to a typeless sample.

        E.g. when needing to add a new item to a typed sample, it is necessary to
        drop the type information and convert it to a typeless sample first, otherwise
        the `with_items` method (that returns a sample with the same type as the
        original) would not allow adding items with keys not present in the typed
        sample definition.
        """
        return TypelessSample(**self)

    def __getitem__(self, key: str) -> Item:
        return self._get_item(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return self._size()


class TypelessSample(Sample):
    """A `TypelessSample` is a `Sample` that does not have a specific type and can
    contain an arbitrary mapping of items with arbitrary keys.

    It is useful when needing to manipulate samples for which the keys and types of the
    items change dynamically and are not known at dev time, or when needing to load
    data quickly without having to define a specific sample type for every step of the
    pipeline.
    """

    def __init__(self, **items: Item) -> None:
        """
        Args:
            **items (Item): Items to include in the sample.
        """
        super().__init__()
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
    """A `TypedSample` is a `Sample` that has a specific type and contains a mapping of
    items with specific keys.

    It is useful when needing to define a specific structure for the data samples in a
    pipeline, with known keys and types for the items. This allows for type checking and
    auto-completion in IDEs, and makes it easier to reason about the data flow in the
    pipeline.

    `TypedSample` is a subclass of `Bundle`, allowing subclasses to define the items as
    plain pyhton dataclasses, with type hints for the item values. This makes it easy to
    define the items and access their values, and provides a convenient way to create
    new samples with different keys and values.

    Every `TypedSample` object can be drop the type information and be converted to a
    `TypelessSample` object using the `typeless` method if needed.

    Be careful when defining `TypedSample` subclasses, as the keys of the items might
    conflict with the methods and attributes of the base class. Avoid using any of the
    following when defining `TypedSample` subclasses:

    - Any key starting with underscore (`_`).
    - `keys`, `values`, `items`, `get` inherited from the `Mapping` protocol.
    - `as_dict` and `from_dict` inherited from the `Bundle` class.
    - `with_item`, `with_items`, `with_value`, `with_values`, `without`, `with_only`,
        `remap`, `typeless` inherited from the `Sample` class.
    """

    def _get_item(self, key: str) -> Item:
        return getattr(self, key)

    def _size(self) -> int:
        return len(self.as_dict())

    def keys(self) -> KeysView[str]:
        return self.as_dict().keys()

    def with_items(self, **items: Item) -> Self:
        return type(self).from_dict(**{**self.__dict__, **items})
