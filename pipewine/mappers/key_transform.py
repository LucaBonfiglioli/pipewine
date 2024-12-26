from collections.abc import Iterable, Mapping

from pipewine.mappers.base import Mapper
from pipewine.sample import Sample, TypelessSample


class DuplicateItemMapper(Mapper[Sample, TypelessSample]):
    """Duplicate an item."""

    def __init__(self, source_key: str, dest_key: str) -> None:
        super().__init__()
        self._source_key = source_key
        self._desk_key = dest_key

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.typeless().with_item(self._desk_key, x[self._source_key])


class KeyFormatMapper(Mapper[Sample, TypelessSample]):
    """Changes key names following a format string."""

    FMT_CHAR = "*"

    def __init__(
        self, key_format: str = FMT_CHAR, apply_to: str | Iterable[str] | None = None
    ) -> None:
        """Constructor.

        Args:
            key_format (str, optional): The new sample key format. Any `*` will be
            replaced with the source key, eg, `my_*_key` on [`image`, `mask`] generates
            `my_image_key` and `my_mask_key`. If no `*` is found, the string is suffixed
            to source key, ie, `MyKey` on `image` gives `imageMyKey`. If empty, the
            source key will not be changed.. Defaults to "*".

            apply_to (str | Iterable[str] | None, optional): The keys to apply the new
            format to. `None` applies to all the keys.. Defaults to None.
        """
        super().__init__()
        if self.FMT_CHAR not in key_format:
            key_format = self.FMT_CHAR + key_format

        self._key_format = key_format
        self._apply_to = apply_to

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        if self._apply_to is None:
            apply_to = x.keys()
        elif isinstance(self._apply_to, str):
            apply_to = [self._apply_to]
        else:
            apply_to = self._apply_to
        remap = {}
        for k in apply_to:
            remap[k] = self._key_format.replace(self.FMT_CHAR, k)
        return x.remap(remap)


class RenameMapper(Mapper[Sample, TypelessSample]):
    """Remaps keys in sample preserving internal values."""

    def __init__(self, remap: Mapping[str, str], exclude: bool = False) -> None:
        super().__init__()
        self._remap = remap
        self._exclude = exclude

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.remap(self._remap, exclude=self._exclude)


class FilterKeysMapper(Mapper[Sample, TypelessSample]):
    """Filters sample keys."""

    def __init__(self, keys: str | Iterable[str], negate: bool = False) -> None:
        super().__init__()
        self._keys = [keys] if isinstance(keys, str) else keys
        self._negate = negate

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.without(*self._keys) if self._negate else x.with_only(*self._keys)
