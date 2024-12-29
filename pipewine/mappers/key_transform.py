from collections.abc import Iterable, Mapping

from pipewine.mappers.base import Mapper
from pipewine.sample import Sample, TypelessSample


class DuplicateItemMapper(Mapper[Sample, TypelessSample]):
    """Duplicate an item."""

    def __init__(self, source_key: str, destination_key: str) -> None:
        super().__init__()
        self._source_key = source_key
        self._destination_key = destination_key

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.typeless().with_item(self._destination_key, x[self._source_key])


class FormatKeysMapper(Mapper[Sample, TypelessSample]):
    """Changes key names following a format string."""

    FMT_CHAR = "*"

    def __init__(
        self, format_string: str = FMT_CHAR, keys: str | Iterable[str] | None = None
    ) -> None:
        """Constructor.

        Args:
            format_string (str, optional): The new sample key format. Any `*` will be
            replaced with the source key, eg, `my_*_key` on [`image`, `mask`] generates
            `my_image_key` and `my_mask_key`. If no `*` is found, the string is suffixed
            to source key, ie, `MyKey` on `image` gives `imageMyKey`. If empty, the
            source key will not be changed. Defaults to "*".

            keys (str | Iterable[str] | None, optional): The keys to apply the new
            format to. `None` applies to all the keys. Defaults to None.
        """
        super().__init__()
        if self.FMT_CHAR not in format_string:
            format_string = self.FMT_CHAR + format_string

        self._format_string = format_string
        self._keys = keys

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        if self._keys is None:
            keys = x.keys()
        elif isinstance(self._keys, str):
            keys = [self._keys]
        else:
            keys = self._keys
        remap = {}
        for k in keys:
            remap[k] = self._format_string.replace(self.FMT_CHAR, k)
        return x.remap(remap)


class RenameMapper(Mapper[Sample, TypelessSample]):
    """Rename some items preserving their content and format."""

    def __init__(self, renaming: Mapping[str, str], exclude: bool = False) -> None:
        super().__init__()
        self._renaming = renaming
        self._exclude = exclude

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.remap(self._renaming, exclude=self._exclude)


class FilterKeysMapper(Mapper[Sample, TypelessSample]):
    """Filters sample keys."""

    def __init__(self, keys: str | Iterable[str], negate: bool = False) -> None:
        super().__init__()
        self._keys = [keys] if isinstance(keys, str) else keys
        self._negate = negate

    def __call__(self, idx: int, x: Sample) -> TypelessSample:
        return x.without(*self._keys) if self._negate else x.with_only(*self._keys)
