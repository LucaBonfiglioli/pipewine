import os
import shutil
from collections.abc import Iterable
from contextlib import nullcontext
from pathlib import Path

import pytest

from pipewine import (
    CachedItem,
    CopyPolicy,
    Item,
    LocalFileReadStorage,
    MemoryItem,
    Parser,
    StoredItem,
    write_item_to_file,
)
from pipewine.parsers.base import Parser


def fail_on_purpose(*args, **kwargs) -> None:
    raise RuntimeError("Fail!")


class StringParser(Parser[str]):
    def parse(self, data: bytes) -> str:
        return data.decode()

    def dump(self, data: str) -> bytes:
        return data.encode()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".txt"]


@pytest.mark.parametrize(
    [
        "item",
        "policy",
        "link_fails",
        "copy_fails",
        "write_fails",
        "actual_policy",
    ],
    [
        [
            MemoryItem("helloworld", StringParser()),
            CopyPolicy.HARD_LINK,
            False,
            False,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            CachedItem(CachedItem(MemoryItem("helloworld", StringParser()))),
            CopyPolicy.HARD_LINK,
            False,
            False,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.HARD_LINK,
            False,
            False,
            False,
            CopyPolicy.HARD_LINK,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.HARD_LINK,
            True,
            False,
            False,
            CopyPolicy.REPLICATE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.HARD_LINK,
            True,
            True,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.HARD_LINK,
            True,
            True,
            True,
            None,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.SYMBOLIC_LINK,
            False,
            False,
            False,
            CopyPolicy.SYMBOLIC_LINK,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.SYMBOLIC_LINK,
            True,
            False,
            False,
            CopyPolicy.REPLICATE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.SYMBOLIC_LINK,
            True,
            True,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.SYMBOLIC_LINK,
            True,
            True,
            True,
            None,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.REPLICATE,
            False,
            False,
            False,
            CopyPolicy.REPLICATE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.REPLICATE,
            False,
            True,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.REPLICATE,
            False,
            True,
            True,
            None,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.REWRITE,
            False,
            False,
            False,
            CopyPolicy.REWRITE,
        ],
        [
            StoredItem(LocalFileReadStorage(Path()), StringParser()),
            CopyPolicy.REWRITE,
            False,
            True,
            True,
            None,
        ],
    ],
)
def test_write_item_to_file(
    monkeypatch,
    tmp_path,
    sample_data,
    item: Item,
    policy: CopyPolicy,
    link_fails: bool,
    copy_fails: bool,
    write_fails: bool,
    actual_policy: CopyPolicy | None,
) -> None:
    dst = tmp_path / "file"
    if isinstance(item, StoredItem):
        item._storage = LocalFileReadStorage(sample_data / "items" / "data.txt")
    if link_fails:
        monkeypatch.setattr(os, "link", fail_on_purpose)
        monkeypatch.setattr(os, "symlink", fail_on_purpose)
    if copy_fails:
        monkeypatch.setattr(shutil, "copy", fail_on_purpose)
    if write_fails:
        dst = tmp_path / "nonexisting" / "file"

    cm = pytest.raises(IOError) if actual_policy is None else nullcontext()

    with cm:
        write_item_to_file(item, dst, policy)

    if actual_policy is None:
        return

    assert dst.exists()
    re_item = StoredItem(LocalFileReadStorage(dst), item.parser)
    assert re_item() == item()
    if actual_policy == CopyPolicy.HARD_LINK:
        source_item = item
        if isinstance(source_item, CachedItem):
            source_item = source_item.source_recursive()
        assert isinstance(source_item, StoredItem)
        assert isinstance(source_item.storage, LocalFileReadStorage)
        a_string = "A very peculiar string that only I can think of"
        with open(source_item.storage.path, "w") as fp:
            fp.write(a_string)
        with open(dst, "r") as fp:
            assert fp.read() == a_string
    elif actual_policy == CopyPolicy.SYMBOLIC_LINK:
        assert dst.is_symlink()
