"""Utilities for file system operations"""

import os
import shutil
from collections.abc import Callable
from enum import Enum
from pathlib import Path

from pipewine.item import CachedItem, Item, StoredItem
from pipewine.reader import LocalFileReader


class CopyPolicy(str, Enum):
    """How to handle cases in which a replication of existing data is detected. In these
    cases, some operations may be avoided, trading off data consistency for speed.
    """

    REWRITE = "REWRITE"
    """Do not copy anything, ever, even if the data is untouched. Treat every write
    alike: serialize the object, encode it and write to a new file. This is the slowest
    option but also the safest."""

    REPLICATE = "REPLICATE"
    """Avoid the serialization (which can be expensive), and simply copy the existing 
    file, replicating all of its content. This option is significantly faster than 
    re-serializing everything every time, but may cause data corruption in case of
    read-write-read races. E.g. pipewine reads the file, some other process modifies it,
    then pipewine copies the modified file assuming it was not modified. Pipewine
    datasets should not be modified inplace, and pipewine never does so unless 
    explicitly configured to do so, making this option relatively safe to use."""

    SYMBOLIC_LINK = "SYMBOLIC_LINK"
    """Symbolic links (a.k.a. soft links) are simply a reference to another file in any
    of the mounted file systems. They are virtually inexpensive, but do not actually
    contain any replicated data. They are prone to cause data loss, because every
    time the original file is modified/deleted/renamed/moved, the symbolic link will 
    point to a modified/deleted file. A symbolic link can point to a file on another 
    file system. This option is extremely unsafe, use at your own risk."""

    HARD_LINK = "HARD_LINK"
    """Hard links are similar to symbolic links in terms of speed and safety, the
    difference is that they point to the same inode of the linked file.

    Soft link:  `MY_LINK` --> `MY_FILE` --> `INODE`

    Hard link:  `MY_FILE` --> `INODE` <-- `MY_LINK`

    While this does not offer any protection against modifications of the original file 
    (because the data is not actually replicated), it prevents links from breaking in
    case the original file is moved/renamed or even deleted. Hard link are also faster
    than symbolic links, making this option preferable. The only limitation is that
    hard links can only be created when both link and linked files exist on the same
    filesystem.
    """


def _try_copy(
    copy_fn: Callable[[str, str], None], src: str, dst: str, errors: list
) -> bool:
    try:
        copy_fn(src, dst)
        return True
    except Exception as e:
        errors.append((copy_fn, src, dst, e))
    return False


def write_item_to_file(
    item: Item, file: Path, copy_policy: CopyPolicy = CopyPolicy.HARD_LINK
) -> None:
    """Write an item to a file, using the specified copy policy.

    Args:
        item (Item): The item to write to the file.
        file (Path): The path to the file to write to.
        copy_policy (CopyPolicy, optional): The copy policy to use when Pipewine
            infers that the item is a copy of an existing file. Defaults to
            CopyPolicy.HARD_LINK.

    Raises:
        IOError: If the item cannot be written to the file.
    """
    if isinstance(item, CachedItem):
        item = item.source_recursive

    errors: list[tuple] = []
    if (
        isinstance(item, StoredItem)
        and isinstance(item.reader, LocalFileReader)
        and item.reader.path.is_file()
    ):
        src = item.reader.path
        if copy_policy == CopyPolicy.HARD_LINK:
            if _try_copy(os.link, str(src), str(file), errors):
                return
            else:
                copy_policy = CopyPolicy.REPLICATE

        if copy_policy == CopyPolicy.SYMBOLIC_LINK:
            if _try_copy(os.symlink, str(src), str(file), errors):
                return
            else:
                copy_policy = CopyPolicy.REPLICATE

        if copy_policy == CopyPolicy.REPLICATE:
            if _try_copy(shutil.copy, str(src), str(file), errors):
                return
            else:
                copy_policy = CopyPolicy.REWRITE

    data = item.parser.dump(item())
    try:
        with open(file, "wb") as fp:
            fp.write(data)
    except Exception:
        msg = f"Failed to write to file {file}. Failed attempts: \n"
        substr = []
        for err in errors:
            modname, fname = err[0].__module__, err[0].__name__
            substr.append(
                f" - Calling {modname}.{fname}({err[1]}, {err[2]}) -> {err[3]}"
            )
        msg += "\n".join(substr)
        raise IOError(msg)
