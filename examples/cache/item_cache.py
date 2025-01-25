from pathlib import Path
import time
from pipewine import UnderfolderSource, ItemCacheOp

if __name__ == "__main__":
    path = Path("tests/sample_data/underfolders/underfolder_0")

    # Every time an item is accessed it will read data (from disk in this case)
    dataset = UnderfolderSource(path)()
    sample = dataset[0]
    t = time.perf_counter()
    for _ in range(100):
        sample["image"]()
    print("Time without item cache:", time.perf_counter() - t)

    # With cached items, consecutive accesses to the same item will use cached data
    # from the previous read.
    # This should be orders of magnitude faster than the previous loop.
    dataset = ItemCacheOp()(dataset)
    sample = dataset[0]
    t = time.perf_counter()
    for _ in range(100):
        sample["image"]()
    print("Time with item cache:", time.perf_counter() - t)

    # Cached items are effective on consecutive calls on the same Sample object.
    # If we re-index the dataset every time (thus creating new Sample objects),
    # using cached items won't make any difference.
    dataset = UnderfolderSource(path)()
    t = time.perf_counter()
    for _ in range(100):
        dataset[0]["image"]()
    print("Time without item cache (reindexing):", time.perf_counter() - t)

    # This should take approximately the same time as the previous loop.
    dataset = ItemCacheOp()(dataset)
    t = time.perf_counter()
    for _ in range(100):
        dataset[0]["image"]()
    print("Time with item cache (reindexing):", time.perf_counter() - t)
