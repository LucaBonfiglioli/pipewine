from collections import defaultdict
from pathlib import Path
import time

import numpy as np
from pipewine import (
    Mapper,
    Dataset,
    Sample,
    UnderfolderSource,
    MapOp,
    CacheOp,
    FIFOCache,
    LIFOCache,
    RRCache,
    LRUCache,
    MemoCache,
    MRUCache,
)

try:
    import matplotlib.pyplot as plt
except:
    print(
        "To run this example script you need matplotlib installed in your environment "
        "but it was not found.\n"
        "Enter the following command to install it:\n\n"
        "   pip install matplotlib\n"
    )
    exit(1)


class SlowMapper(Mapper[Sample, Sample]):
    def __init__(self) -> None:
        # Stateful mapper (never do this!)
        self.miss = 0

    def __call__[T: Sample](self, idx: int, x: T) -> T:
        self.miss += 1
        return x


if __name__ == "__main__":
    path = Path("tests/sample_data/underfolders/underfolder_0")

    maxsize = 5
    n = 26000
    maxind = 26

    patterns = {
        "Cyclic": (np.arange(n) % maxind).tolist(),
        "Back and Forth": np.abs(np.arange(n) % (2 * maxind - 1) - maxind + 1).tolist(),
        "Hot Element": ((np.arange(n) // 2 % maxind) * (np.arange(n) % 2)).tolist(),
        "Blocks": (np.arange(n) // 4 % maxind).tolist(),
        "Uniform": np.random.randint(0, maxind - 1, [n]).tolist(),
        "Zipfian": (np.random.zipf(2, n) % maxind).tolist(),
        "Random Walk": (
            (np.random.randn(n) + 0.5).cumsum().astype(np.int64) % maxind
        ).tolist(),
    }

    caches = {
        "RR": (RRCache, "#15ac37"),
        "FIFO": (FIFOCache, "#1237a7"),
        "LIFO": (LIFOCache, "#2599c3"),
        "LRU": (LRUCache, "#e68624"),
        "MRU": (MRUCache, "#d73677"),
        # "Memo": (MemoCache, "#f23022"), # Unfair comparison!
    }

    results: dict[str, list[float]] = defaultdict(list)
    for pattern_name, pattern in patterns.items():
        for cache_name, (cache, _) in caches.items():
            mapper = SlowMapper()
            slow_map = MapOp(mapper)
            dataset = slow_map(UnderfolderSource(path)())
            if issubclass(cache, MemoCache):
                dataset = CacheOp(cache)(dataset)
            else:
                dataset = CacheOp(cache, maxsize=maxsize)(dataset)
            for idx in pattern:
                sample = dataset[idx]
                letter = sample["metadata"]()["letter"]
            hit_rate = (n - mapper.miss) / n * 100
            results[cache_name].append(hit_rate)
            print(pattern_name, cache_name, hit_rate)

    x = np.arange(len(patterns))
    width = 1 / (len(caches) + 1)
    multiplier = 0

    fig, ax = plt.subplots(layout="constrained")
    for cache_name, hit_rates in results.items():
        offset = width * multiplier
        color = caches[cache_name][1]
        rects = ax.bar(x + offset, hit_rates, width, label=cache_name, color=color)
        ax.bar_label(rects, padding=3, fmt=lambda x: str(round(x, 1)))
        multiplier += 1

    ax.set_title("Cache Benchmark")
    ax.set_ylabel("Hit Rate (%)")
    ax.set_xticks(x + (len(caches) - 1) * width / 2, list(patterns.keys()))
    ax.grid()
    ax.legend()
    ax.set_ylim(0, 100)
    plt.show()
