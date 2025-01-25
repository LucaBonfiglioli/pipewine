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


class SlowMapper(Mapper[Sample, Sample]):
    def __call__[T: Sample](self, idx: int, x: T) -> T:
        time.sleep(0.001)
        return x


if __name__ == "__main__":
    path = Path("tests/sample_data/underfolders/underfolder_0")
    slow_map = MapOp(SlowMapper())

    maxsize = 5
    n = 2600
    maxind = 26

    caches = {
        "no_cache": (None, "#000000"),
        "memo": (MemoCache, "#f23022"),
        "rr": (RRCache, "#158c37"),
        "fifo": (FIFOCache, "#123787"),
        "lifo": (LIFOCache, "#2594b3"),
        "lru": (LRUCache, "#d66624"),
        "mru": (MRUCache, "#c73677"),
    }

    patterns = {
        "cyclic": (np.arange(n) % maxind).tolist(),
        "random": np.random.randint(0, maxind - 1, [n]).tolist(),
        "zipfian": (np.random.zipf(2, n) % maxind).tolist(),
        "blocks": (np.arange(n) // 16 % maxind).tolist(),
        "random_walk": (np.random.randn(n).cumsum().astype(np.int64) % maxind).tolist(),
    }

    time_series: dict[str, dict[str, np.ndarray]] = {}
    for pattern_name, pattern in patterns.items():
        time_series[pattern_name] = {}
        for cache_name, (cache, _) in caches.items():
            ts = []
            dataset = slow_map(UnderfolderSource(path)())
            if cache is not None:
                if issubclass(cache, MemoCache):
                    dataset = CacheOp(cache)(dataset)
                else:
                    dataset = CacheOp(cache, maxsize=maxsize)(dataset)
            t = time.perf_counter()
            for idx in pattern:
                sample = dataset[idx]
                letter = sample["metadata"]()["letter"]
                t2 = time.perf_counter()
                delta = t2 - t
                ts.append(delta)
                print(pattern_name, cache_name, letter, delta)
            time_series[pattern_name][cache_name] = np.array(ts)

    import matplotlib.pyplot as plt

    for pattern_name, tss in time_series.items():
        for cache_name, ts in tss.items():
            c = caches[cache_name][1]
            x = ((time_series["cyclic"]["no_cache"] / ts) - 1) * 100
            plt.plot(x, label=cache_name, c=c)

        plt.title(pattern_name)
        plt.xlabel("N. Accesses")
        plt.ylabel("Speedup (%)")
        plt.grid()
        plt.legend()
        plt.tight_layout()
        plt.show()
