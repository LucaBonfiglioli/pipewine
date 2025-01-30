from pathlib import Path
import time

import numpy as np
from pipewine import *


class ComputeStatsMapper(Mapper[Sample, Sample]):
    def __call__(self, idx: int, x: Sample) -> Sample:
        image: np.ndarray = x["image"]()
        mean = image.mean((0, 1)).tolist()
        std = image.std((0, 1)).tolist()
        min_, max_ = int(image.min()), int(image.max())
        stats_item = MemoryItem(
            {"mean": mean, "std": std, "min": min_, "max": max_}, YAMLParser()
        )
        return x.with_item("stats", stats_item)


if __name__ == "__main__":
    path = Path("tests/sample_data/underfolders/underfolder_0")

    dataset = UnderfolderSource(path)()
    dataset = MapOp(ComputeStatsMapper())(dataset)
    dataset = CacheOp(MemoCache)(dataset)  # <- Comment this to run without cache!
    dataset = RepeatOp(100)(dataset)

    t = time.perf_counter()
    for sample in dataset:
        print(sample["stats"]())
    print(time.perf_counter() - t)
