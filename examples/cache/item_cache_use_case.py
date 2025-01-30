from pathlib import Path
import time
from pipewine import UnderfolderSource, ItemCacheOp, Sample


def function1(sample: Sample) -> None:
    print("Average Color: ", sample["image"]().mean((0, 1)))


def function2(sample: Sample) -> None:
    print("Color Standard Deviation: ", sample["image"]().std((0, 1)))


if __name__ == "__main__":
    path = Path("tests/sample_data/underfolders/underfolder_0")

    dataset = UnderfolderSource(path)()
    dataset = ItemCacheOp()(dataset)

    t = time.perf_counter()
    for sample in dataset:
        function1(sample)
        function2(sample)

    print("Total Time", time.perf_counter() - t)
