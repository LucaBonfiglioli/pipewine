from functools import partial
from pathlib import Path
from tempfile import gettempdir

import numpy as np

from pipewine import (
    Dataset,
    DatasetOperator,
    Grabber,
    LazyDataset,
    Sample,
    UnderfolderSink,
    UnderfolderSource,
)


class NormalizeOp(DatasetOperator[Dataset, Dataset]):
    def __init__(self, grabber: Grabber | None = None) -> None:
        super().__init__()
        self._grabber = grabber or Grabber()

    def _normalize[
        T: Sample
    ](self, dataset: Dataset[T], mu: np.ndarray, sigma: np.ndarray, i: int) -> T:
        # Get the image of the i-th sample
        sample = dataset[i]
        image = sample["image"]().astype(np.float32)

        # Apply normalization then bring values back to the [0, 255] range, clipping
        # values below -sigma to 0 and above sigma to 255.
        image = (image - mu) / sigma
        image = (image * 255 / 2 + 255 / 2).clip(0, 255).astype(np.uint8)

        # Replace the image item value
        return sample.with_value("image", image)

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        # Warning: reading and stacking all images in a dataset is not a good strategy
        # memory-wise. In real-world scenarios, you want to use online algorithms to
        # compute mean and standard deviation!
        all_images = []
        for _, sample in self.loop(x, self._grabber, name="Computing stats"):
            all_images.append(sample["image"]())
        all_images_np = np.concatenate(all_images)
        mu = all_images_np.mean((0, 1), keepdims=True)
        sigma = all_images_np.std((0, 1), keepdims=True)

        # Return a lazy dataset that applies the normalization with precomputed coeffs.
        return LazyDataset(len(x), partial(self._normalize, x, mu, sigma))


if __name__ == "__main__":
    # Read a dataset with an underfolder source
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(input_path)
    dataset = source()

    # Apply the normalization op
    op = NormalizeOp()
    norm_dataset = op(dataset)

    # Write the dataset with an underfolder sink
    output_path = Path(gettempdir()) / "normalize_images_example"
    sink = UnderfolderSink(output_path)
    sink(norm_dataset)
