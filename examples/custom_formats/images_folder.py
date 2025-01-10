from pathlib import Path
from tempfile import gettempdir

import numpy as np

from pipewine import (
    Dataset,
    DatasetSink,
    DatasetSource,
    Grabber,
    Item,
    JpegParser,
    LazyDataset,
    LocalFileReader,
    StoredItem,
    TypedSample,
    write_item_to_file,
)


class ImageSample(TypedSample):
    image: Item[np.ndarray]


class ImagesFolderSource(DatasetSource[Dataset[ImageSample]]):
    # Inherit from DatasetSource and specify the type of data that will be read.
    # In this case, we only want to read a single dataset with a custom sample type.

    def __init__(self, folder: Path) -> None:
        super().__init__()  # Always call the superclass constructor!
        self._folder = folder
        self._files: list[Path]

    def __call__(self) -> Dataset[ImageSample]:
        # Find all JPEG files in the folder in lexicographic order.
        jpeg_files = filter(lambda x: x.suffix == ".jpeg", self._folder.iterdir())
        self._files = sorted(list(jpeg_files))

        # Return a lazy dataset thet loads the samples with the _get_sample method
        return LazyDataset(len(self._files), self._get_sample)

    def _get_sample(self, idx: int) -> ImageSample:
        # Create an Item that reads a JPEG image from the i-th file.
        reader = LocalFileReader(self._files[idx])
        parser = JpegParser()
        image_item = StoredItem(reader, parser)

        # Return an ImageSample that only contains the image item.
        return ImageSample(image=image_item)


class ImagesFolderSink(DatasetSink[Dataset[ImageSample]]):
    # Inherit from DatasetSink and specify the type of data that will be written.
    # In this case, we only want to write a single dataset with a custom sample type.

    def __init__(self, folder: Path, grabber: Grabber | None = None) -> None:
        super().__init__()  # Always call the superclass constructor!
        self._folder = folder
        self._grabber = grabber or Grabber()

    def __call__(self, data: Dataset[ImageSample]) -> None:
        self._folder.mkdir(parents=True, exist_ok=True)

        # Compute the amount of 0-padding to preserve lexicographic order.
        zpadding = len(str(len(data)))

        # Iterate over the dataset and write each sample.
        for i, sample in self.loop(data, self._grabber, name="Writing"):
            fname = self._folder / f"{str(i).zfill(zpadding)}.jpeg"
            write_item_to_file(sample.image, fname)


if __name__ == "__main__":
    # Read a dataset with an images folder source
    input_path = Path("tests/sample_data/underfolders/underfolder_0/data")
    source = ImagesFolderSource(input_path)
    dataset = source()

    # Write the dataset with an underfolder sink
    output_path = Path(gettempdir()) / "images_folder_example"
    sink = ImagesFolderSink(output_path)
    sink(dataset)
