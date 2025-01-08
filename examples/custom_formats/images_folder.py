from pipewine import (
    DatasetSource,
    DatasetSink,
    Dataset,
    TypedSample,
    Item,
    StoredItem,
    LazyDataset,
    JpegParser,
    LocalFileReader,
    write_item_to_file,
)
import numpy as np
from pathlib import Path
import imageio


class ImageSample(TypedSample):
    image: Item[np.ndarray]


class ImagesFolderSource(DatasetSource[Dataset[ImageSample]]):
    def __init__(self, folder: Path) -> None:
        self._folder = folder
        self._files: list[Path]

    def _get_sample(self, idx: int) -> ImageSample:
        reader = LocalFileReader(self._files[idx])
        parser = JpegParser()
        image_item = StoredItem(reader, parser)
        return ImageSample(image=image_item)

    def __call__(self) -> Dataset[ImageSample]:
        jpeg_files = filter(lambda x: x.suffix == ".jpeg", self._folder.iterdir())
        self._files = list(jpeg_files)
        return LazyDataset(len(self._files), self._get_sample)


class ImagesFolderSink(DatasetSink[Dataset[ImageSample]]):
    def __init__(self, folder: Path) -> None:
        self._folder = folder

    def __call__(self, data: Dataset[ImageSample]) -> None:
        zpadding = len(str(len(data)))
        for i, sample in enumerate(data):
            fname = self._folder / f"{str(i).zfill(zpadding)}.jpeg"
            write_item_to_file(sample.image, fname)
