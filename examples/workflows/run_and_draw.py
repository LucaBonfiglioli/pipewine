from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pydantic

from pipewine import *
from pipewine.workflows import *


class LetterMetadata(pydantic.BaseModel):
    letter: str
    color: str


class SharedMetadata(pydantic.BaseModel):
    vowels: list[str]
    consonants: list[str]


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[LetterMetadata]
    shared: Item[SharedMetadata]


class ColorJitter(Mapper[LetterSample, LetterSample]):
    def __call__(self, idx: int, x: LetterSample) -> LetterSample:
        image = x.image()
        col = np.random.randint(0, 255, (1, 1, 3))
        alpha = np.random.uniform(0.1, 0.9, [])
        image = (image * alpha + col * (1 - alpha)).clip(0, 255).astype(np.uint8)
        return x.with_values(image=image)


class GroupCat(DatasetOperator[Mapping[str, Dataset], Dataset]):
    def __call__(self, x: Mapping[str, Dataset]) -> Dataset:
        return CatOp()(list(x.values()))


def group_fn(idx: int, sample: LetterSample) -> str:
    return "vowel" if sample.metadata().letter in "aeiou" else "consonant"


def sort_fn(idx: int, sample: LetterSample) -> float:
    return float(sample.image().mean())


def main_wf(
    source: DatasetSource[Dataset[LetterSample]],
    sink_a: DatasetSink[Dataset[LetterSample]],
    sink_b: DatasetSink[Dataset[LetterSample]],
    grabber: Grabber,
):
    wf = Workflow(WfOptions(checkpoint_grabber=grabber))
    data = wf.node(source)()
    data = wf.node(RepeatOp(1000))(data)
    data = wf.node(MapOp(ColorJitter()), options=WfOptions(checkpoint=True))(data)
    groups = wf.node(GroupByOp(group_fn))(data)
    vowels = groups["vowel"]
    consonants = groups["consonant"]
    data = wf.node(GroupCat())(groups)
    wf.node(sink_a)(data)
    vowels = wf.node(SortOp(sort_fn))(vowels)
    consonants = wf.node(SortOp(sort_fn))(consonants)
    data = wf.node(CatOp())([vowels, consonants])
    wf.node(sink_b)(data)
    run_workflow(wf, tracker=CursesTracker())
    draw_workflow(wf, Path("/tmp/wf.svg"))


def main_nowf(
    source: DatasetSource[Dataset[LetterSample]],
    sink_a: DatasetSink[Dataset[LetterSample]],
    sink_b: DatasetSink[Dataset[LetterSample]],
):
    data = source()
    data = RepeatOp(10)(data)
    data = MapOp(ColorJitter())(data)
    UnderfolderSink(ckpt_path := Path("/tmp/checkpoint"), grabber=grabber)
    data = UnderfolderSource(ckpt_path, sample_type=LetterSample)()
    groups = GroupByOp(group_fn)(data)
    vowels = groups["vowel"]
    consonants = groups["consonant"]
    data = GroupCat()(groups)
    sink_a(data)
    vowels = SortOp(sort_fn)(vowels)
    consonants = SortOp(sort_fn)(consonants)
    data = CatOp()([vowels, consonants])
    sink_b(data)


if __name__ == "__main__":
    input_ = Path("tests/sample_data/underfolders/underfolder_0")
    out_a = Path("/tmp/out_a")
    out_b = Path("/tmp/out_b")
    grabber = Grabber(8, 50)
    source = UnderfolderSource(input_, sample_type=LetterSample)
    sink_a = UnderfolderSink(
        out_a, grabber=grabber, overwrite_policy=OverwritePolicy.OVERWRITE
    )
    sink_b = UnderfolderSink(
        out_b, grabber=grabber, overwrite_policy=OverwritePolicy.OVERWRITE
    )
    main_wf(source, sink_a, sink_b, grabber)
    main_nowf(source, sink_a, sink_b)
