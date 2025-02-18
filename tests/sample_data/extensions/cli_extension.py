from pathlib import Path
from typing import Annotated

import numpy as np
import pydantic
from typer import Option

from pipewine import *
from pipewine.cli import wf_cli
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


def group_fn(idx: int, sample: LetterSample) -> str:
    return (
        "vowel" if sample.metadata().letter in sample.shared().vowels else "consonant"
    )


def sort_fn(idx: int, sample: LetterSample) -> float:
    return sample.image().mean().item()


@wf_cli(name="example")
def example(
    input: Annotated[Path, Option(..., "-i", "--input", help="Input folder.")],
    output: Annotated[Path, Option(..., "-o", "--output", help="Output folder.")],
    workers: Annotated[int, Option(..., "-w", "--workers", help="Num workers.")] = 0,
) -> Workflow:
    grabber = Grabber(workers, 50)
    wf = Workflow(WfOptions(checkpoint_grabber=grabber))
    data = wf.node(UnderfolderSource(input, sample_type=LetterSample))()
    data = wf.node(RepeatOp(100))(data)
    data = wf.node(MapOp(ColorJitter()), options=WfOptions(checkpoint=True))(data)
    groups = wf.node(GroupByOp(group_fn, grabber=None))(data)
    vowels = groups["vowel"]
    consonants = groups["consonant"]
    vowels = wf.node(SortOp(sort_fn, grabber=None))(vowels)
    consonants = wf.node(SortOp(sort_fn, grabber=None))(consonants)
    data = wf.node(CatOp())([vowels, consonants])
    wf.node(UnderfolderSink(output, grabber=grabber))(data)
    return wf
