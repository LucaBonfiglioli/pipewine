import random
from collections import deque
from collections.abc import Sequence
from enum import Enum
from typing import Annotated, Any

from typer import Option

from typelime.cli.main import op_cli
from typelime.grabber import Grabber
from typelime.operators import *
from typelime.sample import Sample


def deep_get(sample: Sample, key: str) -> Any:
    sep = "."
    sub_keys = key.split(sep)
    item_key, other_keys = sub_keys[0], deque(sub_keys[1:])
    current = sample[item_key]()
    while len(other_keys) > 0:
        current_key = other_keys.popleft()
        if isinstance(current, Sequence):
            current = current[int(current_key)]
        else:
            current = current[current_key]
    return current


class Compare(str, Enum):
    eq = "eq"
    neq = "neq"
    gt = "gt"
    lt = "lt"
    ge = "ge"
    le = "le"


key_help = "Filter by the value of key (e.g. metadata.mylist.12.foo)."
compare_help = "How to compare with the target."
target_help = "The target value (gets autocasted to the key value)."
negate_help = "Invert the filtering criterion."


@op_cli()
def clone() -> IdentityOp:
    """Copy a dataset, applying no changes to any sample."""
    return IdentityOp()


@op_cli(name="filter")
def filter_(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
    compare: Annotated[Compare, Option(..., "--compare", "-c", help=compare_help)],
    target: Annotated[str, Option(..., "--target", "-t", help=target_help)],
    negate: Annotated[bool, Option(..., "--negate", "-n", help=negate_help)] = False,
) -> FilterOp:
    """Keep only the samples that satisfy a certain logical comparison with a target."""

    def _filter_fn(idx: int, sample: Sample) -> bool:
        value = deep_get(sample, key)
        if type(value) != bool:
            target_ = type(value)(target)
        else:
            target_ = str(target).lower() in ["yes", "true", "y", "ok", "t", "1"]
        if compare == Compare.eq:
            result = value == target_
        elif compare == Compare.neq:
            result = value != target_
        elif compare == Compare.gt:
            result = value > target_
        elif compare == Compare.lt:
            result = value < target_
        elif compare == Compare.ge:
            result = value >= target_
        else:
            result = value <= target_
        return result

    return FilterOp(_filter_fn, negate=negate, grabber=grabber)


key_help = "Group by the value of the key (e.g. metadata.mylist.12.foo)."


@op_cli()
def groupby(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
) -> GroupByOp:
    """Group together samples with the same value associated to the specified key."""

    def _groupby_fn(idx: int, sample: Sample) -> str:
        return str(deep_get(sample, key))

    return GroupByOp(_groupby_fn, grabber=grabber)


key_help = "Sorting key (e.g. metadata.mylist.12.foo)."
reverse_help = "Sort instead by non-increasing values."


@op_cli()
def sort(
    grabber: Grabber,
    key: Annotated[str, Option(..., "--key", "-k", help=key_help)],
    reverse: Annotated[bool, Option(..., "--reverse", "-r", help=reverse_help)] = False,
) -> SortOp:
    """Sort samples by non-decreasing values associated with the specified key."""

    def _sort_fn(idx: int, sample: Sample) -> Any:
        return deep_get(sample, key)

    return SortOp(_sort_fn, reverse=reverse, grabber=grabber)


@op_cli(name="slice")
def slice_(
    start: Annotated[int, Option(help="Start index.")] = None,  # type: ignore
    stop: Annotated[int, Option(help="Stop index.")] = None,  # type: ignore
    step: Annotated[int, Option(help="Slice step size.")] = None,  # type: ignore
) -> SliceOp:
    """Slice a dataset as you would do with any Python sequence."""
    return SliceOp(start=start, stop=stop, step=step)


times_help = "The number of times to repeat the dataset."
interleave_help = "Instead of ABCABCABCABC, do AAAABBBBCCCC."


@op_cli()
def repeat(
    times: Annotated[int, Option(..., "--times", "-t", help=times_help)],
    interleave: Annotated[
        bool, Option(..., "--interleave", "-I", help=interleave_help)
    ] = False,
) -> RepeatOp:
    """Repeat a dataset N times replicating the samples."""
    return RepeatOp(times, interleave=interleave)


@op_cli()
def cycle(
    length: Annotated[int, Option(..., "--n", "-n", help="Desired number of samples.")]
) -> CycleOp:
    """Repeat the samples until a certain number of samples is reached."""
    return CycleOp(length)


@op_cli()
def reverse() -> ReverseOp:
    """Reverse the order of the samples."""
    return ReverseOp()


length_help = "Desired number of samples."
pad_with_help = "Index of the sample (within the dataset) to use as padding."


@op_cli()
def pad(
    length: Annotated[int, Option(..., "--length", "-l", help=length_help)],
    pad_with: Annotated[int, Option(..., "--pad-width", "-p", help=pad_with_help)] = -1,
) -> PadOp:
    """Pad a dataset until it reaches a specified length."""
    return PadOp(length, pad_with=pad_with)


@op_cli()
def cat() -> CatOp:
    """Concatenate two or more datasets into a single dataset."""
    return CatOp()


@op_cli(name="zip")
def zip_() -> ZipOp:
    """Zip two or more datasets of the same length by merging the individual samples."""
    return ZipOp()


@op_cli()
def shuffle(
    seed: Annotated[int, Option(..., "--seed", "-s", help="Random seed.")] = -1
) -> ShuffleOp:
    """Shuffle the samples of a dataset in random order."""
    if seed >= 0:
        random.seed(seed)
    return ShuffleOp()


batch_size_help = "The number of samples per batch."


@op_cli()
def batch(
    batch_size: Annotated[int, Option(..., "--batch-size", "-b", help=batch_size_help)],
) -> BatchOp:
    """Split a dataset into batches of the specified size."""
    return BatchOp(batch_size)


@op_cli()
def chunk(
    chunks: Annotated[int, Option(..., "--chunk", "-c", help="The number of chunks.")]
) -> ChunkOp:
    """Split a dataset into N chunks."""
    return ChunkOp(chunks)


splits_help = (
    "The size of each dataset, either as exact values (int) or fraction"
    " (float). You can set at most one value to 'null' to mean 'all the"
    " remaining samples'."
)


@op_cli()
def split(
    sizes: Annotated[list[str], Option(..., "-s", "--sizes", help=splits_help)]
) -> SplitOp:
    """Split a dataset into parts with custom size."""
    parsed_sizes = []
    for x in sizes:
        if x == "null":
            parsed = None
        elif "." in x:
            parsed = float(x)
        else:
            parsed = int(x)
        parsed_sizes.append(parsed)
    return SplitOp(parsed_sizes)
