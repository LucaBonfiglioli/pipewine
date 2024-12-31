import numpy as np
from pydantic import BaseModel

from pipewine import Dataset, FilterOp, GroupByOp, Item, MapOp, SortOp, TypedSample
from collections.abc import Callable
import pytest


class LetterMetadata(BaseModel):
    letter: str
    color: str


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[LetterMetadata]
    shared: Item


class TestFilterOp:
    @staticmethod
    def _filter_blue(idx: int, sample: LetterSample) -> bool:
        return sample.metadata().color == "blue"

    @staticmethod
    def _filter_green(idx: int, sample: LetterSample) -> bool:
        return sample.metadata().color == "green"

    @staticmethod
    def _filter_f(idx: int, sample: LetterSample) -> bool:
        return sample.metadata().letter == "f"

    @staticmethod
    def _filter_none(idx: int, sample: LetterSample) -> bool:
        return sample.metadata().letter == "?"

    @staticmethod
    def _filter_even(idx: int, sample: LetterSample) -> bool:
        return idx % 2 == 0

    @pytest.mark.parametrize(
        ["fn", "expected_len"],
        [
            [_filter_blue, 3],
            [_filter_green, 4],
            [_filter_f, 1],
            [_filter_none, 0],
            [_filter_even, 13],
        ],
    )
    def test_call(
        self,
        letter_dataset: Dataset[LetterSample],
        fn: Callable[[int, LetterSample], bool],
        expected_len: int,
    ) -> None:
        op = FilterOp(fn)
        out = op(letter_dataset)
        assert len(out) == expected_len


class TestGroupByOp:

    @staticmethod
    def _group_color(idx: int, sample: LetterSample) -> str:
        return sample.metadata().color

    @staticmethod
    def _group_letter(idx: int, sample: LetterSample) -> str:
        return sample.metadata().letter

    @staticmethod
    def _group_even(idx: int, sample: LetterSample) -> str:
        return "even" if idx % 2 == 0 else "odd"

    @pytest.mark.parametrize(
        ["fn", "expected"],
        [
            [
                _group_color,
                {
                    "black": 1,  # h
                    "blue": 3,  # blo
                    "brown": 4,  # ktuz
                    "cyan": 2,  # dq
                    "green": 4,  # crvw
                    "grey": 3,  # fpx
                    "orange": 1,  # e
                    "pink": 1,  # n
                    "purple": 1,  # g
                    "red": 3,  # ams
                    "yellow": 3,  # ijy
                },
            ],
            [_group_letter, {char: 1 for char in "abcdefghijklmnopqrstuvwxyz"}],
            [_group_even, {"even": 13, "odd": 13}],
        ],
    )
    def test_call(
        self,
        letter_dataset: Dataset[LetterSample],
        fn: Callable[[int, LetterSample], str],
        expected: dict[str, int],
    ) -> None:
        op = GroupByOp(fn)
        out = op(letter_dataset)
        assert set(out.keys()) == set(expected.keys())
        for k in out:
            assert len(out[k]) == expected[k]


class TestSortOp:
    @staticmethod
    def _sort_color_letter(idx: int, sample: LetterSample) -> str:
        return sample.metadata().color + sample.metadata().letter

    @pytest.mark.parametrize(
        ["fn", "expected_letters"],
        [
            [
                _sort_color_letter,
                "hbloktuzdqcrvwfpxengamsijy",
            ]
        ],
    )
    def test_call(
        self,
        letter_dataset: Dataset[LetterSample],
        fn: Callable[[int, LetterSample], str],
        expected_letters: str,
    ) -> None:
        op = SortOp(fn)
        out = op(letter_dataset)
        assert len(out) == len(expected_letters)
        for x, exp in zip(out, expected_letters):
            assert x.metadata().letter == exp
