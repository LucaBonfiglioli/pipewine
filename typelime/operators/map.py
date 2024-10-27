from functools import partial

from typelime.dataset import Dataset, LazyDataset
from typelime.mappers import Mapper
from typelime.operators.base import DatasetOperator
from typelime.sample import Sample


class MapOp[T_IN: Sample, T_OUT: Sample](
    DatasetOperator[Dataset[T_IN], LazyDataset[T_OUT]]
):
    def __init__(self, mapper: Mapper[T_IN, T_OUT]) -> None:
        self._mapper = mapper

    def _get_sample(self, x: Dataset[T_IN], idx: int) -> T_OUT:
        return self._mapper(idx, x[idx])

    def apply(self, x: Dataset[T_IN]) -> LazyDataset[T_OUT]:
        return LazyDataset(len(x), partial(self._get_sample, x))
