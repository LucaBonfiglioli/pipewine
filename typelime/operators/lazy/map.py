from typelime._op_typing import AnyDataset
from typelime.operators.lazy.base import LazyDatasetOperator
from typelime.sample import Sample
from typelime.mappers import Mapper


class _State:
    pass


# class MapOp[T_IN: AnyDataset, T_SAMPLE_OUT: Sample](
#     LazyDatasetOperator[T_IN, T_SAMPLE_OUT, _State]
# ):
#     def __init__(self, mapper: Mapper[T_IN, T_SAMPLE_OUT])

#     def prepare(self, x: T_IN) -> _State:
#         return super().prepare(x)

#     def size(self, state: _State) -> int:
#         return super().size(state)

#     def get_sample(self, state: _State, idx: int) -> T_SAMPLE_OUT:
#         return super().get_sample(state, idx)
