from typelime._op_typing import AnyDataset
from typelime._setuppable import Setuppable
from typelime.operators.base import _DatasetOperator


class EagerDatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](
    _DatasetOperator[T_IN, T_OUT], Setuppable
):
    pass
