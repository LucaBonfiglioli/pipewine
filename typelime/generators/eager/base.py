from typelime._setuppable import Setuppable
from typelime._op_typing import AnyDataset
from typelime.generators.base import _DatasetGenerator


class EagerDatasetGenerator[T: AnyDataset](_DatasetGenerator[T], Setuppable):
    pass
