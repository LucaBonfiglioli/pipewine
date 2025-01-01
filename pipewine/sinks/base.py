from abc import ABC, abstractmethod

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin


class DatasetSink[T: AnyDataset](ABC, LoopCallbackMixin):
    @abstractmethod
    def __call__(self, data: T) -> None: ...

    @property
    def input_type(self):
        return origin_type(self.__call__.__annotations__["data"])
