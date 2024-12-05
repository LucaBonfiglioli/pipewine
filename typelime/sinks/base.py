from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset, origin_type
from typelime._register import RegisterMeta, RegisterCallbackMixin


class DatasetSinkMeta(RegisterMeta):
    def _type(self) -> str:
        return "sink"


class DatasetSink[T: AnyDataset](ABC, RegisterCallbackMixin, metaclass=DatasetSinkMeta):
    @abstractmethod
    def __call__(self, data: T) -> None: ...

    @property
    def input_type(self):
        return origin_type(self.__call__.__annotations__["data"])
