from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset


class DatasetSink[T: AnyDataset](ABC):
    @abstractmethod
    def consume(self, data: T) -> None:
        pass

    def __call__(self, data: T) -> None:
        self.consume(data)
