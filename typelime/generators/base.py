from abc import ABC, abstractmethod

from typelime._op_typing import AnyDataset


class _DatasetGenerator[T: AnyDataset](ABC):
    @abstractmethod
    def generate(self) -> T:
        pass

    def __call__(self) -> T:
        return self.generate()
