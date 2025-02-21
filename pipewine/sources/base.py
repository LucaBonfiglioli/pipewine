"""Base classes for defining data sources."""

from abc import ABC, abstractmethod

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin
from pipewine.dataset import Dataset, LazyDataset
from pipewine.sample import Sample
from inspect import get_annotations


class DatasetSource[T: AnyDataset](ABC, LoopCallbackMixin):
    """Base class for all dataset sources, used to produce datasets, typically at the
    beginning of a pipeline by reading them from disk or from some other storage.

    Subclasses should implement the `__call__` method, and **must** provide type hints
    to enable type inference, required by other pipewine components.

    Dataset source classes can be parameterized by the type of the dataset they produce,
    `T`, which can be one of the following types:
    - `Dataset`: A single dataset.
    - `Sequence[Dataset]`: A sequence of datasets.
    - `tuple[Dataset, ...]`: A tuple of datasets, with the possibility of having
        statically known length and types.
    - `Mapping[str, Dataset]`: A mapping of dataset names to datasets.
    - `Bundle[Dataset]`: A bundle of datasets, with statically known field names and
        types.
    """

    @abstractmethod
    def __call__(self) -> T:
        """Produce a dataset (or collection).

        This method **must** always be correctly annotated with type hints to enable
        type inference by other pipewine components. Failing to do so will result in
        type errors when composing operators into workflows, or when registering
        them to the Pipewine CLI.

        Returns:
            T: The produced dataset (or collection).
        """
        pass

    @property
    def output_type(self):
        """Infer the origin type of this source's output, returning the origin of the
        `__call__` method's return.
        """
        return origin_type(get_annotations(self.__call__, eval_str=True)["return"])


class _LazySourceInterface[T_SAMPLE: Sample](ABC):
    def _prepare(self) -> None:
        pass

    @abstractmethod
    def _size(self) -> int:
        pass

    @abstractmethod
    def _get_sample(self, idx: int) -> T_SAMPLE:
        pass
