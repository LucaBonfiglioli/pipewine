"""Base class for all dataset sinks."""

from abc import ABC, abstractmethod
from inspect import get_annotations

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin


class DatasetSink[T: AnyDataset](ABC, LoopCallbackMixin):
    """Base class for all dataset sinks, used to consume datasets, typically at the end
    of a pipeline by writing them to disk or to some other storage.

    Subclasses should implement the `__call__` method, and **must** provide type hints
    to enable type inference, required by other pipewine components.

    Dataset sink classes can be parameterized by the type of the dataset they consume,
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
    def __call__(self, data: T) -> None:
        """Consume the input dataset (or collection).

        This method **must** always be correctly annotated with type hints to enable
        type inference by other pipewine components. Failing to do so will result in
        type errors when composing operators into workflows, or when registering
        them to the Pipewine CLI.

        Args:
            data (T): The input dataset (or collection) to be consumed.
        """
        pass

    @property
    def input_type(self):
        """Infer the origin type of this sink's input, returning the origin of the
        `__call__` method's `data` parameter.
        """
        return origin_type(get_annotations(self.__call__, eval_str=True)["data"])
