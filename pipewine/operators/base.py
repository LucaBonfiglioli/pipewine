"""`DatasetOperator` base class definition and basic operators."""

from abc import ABC, abstractmethod
from inspect import get_annotations

from pipewine._op_typing import AnyDataset, origin_type
from pipewine._register import LoopCallbackMixin
from pipewine.dataset import Dataset
from pipewine.sample import Sample


class DatasetOperator[T_IN: AnyDataset, T_OUT: AnyDataset](ABC, LoopCallbackMixin):
    """Base class for all Pipewine dataset operators, which are functions that transform
    datasets into other datasets.

    All dataset operator classes must inherit from this class and implement the
    `__call__` method, and **must** provide type hints to enable type inference,
    required by other pipewine components.

    Dataset operator classes can be parameterized by the types of the input and output
    datasets, `T_IN` and `T_OUT`, respectively. These generic types are bound to one
    of the following types:
    - `Dataset`: A single dataset.
    - `Sequence[Dataset]`: A sequence of datasets.
    - `tuple[Dataset, ...]`: A tuple of datasets, with the possibility of having
        statically known length and types.
    - `Mapping[str, Dataset]`: A mapping of dataset names to datasets.
    - `Bundle[Dataset]`: A bundle of datasets, with statically known field names and
        types.
    """

    @abstractmethod
    def __call__(self, x: T_IN) -> T_OUT:
        """Transform the input dataset (or collection) into another dataset (or
        collection).

        This method **must** always be correctly annotated with type hints to enable
        type inference by other pipewine components. Failing to do so will result in
        type errors when composing operators into workflows, or when registering
        them to the Pipewine CLI.

        Args:
            x (T_IN): The input dataset (or collection) to be transformed.

        Returns:
            T_OUT: The transformed output dataset (or collection).
        """
        pass

    @property
    def input_type(self):
        """Infer the origin type of this operator's input, returning the origin of the
        `__call__` method's `x` parameter.
        """
        return origin_type(get_annotations(self.__call__, eval_str=True)["x"])

    @property
    def output_type(self):
        """Infer the origin type of this operator's input, returning the origin of the
        `__call__` method's return.
        """
        return origin_type(get_annotations(self.__call__, eval_str=True)["return"])


class IdentityOp(DatasetOperator[Dataset, Dataset]):
    """Identity operator that accepts a dataset and returns it unchanged. Useful for
    testing and debugging purposes, or when a no-op is needed in a workflow.
    """

    def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
        return x
