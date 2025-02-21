"""`Mapper` base class definition."""

from abc import ABC, abstractmethod

from pipewine.sample import Sample


class Mapper[T_IN: Sample, T_OUT: Sample](ABC):
    """Base class for all Pipewine mappers, which are functions that transform
    individual samples of a dataset.

    All mapper classes must inherit from this class and implement the `__call__`
    method.

    Mapper classes can be parameterized by the types of the input and output
    samples, `T_IN` and `T_OUT`, respectively. These types must be subclasses of
    `Sample`.
    """

    @abstractmethod
    def __call__(self, idx: int, x: T_IN) -> T_OUT:
        """Transform a sample of type `T_IN` into another sample of type `T_OUT`.

        Args:
            idx (int): The index of the sample in the dataset.
            x (T_IN): The input sample to be transformed.

        Returns:
            T_OUT: The transformed output sample.
        """
        pass
