"""Base classes for Pipewine parsers."""

from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Iterable, KeysView
from typing import Any


class ParserRegistry:
    """Container for currently registered parsers, allowing other parts of the code to
    retrieve the most appropriate parser class for a given file format.
    """

    _registered_parsers: dict[str, type["Parser"]] = {}

    @classmethod
    def get(cls, key: str) -> type["Parser"] | None:
        """Get the parser class for a given file format.

        Args:
            key (str): File format extension.

        Returns:
            type[Parser]: Parser class for the given file format, or None if no parser
                is currently registered for the format.
        """
        return cls._registered_parsers.get(key)

    @classmethod
    def keys(cls) -> KeysView[str]:
        """Get the keys of the currently registered parsers."""
        return cls._registered_parsers.keys()


class _ParserMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        /,
        **kwargs: Any,
    ):
        the_cls: type["Parser"] = super().__new__(cls, name, bases, namespace, **kwargs)  # type: ignore
        extensions = the_cls.extensions()  # type: ignore
        if extensions is not None:
            ParserRegistry._registered_parsers.update({k: the_cls for k in extensions})
        return the_cls


class Parser[T: Any](ABC, metaclass=_ParserMeta):
    """Base class for Pipewine parsers, which are used to parse data from bytes and
    vice versa. Parser classes are parameterized by the type `T` of the data they parse.

    Subclasses should implement the `parse`, `dump`, and `extensions` methods.

    All subclasses of this class are automatically registered in the `ParserRegistry`
    on import.
    """

    def __init__(self, type_: type[T] | None = None):
        """Initialize the parser, make sure to call the super constructor in subclasses.

        Args:
            type_ (type[T] | None, optional): Optional concrete type `T` of the returned
                data, if known. Used by some parsers to enforce the return type of the
                parsed data using information only available at runtime. Defaults to
                None, in which case the type of the parsed data is not enforced and can
                be any subclass of `T`.
        """
        super().__init__()
        self._type = type_

    @property
    def type_(self) -> type[T] | None:
        """Get the concrete type `T` of the parsed data, if known."""
        return self._type

    @abstractmethod
    def parse(self, data: bytes) -> T:
        """Parse data from bytes. Implementations can access the `type_` attribute, if
        set, to enforce the return type of the parsed data.

        Args:
            data (bytes): A byte string containing the data to parse.

        Returns:
            T: The parsed data.
        """
        pass

    @abstractmethod
    def dump(self, data: T) -> bytes:
        """Dump data to bytes.

        Args:
            data (T): The data to dump.

        Returns:
            bytes: A byte string containing the dumped data.
        """
        pass

    @classmethod
    @abstractmethod
    def extensions(cls) -> Iterable[str]:
        """Get the file extensions associated with the parser."""
        pass
