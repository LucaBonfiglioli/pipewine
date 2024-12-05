from abc import ABCMeta
from collections import defaultdict
from collections.abc import Callable, Generator, Sequence
from typing import Any, TypeVar
from uuid import uuid1

from typelime.grabber import Grabber


class RegisterMeta(ABCMeta):
    __typelime_title__: str
    __typelime_namespace__: str

    _registered: dict[str, dict[str, dict[str, type]]] = defaultdict(
        lambda: defaultdict(dict)
    )

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        ns: dict[str, Any],
        /,
        title: str | None = None,
        namespace: str | None = None,
    ):
        the_cls = super().__new__(cls, name, bases, ns)
        title = the_cls.__name__ if title is None else title
        namespace = cls._ns_str(the_cls) if namespace is None else namespace
        the_cls.__typelime_title__ = title
        the_cls.__typelime_namespace__ = namespace
        if not the_cls.__abstractmethods__:
            cls._registered[the_cls._type()][namespace][title] = the_cls
        return the_cls

    @staticmethod
    def reset() -> None:
        RegisterMeta._registered.clear()

    @classmethod
    def _ns_str(cls, the_cls: type) -> str:
        return the_cls.__module__.partition(".")[0]

    def _type(self) -> str:
        return ""

    def get(self, full_title: str) -> type | None:
        namespace, _, title = full_title.partition(".")
        if title == "":
            namespace, title = self._ns_str(RegisterMeta), full_title
        print(namespace, title)
        return self._registered.get(self._type(), {}).get(namespace, {}).get(title)

    @property
    def registered(self) -> list[str]:
        result = []
        by_type = self._registered[self._type()]
        for ns, by_ns in by_type.items():
            for k in by_ns:
                result.append(f"{ns}.{k}")
        return result

    @property
    def title(self) -> str:
        return self.__typelime_title__

    @property
    def namespace(self) -> str:
        return self.__typelime_namespace__


T = TypeVar("T")

CallbackType = Callable[[str, int, Sequence[T]], None]


class RegisterCallbackMixin(metaclass=RegisterMeta):
    def __init__(self) -> None:
        super().__init__()
        self._callbacks: list[CallbackType] = []

    def register_callback(self, cb: CallbackType) -> None:
        self._callbacks.append(cb)

    def loop[
        T
    ](self, seq: Sequence[T], grabber: Grabber, name: str | None = None) -> Generator[
        tuple[int, T]
    ]:
        if name is None:
            name = self.__class__.title + uuid1().hex
        with grabber(seq) as ctx:
            for i, x in ctx:
                for cb in self._callbacks:
                    cb(name, i, seq)
                yield i, x
