import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import gettempdir
from types import GenericAlias
from typing import Any, Iterator, cast, overload

from pipewine.bundle import Bundle
from pipewine.dataset import Dataset
from pipewine.grabber import Grabber
from pipewine.operators import DatasetOperator
from pipewine.sample import Sample
from pipewine.sinks import DatasetSink, UnderfolderSink
from pipewine.sources import DatasetSource, UnderfolderSource

AnyAction = DatasetSource | DatasetOperator | DatasetSink


class CheckpointFactory(ABC):
    @abstractmethod
    def create[
        T: Sample
    ](
        self, execution_id: str, name: str, sample_type: type[T], grabber: Grabber
    ) -> tuple[DatasetSink[Dataset[T]], DatasetSource[Dataset[T]]]: ...

    @abstractmethod
    def destroy(self, execution_id: str, name: str) -> None: ...


class UnderfolderCheckpointFactory(CheckpointFactory):
    def __init__(self, folder: Path | None = None) -> None:
        self._folder = folder or Path(gettempdir()) / "pipewine_workflows"

    def create[
        T: Sample
    ](
        self, execution_id: str, name: str, sample_type: type[T], grabber: Grabber
    ) -> tuple[DatasetSink[Dataset[T]], DatasetSource[Dataset[T]]]:
        path = self._folder / execution_id / name
        sink = UnderfolderSink(path, grabber=grabber)
        source = UnderfolderSource(path, sample_type=sample_type)
        return sink, source

    def destroy(self, execution_id: str, name: str) -> None:
        rm_path = self._folder / execution_id / name
        if rm_path.is_dir():
            shutil.rmtree(rm_path)


class Default:
    def __repr__(self) -> str:
        return "Default"

    @classmethod
    def get[T](cls, *optionals: T | "Default", default: T) -> T:
        for maybe in optionals:
            if not isinstance(maybe, Default):
                return maybe
        return default


@dataclass
class WfOptions:
    cache: bool | Default = field(default_factory=Default)
    cache_type: type | Default = field(default_factory=Default)
    cache_params: dict[str, Any] | Default = field(default_factory=Default)
    checkpoint: bool | Default = field(default_factory=Default)
    checkpoint_factory: CheckpointFactory | Default = field(default_factory=Default)
    checkpoint_grabber: Grabber | Default = field(default_factory=Default)
    collect_after_checkpoint: bool | Default = field(default_factory=Default)
    destroy_checkpoints: bool | Default = field(default_factory=Default)

    def __repr__(self) -> str:
        opts = [
            f"{k}={v}" for k, v in self.__dict__.items() if not isinstance(v, Default)
        ]
        opts_repr = ", ".join(opts)
        return f"{self.__class__.__name__}({opts_repr})"


@dataclass(unsafe_hash=True)
class Node[T: AnyAction]:
    name: str
    action: T = field(hash=False)
    options: WfOptions = field(default_factory=WfOptions, hash=False, compare=False)


class All:
    def __hash__(self) -> int:
        return 1

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __eq__(self, value: object) -> bool:
        return isinstance(value, All)


@dataclass(unsafe_hash=True)
class Proxy:
    node: Node
    socket: int | str | None | All


@dataclass(unsafe_hash=True)
class Edge:
    src: Proxy
    dst: Proxy


class _ProxySequence[T](Sequence[T]):
    def __init__(self, factory: Callable[[int], T]) -> None:
        self._data: list[T] = []
        self._factory = factory

    def __len__(self) -> int:
        raise RuntimeError("Proxy sequences do not support len().")

    @overload
    def __getitem__(self, idx: int) -> T: ...
    @overload
    def __getitem__(self, idx: slice) -> "_ProxySequence[T]": ...
    def __getitem__(self, idx: int | slice) -> "T | _ProxySequence[T]":
        if isinstance(idx, slice):
            raise RuntimeError("Proxy sequences do not support slicing.")
        while idx >= len(self._data):
            self._data.append(self._factory(len(self._data)))
        return self._data[idx]

    def __iter__(self) -> Iterator[T]:
        raise RuntimeError("Proxy sequences do not support iter().")


class _ProxyMapping[V](Mapping[str, V]):
    def __init__(
        self, factory: Callable[[str], V], data: Mapping[str, V] | None = None
    ) -> None:
        super().__init__()
        self._factory = factory
        self._data = {**data} if data is not None else {}

    def __len__(self) -> int:
        raise NotImplementedError("Proxy mapppings do not support len().")

    def __getitem__(self, key: str) -> V:
        if key not in self._data:
            self._data[key] = self._factory(key)
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError("Proxy mapppings do not support iter().")


class _ProxyBundle[T](Bundle[T]):
    def __init__(self, **data: T) -> None:
        for k, v in data.items():
            setattr(self, k, v)


class Workflow:
    _INPUT_NAME = "input"
    _OUTPUT_NAME = "output"

    def __init__(self, options: WfOptions | None = None) -> None:
        self._options = options or WfOptions()
        self._nodes: set[Node] = set()
        self._nodes_by_name: dict[str, Node] = {}
        self._inbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._outbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._name_counters: dict[str, int] = defaultdict(int)

    @property
    def options(self) -> WfOptions:
        return self._options

    def _gen_node_name(self, action: AnyAction) -> str:
        title = action.__class__.__name__
        self._name_counters[title] += 1
        return f"{title}_{self._name_counters[title]}"

    def get_nodes(self) -> set[Node]:
        return self._nodes

    def get_node(self, name: str) -> Node | None:
        return self._nodes_by_name.get(name)

    def get_inbound_edges(self, node: Node) -> set[Edge]:
        if node not in self._inbound_edges:
            msg = f"Node '{node.name}' not found"
            raise ValueError(msg)

        return self._inbound_edges[node]

    def get_outbound_edges(self, node: Node) -> set[Edge]:
        if node not in self._outbound_edges:
            msg = f"Node '{node.name}' not found"
            raise ValueError(msg)

        return self._outbound_edges[node]

    def node[
        T: AnyAction
    ](self, action: T, name: str | None = None, options: WfOptions | None = None) -> T:
        name = name or self._gen_node_name(cast(AnyAction, action))
        if name in self._nodes_by_name:
            raise ValueError(f"Name {name} is already associated to another node.")
        options = options or WfOptions()
        node = Node(name=name, action=action, options=options)
        self._nodes.add(node)
        self._nodes_by_name[node.name] = node
        self._inbound_edges[node] = set()
        self._outbound_edges[node] = set()

        action_ = cast(AnyAction, action)
        return_val: Proxy | Sequence[Proxy] | Mapping[str, Proxy] | Bundle[Proxy] | None
        if isinstance(action_, DatasetSink):
            return_val = None
        else:
            return_t = action_.output_type
            if issubclass(return_t, Dataset):
                return_val = Proxy(node, None)
            elif (
                issubclass(return_t, tuple)
                and isinstance(
                    ann := action.__call__.__annotations__["return"], GenericAlias
                )
                and len(ann.__args__) > 0
                and ann.__args__[-1] is not Ellipsis
            ):
                # If the size of the tuple is statically known, we can allow iter() and
                # len() in the returned proxy object.
                return_val = [Proxy(node, i) for i in range(len(ann.__args__))]
            elif issubclass(return_t, Sequence):
                return_val = _ProxySequence(lambda idx: Proxy(node, idx))
            elif issubclass(return_t, Mapping):
                return_val = _ProxyMapping(lambda k: Proxy(node, k))
            elif issubclass(return_t, Bundle):
                fields = return_t.__dataclass_fields__
                return_val = _ProxyBundle(**{k: Proxy(node, k) for k in fields})
            else:  # pragma: no cover (unreachable)
                raise ValueError(f"Unknown type '{return_t}'")

        def connect(*args, **kwargs):
            everything = list(args) + list(kwargs.values())
            edges: list[Edge] = []
            for arg in everything:
                if isinstance(arg, Proxy):
                    edges.append(Edge(arg, Proxy(node, None)))
                elif isinstance(arg, _ProxySequence):
                    orig_node = arg._factory(0).node
                    edges.append(Edge(Proxy(orig_node, All()), Proxy(node, All())))
                elif isinstance(arg, Sequence):
                    edges.extend([Edge(x, Proxy(node, i)) for i, x in enumerate(arg)])
                elif isinstance(arg, _ProxyMapping):
                    orig_node = cast(Node, arg._factory("a").node)
                    edges.append(Edge(Proxy(orig_node, All()), Proxy(node, All())))
                elif isinstance(arg, Mapping):
                    edges.extend([Edge(v, Proxy(node, k)) for k, v in arg.items()])
                elif isinstance(arg, Bundle):
                    edges.extend(
                        [Edge(v, Proxy(node, k)) for k, v in arg.as_dict().items()]
                    )
                else:  # pragma: no cover (unreachable)
                    raise ValueError(f"Unknown type '{type(arg)}'")

            for edge in edges:
                self._inbound_edges[edge.dst.node].add(edge)
                self._outbound_edges[edge.src.node].add(edge)

            return return_val

        return connect  # type: ignore
