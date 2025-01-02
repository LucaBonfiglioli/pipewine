from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Iterator, cast

from pipewine.bundle import Bundle, DefaultBundle
from pipewine.dataset import Dataset
from pipewine.operators import DatasetOperator
from pipewine.sinks import DatasetSink
from pipewine.sources import DatasetSource


class _DefaultList[T](Sequence[T]):
    def __init__(self, factory: Callable[[int], T], *args: T) -> None:
        self._data = list(args)
        self._factory = factory

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> T:  # type: ignore
        while idx >= len(self):
            self._data.append(self._factory(len(self)))
        return self._data[idx]

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)


class _DefaultDict[K, V](Mapping[K, V]):
    def __init__(
        self, factory: Callable[[K], V], data: Mapping[K, V] | None = None
    ) -> None:
        super().__init__()
        self._factory = factory
        self._data = {**data} if data is not None else {}

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, key: K) -> V:
        if key not in self._data:
            self._data[key] = self._factory(key)
        return self._data[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)


AnyAction = DatasetSource | DatasetOperator | DatasetSink


@dataclass(unsafe_hash=True)
class Proxy:
    node: "Node"
    socket: int | str | None


@dataclass(unsafe_hash=True)
class Node[T: AnyAction]:
    name: str
    action: T = field(hash=False)


@dataclass(unsafe_hash=True)
class Edge:
    src: Proxy
    dst: Proxy


class Workflow:
    _INPUT_NAME = "input"
    _OUTPUT_NAME = "output"

    def __init__(self) -> None:
        self._nodes: set[Node] = set()
        self._nodes_by_name: dict[str, Node] = {}
        self._inbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._outbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._name_counters: dict[str, int] = defaultdict(int)

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

    def node[T: AnyAction](self, action: T, name: str | None = None) -> T:
        name = name or self._gen_node_name(cast(AnyAction, action))
        node = Node(name=name, action=action)
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
            elif issubclass(return_t, Sequence):
                return_val = _DefaultList(lambda idx: Proxy(node, idx))
            elif issubclass(return_t, Mapping):
                return_val = _DefaultDict(lambda k: Proxy(node, k))
            elif issubclass(return_t, Bundle):
                return_val = DefaultBundle(lambda k: Proxy(node, k))
            else:  # pragma: no cover (unreachable)
                raise ValueError(f"Unknown type '{return_t}'")

        def connect(*args, **kwargs):
            everything = list(args) + list(kwargs.values())
            edges: list[Edge] = []
            for arg in everything:
                if isinstance(arg, Proxy):
                    edges.append(Edge(arg, Proxy(node, None)))
                elif isinstance(arg, Sequence):
                    edges.extend([Edge(x, Proxy(node, i)) for i, x in enumerate(arg)])
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
