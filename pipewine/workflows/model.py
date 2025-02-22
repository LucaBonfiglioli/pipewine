"""Base classes and components for building workflows."""

import shutil
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from inspect import get_annotations
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
"""Type alias for 'Actions', which can be dataset sources, operators or sinks."""


class CheckpointFactory(ABC):
    """Base class for factories that create and destroy checkpoints.

    Subclasses should implement the `create` and `destroy` methods.
    """

    @abstractmethod
    def create[
        T: Sample
    ](
        self, execution_id: str, name: str, sample_type: type[T], grabber: Grabber
    ) -> tuple[DatasetSink[Dataset[T]], DatasetSource[Dataset[T]]]:
        """Create a checkpoint for a given execution and name.

        Args:
            execution_id: The unique identifier for the current workflow execution.
            name: The name of the checkpoint.
            sample_type: The type of the samples that will be stored in the checkpoint.
            grabber: The grabber that will be used to write the dataset.
        """
        pass

    @abstractmethod
    def destroy(self, execution_id: str, name: str) -> None:
        """Destroy a checkpoint for a given execution and name.

        Args:
            execution_id: The unique identifier for the current workflow execution.
            name: The name of the checkpoint.
        """
        pass


class UnderfolderCheckpointFactory(CheckpointFactory):
    """A checkpoint factory that creates checkpoints as underfolder datasets."""

    def __init__(self, folder: Path | None = None) -> None:
        """
        Args:
            folder (Path, optional): The folder where the checkpoints will be stored.
                Defaults to None, in which case the folder will be a temporary
                directory.
        """
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
    """Sentinel object that represents a default value for an optional argument."""

    def __repr__(self) -> str:
        return "Default"

    @classmethod
    def get[T](cls, *optionals: T | "Default", default: T) -> T:
        """Get the first non-default value from a list of optionals.

        Returns:
            T: The first non-default value found in the list of optionals. If no
                non-default value is found, the default value is returned.
        """
        for maybe in optionals:
            if not isinstance(maybe, Default):
                return maybe
        return default


@dataclass
class WfOptions:
    """Options for the execution of a workflow node. These options can be passed to
    both when creating a `Workflow` object or when adding a single node to the workflow,
    in which case the options will be used only for that node.
    """

    cache: bool | Default = field(default_factory=Default)
    """Whether to cache the output of the node. If True, a `CacheOp` operator will be
    automatically applied to the output of the node. If False, no caching will be
    performed.
    """
    cache_type: type | Default = field(default_factory=Default)
    """The type of the cache to use. If not specified, a default cache type will be
    used. This option is only relevant if `cache` is True.
    """
    cache_params: dict[str, Any] | Default = field(default_factory=Default)
    """Additional parameters to pass to the cache constructor. This option is only 
    relevant if `cache` is True.
    """
    checkpoint: bool | Default = field(default_factory=Default)
    """Whether to create a checkpoint for the node, automatically writing the output
    of a node to disk to avoid recomputing all lazy operations. If False, no checkpoint 
    will be created.
    """
    checkpoint_factory: CheckpointFactory | Default = field(default_factory=Default)
    """The factory to use to create and destroy checkpoints. If not specified, a default
    factory will be used. This option is only relevant if `checkpoint` is True.
    """
    checkpoint_grabber: Grabber | Default = field(default_factory=Default)
    """The grabber to use to write the checkpoint. If not specified, a default grabber
    will be used. This option is only relevant if `checkpoint` is True.
    """
    collect_after_checkpoint: bool | Default = field(default_factory=Default)
    """Whether to force the garbage collector to run after checkpointing the output
    of the node.
    """
    destroy_checkpoints: bool | Default = field(default_factory=Default)
    """Whether to destroy the checkpoints after the workflow execution is finished."""

    def __repr__(self) -> str:
        opts = [
            f"{k}={v}" for k, v in self.__dict__.items() if not isinstance(v, Default)
        ]
        opts_repr = ", ".join(opts)
        return f"{self.__class__.__name__}({opts_repr})"


@dataclass(unsafe_hash=True)
class Node[T: AnyAction]:
    """A node in a workflow graph."""

    name: str
    """The name of the node."""
    action: T = field(hash=False)
    """The action to be executed."""
    options: WfOptions = field(default_factory=WfOptions, hash=False, compare=False)
    """The options for the execution of the node."""


class All:
    """Sentinel object that represents all elements in a sequence or mapping."""

    def __hash__(self) -> int:
        return 1

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __eq__(self, value: object) -> bool:
        return isinstance(value, All)


@dataclass(unsafe_hash=True)
class Proxy:
    """A proxy object that represents the input or output of a node in a workflow graph."""

    node: Node
    """The node that the proxy is associated with."""
    socket: int | str | None | All
    """The socket of the node that the proxy is associated with. 

    - If set to an integer value, the proxy represents the output of a node that returns
        a sequence of datasets, and the value represents the index of the dataset in the
        sequence.
    - If set to a string value, the proxy represents the output of a node that returns
        a mapping of datasets, and the value represents the key of the dataset in the
        mapping.
    - If set to None, the proxy represents the output of a node that returns a single
        dataset, thus needing no index or key.
    - If set to All, the proxy represents all the outputs of a node that returns a
        collection of datasets and it is used to connect the whole collection to another
        node.
    """


@dataclass(unsafe_hash=True)
class Edge:
    """An edge in a workflow graph that connects two nodes."""

    src: Proxy
    """The source of the edge."""
    dst: Proxy
    """The destination of the edge."""


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
    """A workflow is a directed acyclic graph of nodes that represent actions to be
    executed in a specific order. The nodes are connected by edges that represent the
    flow of data between the nodes.

    See the "Workflows" section in the Pipewine documentation for more information.
    """

    _INPUT_NAME = "input"
    _OUTPUT_NAME = "output"

    def __init__(self, options: WfOptions | None = None) -> None:
        """
        Args:
            options (WfOptions, optional): The options for the workflow. Defaults to
                None, in which case default options will be used.
        """
        self._options = options or WfOptions()
        self._nodes: set[Node] = set()
        self._nodes_by_name: dict[str, Node] = {}
        self._inbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._outbound_edges: dict[Node, set[Edge]] = defaultdict(set)
        self._name_counters: dict[str, int] = defaultdict(int)

    @property
    def options(self) -> WfOptions:
        """The options for the workflow."""
        return self._options

    def _gen_node_name(self, action: AnyAction) -> str:
        title = action.__class__.__name__
        self._name_counters[title] += 1
        return f"{title}_{self._name_counters[title]}"

    def get_nodes(self) -> set[Node]:
        """Get all the nodes in the workflow."""
        return self._nodes

    def get_node(self, name: str) -> Node | None:
        """Get a node by name."""
        return self._nodes_by_name.get(name)

    def get_inbound_edges(self, node: Node) -> set[Edge]:
        """Get the inbound edges of a node.

        Args:
            node (Node): The node to get the inbound edges for.

        Returns:
            set[Edge]: The inbound edges of the node.
        """
        if node not in self._inbound_edges:
            msg = f"Node '{node.name}' not found"
            raise ValueError(msg)

        return self._inbound_edges[node]

    def get_outbound_edges(self, node: Node) -> set[Edge]:
        """Get the outbound edges of a node.

        Args:
            node (Node): The node to get the outbound edges for.

        Returns:
            set[Edge]: The outbound edges of the node.
        """
        if node not in self._outbound_edges:
            msg = f"Node '{node.name}' not found"
            raise ValueError(msg)

        return self._outbound_edges[node]

    def node[
        T: AnyAction
    ](self, action: T, name: str | None = None, options: WfOptions | None = None) -> T:
        """Wrap any action into a workflow node and then add it to the workflow.

        Args:
            action (T): The action to be executed. This can be a dataset source,
                operator or sink.
            name (str, optional): The name of the node. If not specified, a name will be
                generated automatically. Defaults to None.
            options (WfOptions, optional): The options for the execution of the node.
                Defaults to None, in which case default workflow options will be used.

        Raises:
            ValueError: if the name is already associated to another node.
            ValueError: if the action has the wrong type or it is not annotated properly.

        Returns:
            T: A callable object that mimics the action's signature and that, when
                called, instead of executing the action (as the original object would
                do), it will just tell the workflow to create the necessary edges to
                connect the action to other nodes.
        """
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
                    ann := get_annotations(action.__call__, eval_str=True)["return"],
                    GenericAlias,
                )
                and len(ann.__args__) > 0
                and ann.__args__[-1] is not Ellipsis
            ):
                # If the size of the tuple is statically known, we can allow iter() and
                # len() in the returned proxy object.
                return_val = tuple(Proxy(node, i) for i in range(len(ann.__args__)))
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
