import typing as t
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial

from typelime.bundle import Bundle, DefaultBundle
from typelime.dataset import Dataset
from typelime.operators import DatasetOperator
from typelime.sinks import DatasetSink
from typelime.sources import DatasetSource
from typelime.workflows.tracking import TaskUpdateEvent, EventQueue


class _DefaultList[T](Sequence[T]):
    def __init__(self, factory: Callable[[int], T], *args: T) -> None:
        self._data = list(args)
        self._factory = factory

    def __len__(self) -> int:
        return len(self._data)

    def __getitem__(self, idx: int) -> T:  # type: ignore
        while idx >= len(self):
            self._data.append(self._factory(idx))
        return self._data[idx]

    def __iter__(self) -> t.Iterator[T]:
        return iter(self._data)


AnyAction = DatasetSource | DatasetOperator | DatasetSink


@dataclass(unsafe_hash=False, eq=False)
class Socket:
    name: str

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Socket):
            return False

        return value.name == self.name


class InputSocket(Socket):
    pass


class OutputSocket(Socket):
    pass


@dataclass(unsafe_hash=False)
class Endpoint[T: Socket]:
    node: "Node"
    socket: T

    def __hash__(self) -> int:
        return hash((self.socket, self.node))


class InputEndpoint(Endpoint[InputSocket]):
    pass


class OutputEndpoint(Endpoint[OutputSocket]):
    pass


class EmptyBundle(Bundle):
    pass


AnyInput = None | InputEndpoint | Sequence[InputEndpoint] | Bundle[InputEndpoint]
AnyOutput = None | OutputEndpoint | Sequence[OutputEndpoint] | Bundle[OutputEndpoint]


@dataclass(unsafe_hash=False, eq=False)
class Node[T_IN: AnyInput, T_OUT: AnyOutput]:
    name: str
    action: AnyAction
    input: T_IN
    output: T_OUT

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Node):
            return False

        return value.name == self.name


@dataclass(unsafe_hash=False)
class Edge:
    src: OutputEndpoint
    dst: InputEndpoint

    def __hash__(self) -> int:
        return hash((self.src, self.dst))


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
        title = action.__class__.title
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

    @t.overload
    def node(
        self, action: DatasetSource[Dataset], name: str | None = None
    ) -> Node[None, OutputEndpoint]: ...

    @t.overload
    def node(
        self, action: DatasetSource[Sequence[Dataset]], name: str | None = None
    ) -> Node[None, Sequence[OutputEndpoint]]: ...

    @t.overload
    def node(
        self,
        action: DatasetSource[Mapping[str, Dataset]] | DatasetSource[Bundle[Dataset]],
        name: str | None = None,
    ) -> Node[None, Bundle[OutputEndpoint]]: ...

    @t.overload
    def node(
        self, action: DatasetSink[Dataset], name: str | None = None
    ) -> Node[InputEndpoint, None]: ...

    @t.overload
    def node(
        self, action: DatasetSink[Sequence[Dataset]], name: str | None = None
    ) -> Node[Sequence[InputEndpoint], None]: ...

    @t.overload
    def node(
        self,
        action: DatasetSink[Mapping[str, Dataset]] | DatasetSink[Bundle[Dataset]],
        name: str | None = None,
    ) -> Node[Bundle[InputEndpoint], None]: ...

    @t.overload
    def node(
        self,
        action: DatasetOperator[Sequence[Dataset], Dataset],
        name: str | None = None,
    ) -> Node[Sequence[InputEndpoint], OutputEndpoint]: ...

    @t.overload
    def node(
        self,
        action: DatasetOperator[Sequence[Dataset], Sequence[Dataset]],
        name: str | None = None,
    ) -> Node[Sequence[InputEndpoint], Sequence[OutputEndpoint]]: ...

    @t.overload
    def node(
        self,
        action: (
            DatasetOperator[Sequence[Dataset], Mapping[str, Dataset]]
            | DatasetOperator[Sequence[Dataset], Bundle[Dataset]]
        ),
        name: str | None = None,
    ) -> Node[Sequence[InputEndpoint], Bundle[OutputEndpoint]]: ...

    @t.overload
    def node(
        self,
        action: (
            DatasetOperator[Mapping[str, Dataset], Dataset]
            | DatasetOperator[Bundle[Dataset], Dataset]
        ),
        name: str | None = None,
    ) -> Node[Bundle[InputEndpoint], OutputEndpoint]: ...

    @t.overload
    def node(
        self,
        action: (
            DatasetOperator[Mapping[str, Dataset], Sequence[Dataset]]
            | DatasetOperator[Bundle[Dataset], Sequence[Dataset]]
        ),
        name: str | None = None,
    ) -> Node[Bundle[InputEndpoint], Sequence[OutputEndpoint]]: ...

    @t.overload
    def node(
        self,
        action: (
            DatasetOperator[Mapping[str, Dataset], Mapping[str, Dataset]]
            | DatasetOperator[Mapping[str, Dataset], Bundle[Dataset]]
            | DatasetOperator[Bundle[Dataset], Mapping[str, Dataset]]
            | DatasetOperator[Bundle[Dataset], Bundle[Dataset]]
        ),
        name: str | None = None,
    ) -> Node[Bundle[InputEndpoint], Bundle[OutputEndpoint]]: ...

    @t.overload
    def node(
        self, action: DatasetOperator[Dataset, Dataset], name: str | None = None
    ) -> Node[InputEndpoint, OutputEndpoint]: ...

    @t.overload
    def node(
        self,
        action: DatasetOperator[Dataset, Sequence[Dataset]],
        name: str | None = None,
    ) -> Node[InputEndpoint, Sequence[OutputEndpoint]]: ...

    @t.overload
    def node(
        self,
        action: (
            DatasetOperator[Dataset, Mapping[str, Dataset]]
            | DatasetOperator[Dataset, Bundle[Dataset]]
        ),
        name: str | None = None,
    ) -> Node[InputEndpoint, Bundle[OutputEndpoint]]: ...

    def node(self, action: AnyAction, name: str | None = None) -> Node:
        name = name or self._gen_node_name(action)
        node = Node(name=name, action=action, input=None, output=None)
        if isinstance(action, DatasetSource):
            input = None
            output = self._auto_output(action.output_type, node)
        elif isinstance(action, DatasetSink):
            input = self._auto_input(action.input_type, node)
            output = None
        else:  # isinstance(action, DatasetOperator)
            input = self._auto_input(action.input_type, node)
            output = self._auto_output(action.output_type, node)

        # Need to bruteforce here, i'm sorry type-checker :(
        node.input = input  # type: ignore
        node.output = output  # type: ignore

        self._nodes.add(node)
        self._nodes_by_name[node.name] = node
        self._inbound_edges[node] = set()
        self._outbound_edges[node] = set()
        return node

    def _auto_input(self, input_t: type, node: Node) -> AnyInput:
        input_: AnyInput
        if issubclass(input_t, Dataset):
            input_ = InputEndpoint(node, InputSocket("return"))
        elif issubclass(input_t, Sequence):
            input_ = _DefaultList(
                lambda idx: InputEndpoint(node, InputSocket(str(idx)))
            )
        elif issubclass(input_t, (Mapping, Bundle)):
            input_ = DefaultBundle(lambda key: InputEndpoint(node, InputSocket(key)))
        else:
            raise NotImplementedError()

        return input_

    def _auto_output(self, output_t: type, node: Node) -> AnyOutput:
        output: AnyOutput
        if issubclass(output_t, Dataset):
            output = OutputEndpoint(node, OutputSocket("return"))
        elif issubclass(output_t, Sequence):
            output = _DefaultList(
                lambda idx: OutputEndpoint(node, OutputSocket(str(idx)))
            )
        elif issubclass(output_t, (Mapping, Bundle)):
            output = DefaultBundle(lambda key: OutputEndpoint(node, OutputSocket(key)))
        else:
            raise NotImplementedError()

        return output

    def node_remove(self, node: Node) -> None:
        if node not in self._nodes:
            msg = f"Node '{node.name}' not found"
            raise ValueError(msg)
        for edge in self._inbound_edges[node]:
            self._outbound_edges[edge.src.node].remove(edge)
        self._inbound_edges[node].clear()
        del self._inbound_edges[node]
        for edge in self._outbound_edges[node]:
            self._inbound_edges[edge.dst.node].remove(edge)
        self._outbound_edges[node].clear()
        del self._outbound_edges[node]
        self._nodes.remove(node)
        del self._nodes_by_name[node.name]

    def edge(self, src_endpoint: OutputEndpoint, dst_endpoint: InputEndpoint) -> Edge:
        edge = Edge(src_endpoint, dst_endpoint)
        if edge.src.node not in self._nodes and edge.src.node:
            msg = f"Source node '{edge.src.node.name}' not found"
            raise ValueError(msg)
        if edge.dst.node not in self._nodes and edge.dst.node:
            msg = f"Destination node '{edge.dst.node.name}' not found"
            raise ValueError(msg)
        if edge in self._inbound_edges[edge.dst.node]:
            msg = (
                f"Edge from '{edge.src.node.name}' to '{edge.dst.node.name}' "
                f"already exists"
            )
            raise ValueError(msg)
        self._inbound_edges[edge.dst.node].add(edge)
        self._outbound_edges[edge.src.node].add(edge)
        return edge

    def edge_remove(self, edge: Edge) -> None:
        if (
            edge.dst.node not in self._inbound_edges
            or edge not in self._inbound_edges[edge.dst.node]
        ):
            msg = (
                f"Edge from '{edge.src.node.name}.{edge.src.socket.name}' "
                f"to '{edge.dst.node.name}.{edge.dst.socket.name}' not found"
            )
            raise ValueError(msg)
        self._inbound_edges[edge.dst.node].remove(edge)
        self._outbound_edges[edge.src.node].remove(edge)


class WorkflowExecutor(ABC):
    @abstractmethod
    def execute(self, workflow: Workflow) -> None:
        pass


class NaiveWorkflowExecutor(WorkflowExecutor):
    def __init__(self, event_queue: EventQueue) -> None:
        super().__init__()
        self._event_q = event_queue

    def execute(self, workflow: Workflow) -> None:
        self._check(workflow)
        sorted_graph = self._topological_sort(workflow)
        state: dict[OutputEndpoint, Dataset] = {}
        for node in sorted_graph:
            self._execute_node(workflow, node, state)

    def _progress_callback(
        self, node: Node, loop_id: str, idx: int, seq: Sequence
    ) -> None:
        task = f"{node.name}/{loop_id}"
        event = TaskUpdateEvent(task, idx, len(seq))
        self._event_q.emit(event)

    def _execute_node(
        self,
        workflow: Workflow,
        node: Node[AnyInput, AnyOutput],
        state: dict[OutputEndpoint, Dataset],
    ) -> None:
        node.action.register_callback(partial(self._progress_callback, node))
        if node.input is None:
            assert isinstance(node.action, DatasetSource)
            outputs = node.action()
        elif isinstance(node.input, InputEndpoint):
            ep = next(iter(workflow.get_inbound_edges(node))).src
            outputs = node.action(state[ep])  # type: ignore
        elif isinstance(node.input, Sequence):
            edges = workflow.get_inbound_edges(node)
            input_map = {x.dst: x.src for x in edges}
            inputs = [input_map[x] for x in node.input]
            outputs = node.action(inputs)  # type: ignore
        else:
            edges = workflow.get_inbound_edges(node)
            input_map = {x.dst: x.src for x in edges}
            inputs = {k: input_map[v] for k, v in node.input.as_dict().items()}
            outputs = node.action(inputs)  # type: ignore

        if outputs is None:
            return
        if isinstance(outputs, Dataset):
            assert isinstance(node.output, OutputEndpoint)
            state[node.output] = outputs
        elif isinstance(outputs, Sequence):
            assert isinstance(node.output, Sequence)
            for ep, data in zip(node.output, outputs):
                state[ep] = data
        else:
            assert isinstance(node.output, Bundle)
            for k, v in node.output.as_dict().items():
                state[v] = outputs[k]

    def _topological_sort(self, workflow: Workflow) -> list[Node]:
        result: list[Node] = []
        mark: dict[Node, int] = {}

        def visit(node: Node) -> None:
            mark_of_node = mark.get(node, 0)
            if mark_of_node == 1:
                raise ValueError("The graph contains cycles.")
            if mark_of_node == 2:
                return
            mark[node] = 1
            for edge in workflow.get_outbound_edges(node):
                visit(edge.dst.node)
            mark[node] = 2
            result.append(node)

        for node in workflow.get_nodes():
            visit(node)
        return result[::-1]

    def _check(self, workflow: Workflow) -> None:
        disc_set: set[InputEndpoint] = set()
        for node in workflow.get_nodes():
            if node.input is None:
                continue
            if isinstance(node.input, InputEndpoint):
                node_endpoints = {node.input}
            elif isinstance(node.input, Sequence):
                node_endpoints = {*node.input}
            else:
                assert isinstance(node.input, Bundle)
                node_endpoints = {*node.input.as_dict().values()}
            edge_endpoints = {x.dst for x in workflow.get_inbound_edges(node)}
            disc_set.update(node_endpoints - edge_endpoints)

        if disc_set:
            disc_repr = {f"{x.node.name}.{x.socket.name}" for x in disc_set}
            msg = f"Disconnected input sockets: {disc_repr}"
            raise ValueError(msg)
