from abc import ABC, abstractmethod
from collections.abc import Sequence
from functools import partial
from typing import cast

from typelime._op_typing import AnyDataset
from typelime.bundle import Bundle
from typelime.dataset import Dataset
from typelime.operators import DatasetOperator
from typelime.sinks import DatasetSink
from typelime.sources import DatasetSource
from typelime.workflows.model import AnyAction, Node, Proxy, Workflow
from typelime.workflows.tracking import EventQueue, TaskUpdateEvent


class WorkflowExecutor(ABC):
    @abstractmethod
    def execute(self, workflow: Workflow) -> None: ...

    @abstractmethod
    def attach(self, event_queue: EventQueue) -> None: ...

    @abstractmethod
    def detach(self) -> None: ...


class NaiveWorkflowExecutor(WorkflowExecutor):
    def __init__(self) -> None:
        super().__init__()
        self._eq: EventQueue | None = None

    def attach(self, event_queue: EventQueue) -> None:
        if self._eq is not None:
            raise RuntimeError("Already attached to another event queue.")
        self._eq = event_queue

    def detach(self) -> None:
        if self._eq is None:
            raise RuntimeError("Not attached to any event queue.")
        self._eq = None

    def execute(self, workflow: Workflow) -> None:
        sorted_graph = self._topological_sort(workflow)
        state: dict[Proxy, Dataset] = {}
        for node in sorted_graph:
            self._execute_node(workflow, node, state)

    def _progress_callback(
        self, node: Node, loop_id: str, idx: int, seq: Sequence
    ) -> None:
        if self._eq is not None:
            task = f"{node.name}/{loop_id}"
            event = TaskUpdateEvent(task, idx, len(seq))
            self._eq.emit(event)

    def _execute_node(
        self,
        workflow: Workflow,
        node: Node,
        state: dict[Proxy, Dataset],
    ) -> None:
        action = cast(AnyAction, node.action)
        action.register_callback(partial(self._progress_callback, node))
        edges = workflow.get_inbound_edges(node)

        all_none = len(edges) == 1 and all(x.dst.socket is None for x in edges)
        all_int = all(isinstance(x.dst.socket, int) for x in edges)
        all_str = all(isinstance(x.dst.socket, str) for x in edges)
        assert len(edges) == 0 or all_none or all_int or all_str

        if len(edges) == 0:
            assert isinstance(action, DatasetSource)
            output = action()
        else:
            assert isinstance(action, (DatasetOperator, DatasetSink))
            input_: AnyDataset
            if all_none:
                edge = next(iter(edges))
                input_ = state[edge.src]
            elif all_int:
                inputs = [None] * len(edges)
                for edge in edges:
                    inputs[cast(int, edge.src.socket)] = state[edge.src]  # type: ignore
                if issubclass(action.input_type, tuple):
                    input_ = tuple(inputs)  # type: ignore
                else:
                    input_ = list(inputs)  # type: ignore
            else:
                inputs = {}
                for edge in edges:
                    inputs[cast(str, edge.src.socket)] = state[edge.src]
                if issubclass(action.input_type, Bundle):
                    input_ = action.input_type(**inputs)
                else:
                    input_ = inputs

            output = action(input_)

        if output is None:
            return
        if isinstance(output, Dataset):
            state[Proxy(node, None)] = output
        elif isinstance(output, Sequence):
            for i, dataset in enumerate(output):
                state[Proxy(node, i)] = dataset
        elif isinstance(output, dict):
            for k, v in output.items():
                state[Proxy(node, k)] = v
        elif isinstance(output, Bundle):
            for k, v in output.as_dict().items():
                state[Proxy(node, k)] = v

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
