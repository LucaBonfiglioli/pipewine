"""This module contains the implementation necessary for the execution of workflows."""

import gc
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from functools import partial
from typing import cast
from uuid import uuid1

from pipewine._op_typing import AnyDataset
from pipewine.bundle import Bundle
from pipewine.dataset import Dataset
from pipewine.grabber import Grabber
from pipewine.operators import CacheOp, DatasetOperator
from pipewine.operators.cache import LIFOCache
from pipewine.sinks import DatasetSink
from pipewine.sources import DatasetSource
from pipewine.workflows.model import (
    All,
    AnyAction,
    Default,
    Node,
    Proxy,
    UnderfolderCheckpointFactory,
    WfOptions,
    Workflow,
)
from pipewine.workflows.tracking import (
    EventQueue,
    TaskCompleteEvent,
    TaskStartEvent,
    TaskUpdateEvent,
)


def _on_enter_cb(
    queue: EventQueue | None, node: Node, loop_id: str, total: int
) -> None:
    if queue is not None:
        task = f"{node.name}/{loop_id}"
        event = TaskStartEvent(task, total)
        queue.emit(event)


def _on_iter_cb(queue: EventQueue | None, node: Node, loop_id: str, idx: int) -> None:
    if queue is not None:
        task = f"{node.name}/{loop_id}"
        event = TaskUpdateEvent(task, idx)
        queue.emit(event)


def _on_exit_cb(queue: EventQueue | None, node: Node, loop_id: str) -> None:
    if queue is not None:
        task = f"{node.name}/{loop_id}"
        event = TaskCompleteEvent(task)
        queue.emit(event)


class WorkflowExecutor(ABC):
    """Abstract class for workflow executors, which are responsible for executing
    the actions contained in a workflow in the correct order.

    Subclasses must implement the `execute`, `attach`, and `detach` methods.
    """

    @abstractmethod
    def execute(self, workflow: Workflow) -> None:
        """Executes the given workflow.

        Args:
            workflow (Workflow): The workflow to execute.
        """
        pass

    @abstractmethod
    def attach(self, event_queue: EventQueue) -> None:
        """Attaches the executor to an event queue, allowing it to emit events.

        Args:
            event_queue (EventQueue): The event queue to attach to.
        """
        pass

    @abstractmethod
    def detach(self) -> None:
        """Detaches the executor from the event queue."""
        pass


class SequentialWorkflowExecutor(WorkflowExecutor):
    """A workflow executor that executes the actions in a workflow in a sequential
    manner, respecting the dependencies between the nodes.
    """

    def __init__(self) -> None:
        """Initializes the executor."""
        super().__init__()
        self._eq: EventQueue | None = None
        self._def_cache = True
        self._def_cache_type = LIFOCache
        self._def_cache_params = {"maxsize": 1}
        self._def_checkpoint = False
        self._def_checkpoint_factory = UnderfolderCheckpointFactory()
        self._def_checkpoint_grabber = Grabber()
        self._def_collect_after_checkpoint = True
        self._def_destroy_checkpoints = True

    def attach(self, event_queue: EventQueue) -> None:
        if self._eq is not None:
            raise RuntimeError("Already attached to another event queue.")
        self._eq = event_queue

    def detach(self) -> None:
        if self._eq is None:
            raise RuntimeError("Not attached to any event queue.")
        self._eq = None

    def _register_all_cbs(self, action: AnyAction, node: Node) -> None:
        action.register_on_iter(partial(_on_iter_cb, self._eq, node))
        action.register_on_enter(partial(_on_enter_cb, self._eq, node))
        action.register_on_exit(partial(_on_exit_cb, self._eq, node))

    def _execute_node(
        self,
        workflow: Workflow,
        node: Node,
        state: dict[Proxy, AnyDataset],
        id_: str,
        wf_opts: WfOptions,
    ) -> None:
        action = cast(AnyAction, node.action)
        self._register_all_cbs(action, node)
        edges = workflow.get_inbound_edges(node)
        all_or_none = len(edges) == 1 and all(
            x.dst.socket is None or isinstance(x.dst.socket, All) for x in edges
        )
        all_int = all(isinstance(x.dst.socket, (int, All)) for x in edges)
        all_str = all(isinstance(x.dst.socket, (str, All)) for x in edges)
        assert len(edges) == 0 or all_or_none or all_int or all_str

        if len(edges) == 0:
            assert isinstance(action, DatasetSource)
            output = action()
        else:
            assert isinstance(action, (DatasetOperator, DatasetSink))
            input_: AnyDataset
            if all_or_none:
                edge = next(iter(edges))
                input_ = state[edge.src]
            elif all_int:
                inputs_list = [None] * len(edges)
                for edge in edges:
                    inputs_list[cast(int, edge.dst.socket)] = state[edge.src]  # type: ignore
                if issubclass(action.input_type, tuple):
                    input_ = tuple(inputs_list)  # type: ignore
                else:
                    input_ = list(inputs_list)  # type: ignore
            else:
                inputs_dict = {}
                for edge in edges:
                    inputs_dict[cast(str, edge.dst.socket)] = state[edge.src]
                if issubclass(action.input_type, Bundle):
                    input_ = action.input_type(**inputs_dict)
                else:
                    input_ = inputs_dict

            output = action(input_)

        if output is None:
            return
        state[Proxy(node, All())] = output
        if isinstance(output, Dataset):
            self._handle_output(state, Proxy(node, None), output, id_, wf_opts)
        elif isinstance(output, Sequence):
            for i, dataset in enumerate(output):
                self._handle_output(state, Proxy(node, i), dataset, id_, wf_opts)
        elif isinstance(output, Mapping):
            for k, v in output.items():
                self._handle_output(state, Proxy(node, k), v, id_, wf_opts)
        else:
            assert isinstance(output, Bundle)
            for k, v in output.as_dict().items():
                self._handle_output(state, Proxy(node, k), v, id_, wf_opts)

    def _handle_output(
        self,
        state: dict[Proxy, AnyDataset],
        proxy: Proxy,
        dataset: Dataset,
        id_: str,
        wf_opts: WfOptions,
    ) -> None:
        opts = proxy.node.options
        ckpt = Default.get(
            opts.checkpoint, wf_opts.checkpoint, default=self._def_checkpoint
        )
        if (
            ckpt
            and len(dataset) > 0
            and not isinstance(proxy.node.action, DatasetSource)
        ):
            ckpt_fact = Default.get(
                opts.checkpoint_factory,
                wf_opts.checkpoint_factory,
                default=self._def_checkpoint_factory,
            )
            grabber = Default.get(
                opts.checkpoint_grabber,
                wf_opts.checkpoint_grabber,
                default=self._def_checkpoint_grabber,
            )
            name = proxy.node.name
            if proxy.socket is not None:
                name += str(proxy.socket)
            sink, source = ckpt_fact.create(id_, name, type(dataset[0]), grabber)
            self._register_all_cbs(sink, proxy.node)
            self._register_all_cbs(source, proxy.node)
            sink(dataset)

            collect = Default.get(
                opts.collect_after_checkpoint,
                wf_opts.collect_after_checkpoint,
                default=self._def_collect_after_checkpoint,
            )
            if collect:
                del dataset
                gc.collect()

            dataset = source()

        cache = Default.get(opts.cache, wf_opts.cache, default=self._def_cache)
        if cache:
            cache_type = Default.get(
                opts.cache_type, wf_opts.cache_type, default=self._def_cache_type
            )
            cache_params = Default.get(
                opts.cache_params, wf_opts.cache_params, default=self._def_cache_params
            )
            cache_op = CacheOp(cache_type=cache_type, **{**cache_params})
            dataset = cache_op(dataset)

        state[proxy] = dataset

    def _topological_sort(self, workflow: Workflow) -> list[Node]:
        result: list[Node] = []
        mark: dict[Node, int] = {}

        def visit(node: Node) -> None:
            mark_of_node = mark.get(node, 0)
            if mark_of_node == 1:  # pragma: no cover
                # Excluding from coverage because it's pretty much impossible to create
                # graphs with cycles with the current graph builder imperative approach.
                # Might change in the future.
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

    def execute(self, workflow: Workflow) -> None:
        id_ = uuid1()
        wf_opts = workflow.options
        sorted_graph = self._topological_sort(workflow)
        state: dict[Proxy, AnyDataset] = {}
        for node in sorted_graph:
            self._execute_node(workflow, node, state, id_.hex, wf_opts)

        for node in workflow.get_nodes():
            opts = node.options
            destroy = Default.get(
                opts.destroy_checkpoints,
                wf_opts.destroy_checkpoints,
                default=self._def_destroy_checkpoints,
            )
            ckpt_fact = Default.get(
                opts.checkpoint_factory,
                wf_opts.checkpoint_factory,
                default=self._def_checkpoint_factory,
            )
            if destroy and ckpt_fact is not None:
                ckpt_fact.destroy(id_.hex, node.name)
