import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from pipewine import (
    Bundle,
    DatasetOperator,
    DatasetSink,
    DatasetSource,
    ListDataset,
    MemoryItem,
    PickleParser,
    TypelessSample,
)
from pipewine.workflows import (
    Event,
    EventQueue,
    SequentialWorkflowExecutor,
    Workflow,
    WfOptions,
)


class MockQueue(EventQueue):
    def __init__(self) -> None:
        super().__init__()
        self._queue: deque[Event] = deque()

    def start(self) -> None:
        pass

    def emit(self, event: Event) -> None:
        self._queue.append(event)

    def capture(self) -> Event | None:
        if not self._queue:
            return None
        return self._queue.popleft()

    def capture_blocking(self, timeout: float | None = None) -> Event | None:
        t = time.perf_counter()
        while True:
            if self._queue:
                return self._queue.popleft()
            if timeout is not None and time.perf_counter() > t + timeout:
                return None
            time.sleep(0.01)

    def close(self) -> None:
        pass


class MyDataset(ListDataset[TypelessSample]):
    def __init__(self, size: int) -> None:
        super().__init__(
            [TypelessSample(number=MemoryItem(i, PickleParser())) for i in range(size)]
        )


class MyBundle(Bundle[MyDataset]):
    a: MyDataset
    b: MyDataset
    c: MyDataset


class MockAction:
    def __init__(self) -> None:
        self.called: list[list[Any]] = []


class Source(DatasetSource[MyDataset], MockAction):
    def __call__(self) -> MyDataset:
        self.called.append([])
        return MyDataset(10)


class Sink(DatasetSink[MyDataset], MockAction):
    def __call__(self, data: MyDataset) -> None:
        for _ in self.loop(data):
            pass
        self.called.append([data])


class Dataset2Dataset(DatasetOperator[MyDataset, MyDataset], MockAction):
    def __call__(self, x: MyDataset) -> MyDataset:
        for _ in self.loop(x):
            pass
        self.called.append([x])
        return x


class Tuple2Tuple(
    DatasetOperator[
        tuple[MyDataset, MyDataset, MyDataset], tuple[MyDataset, MyDataset, MyDataset]
    ],
    MockAction,
):
    def __call__(
        self, x: tuple[MyDataset, MyDataset, MyDataset]
    ) -> tuple[MyDataset, MyDataset, MyDataset]:
        self.called.append([x])
        return x


class List2List(DatasetOperator[Sequence[MyDataset], Sequence[MyDataset]], MockAction):
    def __call__(self, x: Sequence[MyDataset]) -> Sequence[MyDataset]:
        self.called.append([x])
        return x


class Mapping2Mapping(
    DatasetOperator[Mapping[str, MyDataset], Mapping[str, MyDataset]], MockAction
):
    def __call__(self, x: Mapping[str, MyDataset]) -> Mapping[str, MyDataset]:
        self.called.append([x])
        return x


class Bundle2Bundle(DatasetOperator[MyBundle, MyBundle], MockAction):
    def __call__(self, x: MyBundle) -> MyBundle:
        self.called.append([x])
        return x


@dataclass
class WorkflowFixture:
    wf: Workflow


def __workflow__0(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    wf.node(Source())()
    return WorkflowFixture(wf)


def __workflow__1(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data = wf.node(Source())()
    wf.node(Sink())(data)
    return WorkflowFixture(wf)


def __workflow__2(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data = wf.node(Source())()
    data = wf.node(Dataset2Dataset())(data)
    wf.node(Sink())(data)
    return WorkflowFixture(wf)


def __workflow__3(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data1 = wf.node(Source())()
    data2 = wf.node(Source())()
    data3 = wf.node(Source())()
    data = wf.node(Tuple2Tuple())((data1, data2, data3))
    wf.node(Sink())(data[0])
    wf.node(Sink())(data[1])
    wf.node(Sink())(data[2])
    return WorkflowFixture(wf)


def __workflow__4(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data1 = wf.node(Source())()
    data2 = wf.node(Source())()
    data3 = wf.node(Source())()
    data = wf.node(List2List())([data1, data2, data3])
    wf.node(Sink())(data[0])
    wf.node(Sink())(data[1])
    wf.node(Sink())(data[2])
    return WorkflowFixture(wf)


def __workflow__5(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data1 = wf.node(Source())()
    data2 = wf.node(Source())()
    data3 = wf.node(Source())()
    data = wf.node(Mapping2Mapping())({"a": data1, "b": data2, "c": data3})
    wf.node(Sink())(data["a"])
    wf.node(Sink())(data["b"])
    wf.node(Sink())(data["c"])
    return WorkflowFixture(wf)


def __workflow__6(options: WfOptions) -> WorkflowFixture:
    wf = Workflow(options=options)
    data1 = wf.node(Source())()
    data2 = wf.node(Source())()
    data3 = wf.node(Source())()
    data = wf.node(Bundle2Bundle())(MyBundle(a=data1, b=data2, c=data3))
    wf.node(Sink())(data.a)
    wf.node(Sink())(data.b)
    wf.node(Sink())(data.c)
    return WorkflowFixture(wf)


@pytest.fixture(params=[v for k, v in locals().items() if k.startswith("__workflow__")])
def make_wf(request) -> Callable[[WfOptions], WorkflowFixture]:
    return request.param


class TestSequentialWorkflowExecutor:
    def test_attach_detach(self):
        executor = SequentialWorkflowExecutor()
        with pytest.raises(RuntimeError):
            executor.detach()
        executor.attach(MockQueue())
        with pytest.raises(RuntimeError):
            executor.attach(MockQueue())
        executor.detach()

    @pytest.mark.parametrize("queue", [MockQueue(), None])
    @pytest.mark.parametrize(
        "options",
        [
            WfOptions(),
            WfOptions(cache=True),
            WfOptions(cache=False),
            WfOptions(checkpoint=True),
            WfOptions(checkpoint=True, collect_after_checkpoint=True),
            WfOptions(checkpoint=True, collect_after_checkpoint=False),
            WfOptions(checkpoint=True, destroy_checkpoints=False),
            WfOptions(checkpoint=True, destroy_checkpoints=True),
            WfOptions(checkpoint=False),
        ],
    )
    def test_execute(
        self,
        make_wf: Callable[[WfOptions], WorkflowFixture],
        queue: EventQueue | None,
        options: WfOptions,
    ) -> None:
        wf = make_wf(options).wf
        for node in wf.get_nodes():
            assert isinstance(node.action, MockAction)
            assert len(node.action.called) == 0

        executor = SequentialWorkflowExecutor()
        if queue is not None:
            queue.start()
            executor.attach(queue)
        executor.execute(wf)

        for node in wf.get_nodes():
            assert isinstance(node.action, MockAction)
            assert len(node.action.called) == 1

        if queue is not None:
            queue.close()
