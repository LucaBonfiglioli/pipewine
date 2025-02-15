from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from pipewine import (
    Bundle,
    Dataset,
    DatasetOperator,
    DatasetSink,
    DatasetSource,
    Grabber,
    TypelessSample,
    UnderfolderSink,
    UnderfolderSource,
)
from pipewine.workflows import (
    Edge,
    Node,
    Proxy,
    UnderfolderCheckpointFactory,
    Workflow,
    Default,
    WfOptions,
)
from pipewine.workflows.model import All, _ProxyMapping, _ProxySequence


class TestUnderfolderCheckpointFactory:
    def test_create(self, tmp_path: Path) -> None:
        ckpt_fact = UnderfolderCheckpointFactory(folder=tmp_path)
        sink, source = ckpt_fact.create("test", "uf_0", TypelessSample, Grabber())
        assert isinstance(sink, UnderfolderSink)
        assert isinstance(source, UnderfolderSource)

    def test_destroy(self, tmp_path: Path) -> None:
        ckpt_fact = UnderfolderCheckpointFactory(folder=tmp_path)
        ex_id = "test"
        name = "uf_0"
        expected_folder = tmp_path / ex_id / name
        assert not expected_folder.exists()
        ckpt_fact.destroy(ex_id, name)
        assert not expected_folder.exists()
        expected_folder.mkdir(parents=True)
        ckpt_fact.destroy(ex_id, name)
        assert not expected_folder.exists()


class TestDefault:
    @pytest.mark.parametrize(
        ["optionals", "default", "expected"],
        [
            [[], 10, 10],
            [[20], 10, 20],
            [[Default(), 20], 10, 20],
            [[20, Default()], 10, 20],
            [[Default()], 10, 10],
            [[Default(), Default()], 10, 10],
        ],
    )
    def test_get(self, optionals: list[Any], default: Any, expected: Any) -> None:
        assert Default.get(*optionals, default=default) == expected


class TestProxyMapping:
    def test_dict(self) -> None:
        pmap: _ProxyMapping[str] = _ProxyMapping(lambda k: k.upper())
        assert dict(pmap._data) == {}
        assert len(pmap._data) == 0
        pmap["a"]
        pmap["b"]
        pmap["c"]
        pmap["d"]
        assert dict(pmap._data) == {"a": "A", "b": "B", "c": "C", "d": "D"}
        assert len(pmap._data) == 4
        assert pmap["b"] == "B"
        with pytest.raises(RuntimeError):
            len(pmap)
        with pytest.raises(RuntimeError):
            dict(pmap)


class TestProxySequence:
    def test_list(self) -> None:
        pseq: _ProxySequence[str] = _ProxySequence(lambda x: f"n{x}")
        assert list(pseq._data) == []
        assert len(pseq._data) == 0
        pseq[4]
        pseq[5]
        pseq[2]
        assert list(pseq._data) == ["n0", "n1", "n2", "n3", "n4", "n5"]
        assert len(pseq._data) == 6
        with pytest.raises(RuntimeError):
            len(pseq)
        with pytest.raises(RuntimeError):
            list(pseq)
        with pytest.raises(RuntimeError):
            pseq[:10]


class MyBundle(Bundle[Dataset]):
    a: Dataset
    b: Dataset
    c: Dataset


class Source(DatasetSource[Dataset]):
    def __call__(self) -> Dataset:
        raise NotImplementedError()


class Sink(DatasetSink[Dataset]):
    def __call__(self, data: Dataset) -> None:
        raise NotImplementedError()


class Dataset2Dataset(DatasetOperator[Dataset, Dataset]):
    def __call__(self, x: Dataset) -> Dataset:
        raise NotImplementedError()


class Dataset2Tuple(DatasetOperator[Dataset, tuple[Dataset, Dataset, Dataset]]):
    def __call__(self, x: Dataset) -> tuple[Dataset, Dataset, Dataset]:
        raise NotImplementedError()


class Dataset2List(DatasetOperator[Dataset, Sequence[Dataset]]):
    def __call__(self, x: Dataset) -> Sequence[Dataset]:
        raise NotImplementedError()


class Dataset2Mapping(DatasetOperator[Dataset, Mapping[str, Dataset]]):
    def __call__(self, x: Dataset) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class Dataset2Bundle(DatasetOperator[Dataset, MyBundle]):
    def __call__(self, x: Dataset) -> MyBundle:
        raise NotImplementedError()


class Tuple2Dataset(DatasetOperator[tuple[Dataset, Dataset, Dataset], Dataset]):
    def __call__(self, x: tuple[Dataset, Dataset, Dataset]) -> Dataset:
        raise NotImplementedError()


class Tuple2Tuple(
    DatasetOperator[tuple[Dataset, Dataset, Dataset], tuple[Dataset, Dataset, Dataset]]
):
    def __call__(
        self, x: tuple[Dataset, Dataset, Dataset]
    ) -> tuple[Dataset, Dataset, Dataset]:
        raise NotImplementedError()


class Tuple2List(DatasetOperator[tuple[Dataset, Dataset, Dataset], Sequence[Dataset]]):
    def __call__(self, x: tuple[Dataset, Dataset, Dataset]) -> Sequence[Dataset]:
        raise NotImplementedError()


class Tuple2Mapping(
    DatasetOperator[tuple[Dataset, Dataset, Dataset], Mapping[str, Dataset]]
):
    def __call__(self, x: tuple[Dataset, Dataset, Dataset]) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class Tuple2Bundle(DatasetOperator[tuple[Dataset, Dataset, Dataset], MyBundle]):
    def __call__(self, x: tuple[Dataset, Dataset, Dataset]) -> MyBundle:
        raise NotImplementedError()


class List2Dataset(DatasetOperator[Sequence[Dataset], Dataset]):
    def __call__(self, x: Sequence[Dataset]) -> Dataset:
        raise NotImplementedError()


class List2Tuple(DatasetOperator[Sequence[Dataset], tuple[Dataset, Dataset, Dataset]]):
    def __call__(self, x: Sequence[Dataset]) -> tuple[Dataset, Dataset, Dataset]:
        raise NotImplementedError()


class List2List(DatasetOperator[Sequence[Dataset], Sequence[Dataset]]):
    def __call__(self, x: Sequence[Dataset]) -> Sequence[Dataset]:
        raise NotImplementedError()


class List2Mapping(DatasetOperator[Sequence[Dataset], Mapping[str, Dataset]]):
    def __call__(self, x: Sequence[Dataset]) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class List2Bundle(DatasetOperator[Sequence[Dataset], MyBundle]):
    def __call__(self, x: Sequence[Dataset]) -> MyBundle:
        raise NotImplementedError()


class Mapping2Dataset(DatasetOperator[Mapping[str, Dataset], Dataset]):
    def __call__(self, x: Mapping[str, Dataset]) -> Dataset:
        raise NotImplementedError()


class Mapping2Tuple(
    DatasetOperator[Mapping[str, Dataset], tuple[Dataset, Dataset, Dataset]]
):
    def __call__(self, x: Mapping[str, Dataset]) -> tuple[Dataset, Dataset, Dataset]:
        raise NotImplementedError()


class Mapping2List(DatasetOperator[Mapping[str, Dataset], Sequence[Dataset]]):
    def __call__(self, x: Mapping[str, Dataset]) -> Sequence[Dataset]:
        raise NotImplementedError()


class Mapping2Mapping(DatasetOperator[Mapping[str, Dataset], Mapping[str, Dataset]]):
    def __call__(self, x: Mapping[str, Dataset]) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class Mapping2Bundle(DatasetOperator[Mapping[str, Dataset], MyBundle]):
    def __call__(self, x: Mapping[str, Dataset]) -> MyBundle:
        raise NotImplementedError()


class Bundle2Dataset(DatasetOperator[MyBundle, Dataset]):
    def __call__(self, x: MyBundle) -> Dataset:
        raise NotImplementedError()


class Bundle2Tuple(DatasetOperator[MyBundle, tuple[Dataset, Dataset, Dataset]]):
    def __call__(self, x: MyBundle) -> tuple[Dataset, Dataset, Dataset]:
        raise NotImplementedError()


class Bundle2List(DatasetOperator[MyBundle, Sequence[Dataset]]):
    def __call__(self, x: MyBundle) -> Sequence[Dataset]:
        raise NotImplementedError()


class Bundle2Mapping(DatasetOperator[MyBundle, Mapping[str, Dataset]]):
    def __call__(self, x: MyBundle) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class Bundle2Bundle(DatasetOperator[MyBundle, MyBundle]):
    def __call__(self, x: MyBundle) -> MyBundle:
        raise NotImplementedError()


@dataclass
class WorkflowFixture:
    wf: Workflow
    nodes: set[Node]
    edges: set[Edge]


def __workflow__1() -> WorkflowFixture:
    wf = Workflow()
    source, sink = Source(), Sink()
    data = wf.node(source, name="source")()
    wf.node(sink, name="sink")(data)

    source_node = Node("source", source)
    sink_node = Node("sink", sink)
    nodes: set[Node] = {source_node, sink_node}
    edges: set[Edge] = {Edge(Proxy(source_node, None), Proxy(sink_node, None))}
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__2() -> WorkflowFixture:
    wf = Workflow()
    source, op, sink = Source(), Dataset2Dataset(), Sink()
    data = wf.node(source, name="source")()
    data = wf.node(op, name="op")(data)
    wf.node(sink, name="sink")(data)

    source_node = Node("source", source)
    op_node = Node("op", op)
    sink_node = Node("sink", sink)
    nodes: set[Node] = {source_node, op_node, sink_node}
    edges: set[Edge] = {
        Edge(Proxy(source_node, None), Proxy(op_node, None)),
        Edge(Proxy(op_node, None), Proxy(sink_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__3() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1, source_2 = Source(), Source(), Source()
    sink_0, sink_1, sink_2 = Sink(), Sink(), Sink()
    op = Tuple2Tuple()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data_2 = wf.node(source_2, name="source_2")()
    data = wf.node(op, name="op")((data_0, data_1, data_2))
    wf.node(sink_0, name="sink_0")(data[0])
    wf.node(sink_1, name="sink_1")(data[1])
    wf.node(sink_2, name="sink_2")(data[2])

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    source_2_node = Node("source_2", source_2)
    op_node = Node("op", op)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    sink_2_node = Node("sink_2", sink_2)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        source_2_node,
        op_node,
        sink_0_node,
        sink_1_node,
        sink_2_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op_node, 0)),
        Edge(Proxy(source_1_node, None), Proxy(op_node, 1)),
        Edge(Proxy(source_2_node, None), Proxy(op_node, 2)),
        Edge(Proxy(op_node, 0), Proxy(sink_0_node, None)),
        Edge(Proxy(op_node, 1), Proxy(sink_1_node, None)),
        Edge(Proxy(op_node, 2), Proxy(sink_2_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__4() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1 = Source(), Source()
    sink_0, sink_1 = Sink(), Sink()
    op = List2List()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data = wf.node(op, name="op")((data_0, data_1))
    wf.node(sink_0, name="sink_0")(data[0])
    wf.node(sink_1, name="sink_1")(data[1])

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    op_node = Node("op", op)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        op_node,
        sink_0_node,
        sink_1_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op_node, 0)),
        Edge(Proxy(source_1_node, None), Proxy(op_node, 1)),
        Edge(Proxy(op_node, 0), Proxy(sink_0_node, None)),
        Edge(Proxy(op_node, 1), Proxy(sink_1_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__5() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1 = Source(), Source()
    sink_0, sink_1 = Sink(), Sink()
    op = Mapping2Mapping()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data = wf.node(op, name="op")({"alice": data_0, "bob": data_1})
    wf.node(sink_0, name="sink_0")(data["alice"])
    wf.node(sink_1, name="sink_1")(data["bob"])

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    op_node = Node("op", op)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        op_node,
        sink_0_node,
        sink_1_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op_node, "alice")),
        Edge(Proxy(source_1_node, None), Proxy(op_node, "bob")),
        Edge(Proxy(op_node, "alice"), Proxy(sink_0_node, None)),
        Edge(Proxy(op_node, "bob"), Proxy(sink_1_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__6() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1, source_2 = Source(), Source(), Source()
    sink_0, sink_1, sink_2 = Sink(), Sink(), Sink()
    op = Bundle2Bundle()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data_2 = wf.node(source_2, name="source_2")()
    data = wf.node(op, name="op")(MyBundle(a=data_0, b=data_1, c=data_2))
    wf.node(sink_0, name="sink_0")(data.a)
    wf.node(sink_1, name="sink_1")(data.b)
    wf.node(sink_2, name="sink_2")(data.c)

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    source_2_node = Node("source_2", source_2)
    op_node = Node("op", op)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    sink_2_node = Node("sink_2", sink_2)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        source_2_node,
        op_node,
        sink_0_node,
        sink_1_node,
        sink_2_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op_node, "a")),
        Edge(Proxy(source_1_node, None), Proxy(op_node, "b")),
        Edge(Proxy(source_2_node, None), Proxy(op_node, "c")),
        Edge(Proxy(op_node, "a"), Proxy(sink_0_node, None)),
        Edge(Proxy(op_node, "b"), Proxy(sink_1_node, None)),
        Edge(Proxy(op_node, "c"), Proxy(sink_2_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__7() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1 = Source(), Source()
    sink_0, sink_1 = Sink(), Sink()
    op1 = List2List()
    op2 = List2List()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data = wf.node(op1, name="op1")((data_0, data_1))
    data = wf.node(op2, name="op2")(data)
    wf.node(sink_0, name="sink_0")(data[0])
    wf.node(sink_1, name="sink_1")(data[1])

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    op1_node = Node("op1", op1)
    op2_node = Node("op2", op2)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        op1_node,
        op2_node,
        sink_0_node,
        sink_1_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op1_node, 0)),
        Edge(Proxy(source_1_node, None), Proxy(op1_node, 1)),
        Edge(Proxy(op1_node, All()), Proxy(op2_node, All())),
        Edge(Proxy(op2_node, 0), Proxy(sink_0_node, None)),
        Edge(Proxy(op2_node, 1), Proxy(sink_1_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


def __workflow__8() -> WorkflowFixture:
    wf = Workflow()
    source_0, source_1 = Source(), Source()
    sink_0, sink_1 = Sink(), Sink()
    op1 = Mapping2Mapping()
    op2 = Mapping2Mapping()
    data_0 = wf.node(source_0, name="source_0")()
    data_1 = wf.node(source_1, name="source_1")()
    data = wf.node(op1, name="op1")({"0": data_0, "1": data_1})
    data = wf.node(op2, name="op2")(data)
    wf.node(sink_0, name="sink_0")(data["0"])
    wf.node(sink_1, name="sink_1")(data["1"])

    source_0_node = Node("source_0", source_0)
    source_1_node = Node("source_1", source_1)
    op1_node = Node("op1", op1)
    op2_node = Node("op2", op2)
    sink_0_node = Node("sink_0", sink_0)
    sink_1_node = Node("sink_1", sink_1)
    nodes: set[Node] = {
        source_0_node,
        source_1_node,
        op1_node,
        op2_node,
        sink_0_node,
        sink_1_node,
    }
    edges: set[Edge] = {
        Edge(Proxy(source_0_node, None), Proxy(op1_node, "0")),
        Edge(Proxy(source_1_node, None), Proxy(op1_node, "1")),
        Edge(Proxy(op1_node, All()), Proxy(op2_node, All())),
        Edge(Proxy(op2_node, "0"), Proxy(sink_0_node, None)),
        Edge(Proxy(op2_node, "1"), Proxy(sink_1_node, None)),
    }
    return WorkflowFixture(wf=wf, nodes=nodes, edges=edges)


@pytest.fixture(params=[v for k, v in locals().items() if k.startswith("__workflow__")])
def make_wf(request) -> Callable[[], WorkflowFixture]:
    return request.param


class TestWorkflow:
    def test_build_workflow(self, make_wf: Callable[[], WorkflowFixture]) -> None:
        wf_fixture = make_wf()
        wf, exp_nodes, exp_edges = wf_fixture.wf, wf_fixture.nodes, wf_fixture.edges
        assert wf.get_nodes() == exp_nodes
        edges: set[Edge] = set()
        for node in exp_nodes:
            ob_edges = wf.get_outbound_edges(node)
            edges.update(ob_edges)
            for edge in ob_edges:
                assert edge.src.node == node
        edges.clear()
        for node in exp_nodes:
            ib_edges = wf.get_inbound_edges(node)
            edges.update(ib_edges)
            for edge in ib_edges:
                assert edge.dst.node == node
        for node in exp_nodes:
            assert wf.get_node(node.name) == node
        assert edges == exp_edges

    def test_workflow_without_names(self) -> None:
        wf = Workflow()
        action = Source()
        wf.node(action)
        assert next(iter(wf.get_nodes())).action == action

    def test_workflow_options(self) -> None:
        wf = Workflow(options=WfOptions(cache=True))
        assert wf.options.cache

    def test_workflow_fail_node_not_found(
        self, make_wf: Callable[[], WorkflowFixture]
    ) -> None:
        wf = make_wf().wf
        with pytest.raises(ValueError):
            wf.get_inbound_edges(Node("__WRONG__", Source()))
        with pytest.raises(ValueError):
            wf.get_outbound_edges(Node("__WRONG__", Source()))

    def test_workflow_fail_duplicate_node(self) -> None:
        wf = Workflow()
        wf.node(Source(), name="name")
        with pytest.raises(ValueError):
            wf.node(Source(), name="name")
