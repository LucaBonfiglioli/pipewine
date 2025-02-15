from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir
from collections.abc import Mapping, Sequence
import pytest

from pipewine import (
    CatOp,
    Dataset,
    RepeatOp,
    SliceOp,
    TypelessSample,
    UnderfolderSink,
    UnderfolderSource,
    DatasetOperator,
    DatasetSource,
    DatasetSink,
)
from pipewine.workflows import (
    Drawer,
    Edge,
    Layout,
    OptimizedLayout,
    SVGDrawer,
    Workflow,
)


class Source(DatasetSource[Dataset]):
    def __call__(self) -> Dataset:
        raise NotImplementedError()


class Sink(DatasetSink[Dataset]):
    def __call__(self, data: Dataset) -> None:
        raise NotImplementedError()


class Mapping2Mapping(DatasetOperator[Mapping[str, Dataset], Mapping[str, Dataset]]):
    def __call__(self, x: Mapping[str, Dataset]) -> Mapping[str, Dataset]:
        raise NotImplementedError()


class List2List(DatasetOperator[Sequence[Dataset], Sequence[Dataset]]):
    def __call__(self, x: Sequence[Dataset]) -> Sequence[Dataset]:
        raise NotImplementedError()


@dataclass
class WorkflowFixture:
    name: str
    wf: Workflow


def __workflow__0() -> WorkflowFixture:
    wf = Workflow()
    wf.node(UnderfolderSource(folder=Path("i1")))()
    return WorkflowFixture("00000", wf)


def __workflow__1() -> WorkflowFixture:
    wf = Workflow()
    data: Dataset[TypelessSample] = wf.node(UnderfolderSource(folder=Path("i1")))()
    wf.node(UnderfolderSink(Path("o3")))(data)
    return WorkflowFixture("00001", wf)


def __workflow__2() -> WorkflowFixture:
    wf = Workflow()
    data1: Dataset[TypelessSample] = wf.node(UnderfolderSource(folder=Path("i1")))()
    data2: Dataset[TypelessSample] = wf.node(UnderfolderSource(folder=Path("i2")))()
    repeat = wf.node(RepeatOp(1000))(data1)
    slice_ = wf.node(SliceOp(step=2))(repeat)
    wf.node(UnderfolderSink(Path("o1")))(slice_)
    cat = wf.node(CatOp())([data1, data2, repeat])
    wf.node(UnderfolderSink(Path("o2")))(cat)
    cat2 = wf.node(CatOp())([cat, slice_, data2])
    wf.node(UnderfolderSink(Path("o3")))(cat2)
    return WorkflowFixture("00002", wf)


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
    return WorkflowFixture("00003", wf=wf)


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
    return WorkflowFixture("00004", wf=wf)


@pytest.fixture(params=[v for k, v in locals().items() if k.startswith("__workflow__")])
def make_wf(request) -> Callable[[], WorkflowFixture]:
    return request.param


class TestOptimizedLayout:
    def test_layout(self, make_wf: Callable[[], WorkflowFixture]) -> None:
        layout = OptimizedLayout(optimize_steps=50)
        wf = make_wf().wf
        vg = layout.layout(wf)
        all_edges: set[Edge] = set()
        for node in wf.get_nodes():
            all_edges.update(wf.get_inbound_edges(node))
        assert len(vg.nodes) == len(wf.get_nodes())
        assert len(vg.edges) == len(all_edges)


@pytest.mark.parametrize(
    "layout",
    [
        OptimizedLayout(optimize_time_budget=0.5),
        OptimizedLayout(optimize_time_budget=0.5, verbose=True),
    ],
)
@pytest.mark.parametrize(["drawer", "ext"], [[SVGDrawer(), ".svg"]])
def test_drawing_integration(
    make_wf: Callable[[], WorkflowFixture], layout: Layout, drawer: Drawer, ext: str
) -> None:
    wf_data = make_wf()
    file = (
        Path(gettempdir())
        / "pipewine-drawings"
        / f"{wf_data.name}_{type(layout).__name__}_{type(drawer).__name__}{ext}"
    )
    file.parent.mkdir(parents=True, exist_ok=True)

    with open(file, "wb") as fp:
        drawer.draw(layout.layout(wf_data.wf), fp)
