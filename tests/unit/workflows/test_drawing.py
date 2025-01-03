from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir

import pytest

from pipewine import (
    CatOp,
    Dataset,
    RepeatOp,
    SliceOp,
    TypelessSample,
    UnderfolderSink,
    UnderfolderSource,
)
from pipewine.workflows import (
    Drawer,
    Edge,
    Layout,
    OptimizedLayout,
    SVGDrawer,
    Workflow,
)


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


@pytest.mark.parametrize("layout", [OptimizedLayout()])
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
