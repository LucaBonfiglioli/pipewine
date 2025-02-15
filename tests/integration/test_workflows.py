from contextlib import nullcontext
import pytest

from pipewine import (
    Dataset,
    Grabber,
    RepeatOp,
    SliceOp,
    TypelessSample,
    UnderfolderSink,
    UnderfolderSource,
    DatasetOperator,
)
from pipewine.workflows import Workflow, run_workflow, draw_workflow


class FailingOp(DatasetOperator[Dataset, Dataset]):
    def __call__(self, x: Dataset) -> Dataset:
        raise RuntimeError("Boom")


@pytest.mark.parametrize("grabber", [Grabber(), Grabber(4, 20)])
@pytest.mark.parametrize("fail", [True, False])
def test_workflow(underfolder, tmp_path, grabber: Grabber, fail: bool) -> None:
    wf = Workflow()
    input_path = underfolder.folder
    output_path = tmp_path / "output"
    draw_path = tmp_path / "wf.svg"
    data: Dataset[TypelessSample] = wf.node(UnderfolderSource(folder=input_path))()
    data = wf.node(RepeatOp(100))(data)
    if fail:
        data = wf.node(FailingOp())(data)
    data = wf.node(SliceOp(step=2))(data)
    wf.node(UnderfolderSink(output_path, grabber=grabber))(data)
    cm = pytest.raises(RuntimeError) if fail else nullcontext()
    with cm:
        run_workflow(wf)
    draw_workflow(wf, draw_path)
    assert output_path.is_dir() or fail
    assert draw_path.is_file()
