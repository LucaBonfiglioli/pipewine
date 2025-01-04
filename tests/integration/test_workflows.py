import pytest

from pipewine import (
    Dataset,
    Grabber,
    RepeatOp,
    SliceOp,
    TypelessSample,
    UnderfolderSink,
    UnderfolderSource,
)
from pipewine.workflows import Workflow, run_workflow


@pytest.mark.parametrize("grabber", [Grabber(), Grabber(4, 20)])
def test_workflow(underfolder, tmp_path, grabber: Grabber) -> None:
    wf = Workflow()
    input_path = underfolder.folder
    output_path = tmp_path / "output"
    data: Dataset[TypelessSample] = wf.node(UnderfolderSource(folder=input_path))()
    repeat = wf.node(RepeatOp(100))(data)
    slice_ = wf.node(SliceOp(step=2))(repeat)
    wf.node(UnderfolderSink(output_path, grabber=grabber))(slice_)
    run_workflow(wf)
    assert output_path.is_dir()
