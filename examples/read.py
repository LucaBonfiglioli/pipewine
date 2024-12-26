from pipewine import *
from pathlib import Path

from examples.custom_mapper import SlowMapper
from pipewine.cli.main import _run_cli_workflow


def main():
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")
    data = UnderfolderSource(folder=input_path)()
    repeat = RepeatOp(20)(data)
    slice_ = SliceOp(step=2)(repeat)
    slownode = MapOp(SlowMapper())(slice_)
    UnderfolderSink(output_path, grabber=Grabber(num_workers=4))(slownode)


# if __name__ == "__main__":
#     main()


def main_with_workflow():
    wf = Workflow()
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")
    data = wf.node(UnderfolderSource(folder=input_path))()
    repeat = wf.node(RepeatOp(1000))(data)
    slice_ = wf.node(SliceOp(step=2))(repeat)
    slownode = wf.node(MapOp(SlowMapper()))(slice_)
    wf.node(UnderfolderSink(output_path, grabber=Grabber(num_workers=2)))(slownode)
    _run_cli_workflow(wf)


if __name__ == "__main__":
    main_with_workflow()
