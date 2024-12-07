from typelime import *
from pathlib import Path

from examples.custom_mapper import SlowMapper
from collections.abc import Callable


def wrapper(fn: Callable[[Workflow], None]):
    workflow = Workflow()
    fn(workflow)
    eq = InMemoryEventQueue()
    executor = NaiveWorkflowExecutor(eq)
    tracker = ProgressGUITracker()
    error: KeyboardInterrupt | Exception | None = None
    try:
        tracker.attach(eq)
        executor.execute(workflow)
    except (KeyboardInterrupt, Exception) as e:
        error = e

    try:
        tracker.detach()
        eq.close()
    except Exception as e:
        if error is not None:
            raise e from error
        else:
            raise e

    if error is not None and not isinstance(error, KeyboardInterrupt):
        raise error


def main():
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")

    data = UnderfolderSource(folder=input_path)()
    repeat = RepeatOp(20)(data)
    slice_ = SliceOp(None, None, 2)(repeat)
    slownode = MapOp(SlowMapper())(slice_)
    UnderfolderSink(output_path, grabber=Grabber(num_workers=4))(slownode)


# if __name__ == "__main__":
#     main()


def main_with_workflow(wf: Workflow):
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")

    data = wf.node(UnderfolderSource(folder=input_path))()
    repeat = wf.node(RepeatOp(1000))(data)
    slice_ = wf.node(SliceOp(None, None, 2))(repeat)
    slownode = wf.node(MapOp(SlowMapper()))(slice_)
    wf.node(UnderfolderSink(output_path, grabber=Grabber(num_workers=10)))(slownode)


if __name__ == "__main__":
    wrapper(main_with_workflow)
