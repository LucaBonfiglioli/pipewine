from typelime import *
from pathlib import Path

from examples.mapper import SlowMapper
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


def main(workflow: Workflow):
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")

    read_node = workflow.node(UnderfolderSource(folder=input_path))

    repeat = workflow.node(RepeatOp(1000))
    workflow.edge(read_node.output, repeat.input)

    slice_ = workflow.node(SliceOp(None, None, 2))
    workflow.edge(repeat.output, slice_.input)

    slownode = workflow.node(MapOp(SlowMapper()))
    workflow.edge(slice_.output, slownode.input)

    write_node = workflow.node(
        UnderfolderSink(output_path, grabber=Grabber(num_workers=10, prefetch=2))
    )
    workflow.edge(slownode.output, write_node.input)


def main_no_wf():
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    output_path = Path("/tmp/out")

    data = UnderfolderSource(folder=input_path)()
    repeat = RepeatOp(20)(data)
    slice_ = SliceOp(None, None, 2)(repeat)
    slownode = MapOp(SlowMapper())(slice_)
    UnderfolderSink(output_path, grabber=Grabber(num_workers=4))(slownode)


if __name__ == "__main__":
    wrapper(main)
