from pathlib import Path
from pipewine.workflows.drawing import (
    Drawer,
    Layout,
    OptimizedLayout,
    SVGDrawer,
    ViewEdge,
    ViewGraph,
    ViewNode,
)
from pipewine.workflows.events import Event, EventQueue, ProcessSharedEventQueue
from pipewine.workflows.execution import SequentialWorkflowExecutor, WorkflowExecutor
from pipewine.workflows.model import (
    AnyAction,
    Workflow,
    Node,
    Edge,
    Proxy,
    CheckpointFactory,
    UnderfolderCheckpointFactory,
    WfOptions,
    Default,
)
from pipewine.workflows.tracking import (
    CursesTracker,
    Task,
    TaskCompleteEvent,
    TaskGroup,
    TaskStartEvent,
    TaskUpdateEvent,
    Tracker,
    TrackingEvent,
)


def run_workflow(
    workflow: Workflow,
    executor: WorkflowExecutor | None = None,
    event_queue: EventQueue | None = None,
    tracker: Tracker | None = None,
) -> None:
    executor = executor or SequentialWorkflowExecutor()
    event_queue = event_queue or ProcessSharedEventQueue()
    success = True
    try:
        if event_queue and tracker:
            event_queue.start()
            executor.attach(event_queue)
            tracker.attach(event_queue)
        executor.execute(workflow)
    except BaseException as e:
        success = False
        raise e
    finally:
        if event_queue and tracker:
            executor.detach()
            tracker.detach(graceful=success)
            event_queue.close()


def draw_workflow(
    workflow: Workflow,
    path: Path,
    layout: Layout | None = None,
    drawer: Drawer | None = None,
) -> None:
    layout = layout or OptimizedLayout()
    drawer = drawer or SVGDrawer()
    vg = layout.layout(workflow)
    with open(path, "wb") as fp:
        drawer.draw(vg, fp)
