"""Package for Pipewine Workflows and all related components."""

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
    CheckpointFactory,
    Default,
    Edge,
    Node,
    Proxy,
    UnderfolderCheckpointFactory,
    WfOptions,
    Workflow,
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
    """Utility function to easily run a workflow, optionally attaching an event queue
    and a tracker to it.

    Args:
        workflow (Workflow): The workflow to run.
        executor (WorkflowExecutor | None, optional): The workflow executor to use.
            Defaults to None, in which case a SequentialWorkflowExecutor is used.
        event_queue (EventQueue | None, optional): The event queue to use. Defaults to
            None, in which case a ProcessSharedEventQueue is used. The event queue is
            not used if the tracker is not provided.
        tracker (Tracker | None, optional): The tracker to use. Defaults to None, in
            which case no tracker is used.

    Raises:
        Exception: If an exception occurs during the execution of the workflow, cleaning
            up the used resources and re-raising it transparently.
    """
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
    """Utility function to easily draw a workflow.

    Args:
        workflow (Workflow): The workflow to draw.
        path (Path): The path to save the drawing to.
        layout (Layout | None, optional): The layout to use. Defaults to None, in
            which case an OptimizedLayout is used.
        drawer (Drawer | None, optional): The drawer to use. Defaults to None, in
            which case an SVGDrawer is used.
    """
    layout = layout or OptimizedLayout()
    drawer = drawer or SVGDrawer()
    vg = layout.layout(workflow)
    with open(path, "wb") as fp:
        drawer.draw(vg, fp)
