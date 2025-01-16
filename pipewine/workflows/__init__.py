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
    NodeOptions,
)
from pipewine.workflows.tracking import (
    CursesTracker,
    NoTracker,
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
    event_queue: EventQueue | None = None,
    executor: WorkflowExecutor | None = None,
    tracker: Tracker | None = None,
) -> None:
    event_queue = event_queue or ProcessSharedEventQueue()
    executor = executor or SequentialWorkflowExecutor()
    tracker = tracker or NoTracker()
    try:
        event_queue.start()
        executor.attach(event_queue)
        tracker.attach(event_queue)
        executor.execute(workflow)
    finally:
        tracker.detach()
        executor.detach()
        event_queue.close()
