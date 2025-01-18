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
    try:
        if event_queue and tracker:
            event_queue.start()
            executor.attach(event_queue)
            tracker.attach(event_queue)
        executor.execute(workflow)
    finally:
        if event_queue and tracker:
            tracker.detach()
            executor.detach()
            event_queue.close()
