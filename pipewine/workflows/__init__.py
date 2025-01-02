from pipewine.workflows.events import Event, EventQueue, SharedMemoryEventQueue
from pipewine.workflows.execution import NaiveWorkflowExecutor, WorkflowExecutor
from pipewine.workflows.model import AnyAction, Workflow
from pipewine.workflows.tracking import (
    CursesTracker,
    Task,
    TaskCompleteEvent,
    TaskGroup,
    TaskUpdateEvent,
    TaskStartEvent,
    Tracker,
    TrackingEvent,
    NoTracker,
)


def run_workflow(
    workflow: Workflow,
    event_queue: EventQueue | None = None,
    executor: WorkflowExecutor | None = None,
    tracker: Tracker | None = None,
) -> None:
    event_queue = event_queue or SharedMemoryEventQueue()
    executor = executor or NaiveWorkflowExecutor()
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
