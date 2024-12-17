from typelime.workflows.events import Event, EventQueue, InMemoryEventQueue
from typelime.workflows.execution import NaiveWorkflowExecutor, WorkflowExecutor
from typelime.workflows.model import AnyAction, Workflow
from typelime.workflows.tracking import (
    CursesTracker,
    Task,
    TaskCompleteEvent,
    TaskGroup,
    TaskUpdateEvent,
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
    event_queue = event_queue or InMemoryEventQueue()
    executor = executor or NaiveWorkflowExecutor()
    tracker = tracker or NoTracker()
    try:
        executor.attach(event_queue)
        tracker.attach(event_queue)
        executor.execute(workflow)
    finally:
        tracker.detach()
        executor.detach()
        event_queue.close()
