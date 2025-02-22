"""Progress tracking utilities for workflows."""

import curses
import time
from abc import ABC, abstractmethod
from collections import OrderedDict, deque
from curses import window, wrapper
from dataclasses import dataclass, field
import threading

from pipewine.workflows.events import Event, EventQueue


@dataclass
class TrackingEvent(Event):
    """Base class for tracking events."""

    task_id: str
    """The unique identifier of the task."""


@dataclass
class TaskStartEvent(TrackingEvent):
    """Event that signals the start of a task."""

    total: int
    """The total number of units in the task."""


@dataclass
class TaskUpdateEvent(TrackingEvent):
    """Event that signals the update of a task."""

    unit: int
    """The index of the unit that was updated."""


@dataclass
class TaskCompleteEvent(TrackingEvent):
    """Event that signals the completion of a task."""

    pass


class Tracker(ABC):
    """Base class for tracking the progress of a workflow.

    Subclasses should implement the `attach` and `detach` methods.
    """

    @abstractmethod
    def attach(self, event_queue: EventQueue) -> None:
        """Attach the tracker to an event queue.

        Args:
            event_queue (EventQueue): The event queue to attach to.
        """
        pass

    @abstractmethod
    def detach(self, graceful: bool = True) -> None:
        """Detach the tracker from the event queue.

        Args:
            graceful (bool, optional): Whether to wait for the event queue to be empty
                before detaching. Defaults to True.
        """
        pass


@dataclass
class Task:
    """Data container that represents the state of a task."""

    name: str
    """The name of the task."""
    units: list[bool]
    """A list of booleans that represent the completion state of each unit."""
    complete: bool = False
    """Whether the task is complete."""


@dataclass
class TaskGroup:
    """Data container that represents a group of tasks."""

    name: str
    """The name of the group."""
    groups: OrderedDict[str, "TaskGroup"] = field(default_factory=OrderedDict)
    """A dictionary of subgroups."""
    tasks: OrderedDict[str, Task] = field(default_factory=OrderedDict)
    """A dictionary of tasks."""


class CursesTracker(Tracker):
    """A tracker that uses curses to display the progress of a workflow in a terminal."""

    MAX_COLOR = 1000

    def __init__(self, refresh_rate: float = 0.1) -> None:
        """
        Args:
            refresh_rate (float, optional): The refresh rate of the display in seconds.
                Defaults to 0.1.
        """
        super().__init__()
        self._refresh_rate = refresh_rate
        self._n_shades = 10

        self._eq: EventQueue | None = None
        self._tui_thread: threading.Thread | None = None
        self._read_thread: threading.Thread | None = None
        self._buffer: deque[Event] = deque()
        self._stop_flag_forced = threading.Event()
        self._stop_flag_graceful = threading.Event()

    def attach(self, event_queue: EventQueue) -> None:
        if self._eq is not None or self._tui_thread is not None:
            raise RuntimeError("Already attached to another event queue.")
        self._eq = event_queue
        self._tui_thread = threading.Thread(target=self._tui_loop)
        self._read_thread = threading.Thread(target=self._read_loop)
        self._stop_flag_forced.clear()
        self._stop_flag_graceful.clear()
        self._tui_thread.start()
        self._read_thread.start()

    def detach(self, graceful: bool = True) -> None:
        if self._eq is None or self._tui_thread is None:
            raise RuntimeError("Not attached to any event queue.")
        assert self._read_thread is not None
        if graceful:
            self._stop_flag_graceful.set()
        else:
            self._stop_flag_forced.set()
        self._read_thread.join()
        self._tui_thread.join()
        self._read_thread = None
        self._tui_thread = None
        self._eq = None

    def _get_group(self, group: TaskGroup, path: str) -> TaskGroup:
        path_chunks = path.split("/")
        for p in path_chunks:
            if p not in group.groups:
                group.groups[p] = TaskGroup(p)
            group = group.groups[p]
        return group

    def _spawn_task(self, group: TaskGroup, path: str, total: int) -> Task:
        group_path, _, task_name = path.rpartition("/")
        units = [False for _ in range(total)]
        task = Task(task_name, units)
        group = self._get_group(group, group_path)
        group.tasks[task_name] = task
        return task

    def _get_task(self, group: TaskGroup, path: str) -> Task:
        group_path, _, task_name = path.rpartition("/")
        group = self._get_group(group, group_path)
        return group.tasks[task_name]

    def _preorder(
        self, group: TaskGroup, depth: int
    ) -> list[tuple[int, Task | TaskGroup]]:
        next = depth + 1
        result: list[tuple[int, Task | TaskGroup]] = [(depth, group)]
        for tg in group.groups.values():
            result.extend(self._preorder(tg, depth=next))
        for task in group.tasks.values():
            result.append((next, task))
        return result

    def _compute_bar(self, units: list[bool], width: int) -> list[list[int]]:
        if len(units) < width:
            bar = []
            div = width // len(units)
            rem = width % len(units)
            for ui, unit in enumerate(units):
                bar.append([div + int(ui < rem), int(unit), 1])
        else:
            bar = [[1, 0, 0] for _ in range(width)]
            for ui, unit in enumerate(units):
                cell = bar[int(ui / len(units) * len(bar))]
                cell[1] += int(unit)
                cell[2] += 1
        return bar

    def _init_colors(self) -> None:  # pragma: no cover
        curses.start_color()
        curses.use_default_colors()
        if curses.can_change_color():
            for i in range(self._n_shades):
                c = int((float(i + 1) / self._n_shades) * self.MAX_COLOR)
                curses.init_color(i + 1, c, c, c)
                curses.init_pair(i + 1, i + 1, -1)
        else:
            self._n_shades = 5
            curses.init_pair(1, curses.COLOR_BLACK, -1)
            curses.init_pair(2, curses.COLOR_RED, -1)
            curses.init_pair(3, curses.COLOR_YELLOW, -1)
            curses.init_pair(4, curses.COLOR_GREEN, -1)
            curses.init_pair(5, curses.COLOR_WHITE, -1)

    def _color_of(self, frac: float) -> int:
        return curses.color_pair(1 + int(frac * (self._n_shades - 1)))

    def _render_tasks(
        self,
        screen: window,
        tasks: list[tuple[int, Task | TaskGroup]],
        global_step: int,
    ) -> None:
        screen.clear()
        bar_elem = "██"
        H, W = screen.getmaxyx()
        TITLE_H, TITLE_W = len(tasks), min(20, W - 1)
        PROG_H = TITLE_H
        PROG_W = W - TITLE_W
        padding = 0
        for _, entry in tasks:
            if isinstance(entry, Task):
                padding_i = len(str(len(entry.units))) * 2 + 1 + 11
                padding = max(padding, padding_i)
        bar_w = (PROG_W - padding) // len(bar_elem)
        title_pad = curses.newpad(TITLE_H, TITLE_W)
        prog_pad = curses.newpad(PROG_H, PROG_W)
        for i, (depth, entry) in enumerate(tasks):
            space = TITLE_W - 2 * depth - 1
            text = entry.name
            if len(text) > space:  # pragma: no cover
                start = (global_step // 2) % (len(text) - space)
                text = text[start : start + space]
            title_pad.addstr(i, 2 * depth, text)
            if isinstance(entry, Task):
                j = 0
                if entry.complete:
                    color = self._color_of(1.0)
                    if bar_w > 0:  # pragma: no cover
                        prog_pad.addstr(i, 0, bar_elem * bar_w, color)
                        j += len(bar_elem) * bar_w
                    sum_ = len(entry.units)
                else:
                    if bar_w > 0:  # pragma: no cover
                        for size, comp, total in self._compute_bar(entry.units, bar_w):
                            color = self._color_of(comp / total)
                            prog_pad.addstr(i, j, bar_elem * size, color)
                            j += size * len(bar_elem)
                    sum_ = sum(entry.units)
                total = len(entry.units)
                perc = round((sum_ / total) * 100, 2)
                text = f"{sum_}/{total} {perc}%"
                if len(text) + 2 < PROG_W:  # pragma: no cover
                    prog_pad.addstr(i, j + 2, text)

        title_pad.noutrefresh(max(0, TITLE_H - H), 0, 0, 0, H - 1, W - 1)
        prog_pad.noutrefresh(max(0, TITLE_H - H), 0, 0, TITLE_W, H - 1, W - 1)
        curses.doupdate()

    def _curses(self, stdscr: window) -> None:
        self._init_colors()
        root = TaskGroup("__root__")
        global_step = -1
        while not self._stop_flag_forced.is_set():
            time.sleep(self._refresh_rate)
            global_step = global_step + 1 % 10000
            while True:
                try:
                    event = self._buffer.popleft()
                except:
                    break

                if isinstance(event, TaskStartEvent):
                    task = self._spawn_task(root, event.task_id, event.total)
                elif isinstance(event, TaskUpdateEvent):
                    task = self._get_task(root, event.task_id)
                    task.units[event.unit] = True
                elif isinstance(event, TaskCompleteEvent):
                    task = self._get_task(root, event.task_id)
                    task.complete = True

            list_of_tasks = self._preorder(root, -1)[1:]
            if len(list_of_tasks) > 0:
                self._render_tasks(stdscr, list_of_tasks, global_step)

            if (
                self._stop_flag_graceful.is_set()
                and not self._buffer
                and self._read_thread
                and not self._read_thread.is_alive()
            ):
                break

    def _read_loop(self) -> None:
        eq = self._eq
        assert eq is not None
        while not self._stop_flag_forced.is_set():
            while (event := eq.capture_blocking(timeout=0.1)) is not None:
                self._buffer.append(event)
            if event is None and self._stop_flag_graceful.is_set():
                break

    def _tui_loop(self) -> None:
        wrapper(self._curses)
