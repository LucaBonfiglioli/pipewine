import curses
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from curses import window, wrapper
from dataclasses import dataclass, field
from threading import Thread

from typelime.workflows.events import Event, EventQueue


@dataclass
class TrackingEvent(Event):
    task_id: str


@dataclass
class TaskUpdateEvent(TrackingEvent):
    unit: int
    total: int


@dataclass
class TaskCompleteEvent(TrackingEvent):
    pass


class Tracker(ABC):
    @abstractmethod
    def attach(self, event_queue: EventQueue) -> None: ...

    @abstractmethod
    def detach(self) -> None: ...


@dataclass
class Task:
    name: str
    units: list[bool]
    complete: bool = False


@dataclass
class TaskGroup:
    name: str
    groups: OrderedDict[str, "TaskGroup"] = field(default_factory=OrderedDict)
    tasks: OrderedDict[str, Task] = field(default_factory=OrderedDict)


class NoTracker(Tracker):
    def attach(self, event_queue: EventQueue) -> None:
        return

    def detach(self) -> None:
        return


class CursesTracker(Tracker):
    def __init__(self, refresh_rate: float = 0.1) -> None:
        super().__init__()
        self._refresh_rate = refresh_rate

        self._eq: EventQueue | None = None
        self._thread: Thread | None = None
        self._stop_flag = False

    def attach(self, event_queue: EventQueue) -> None:
        if self._eq is not None or self._thread is not None:
            raise RuntimeError("Already attached to another event queue.")
        self._eq = event_queue
        self._thread = Thread(target=self._loop)
        self._stop_flag = False
        self._thread.start()

    def detach(self) -> None:
        if self._eq is None or self._thread is None:
            raise RuntimeError("Not attached to any event queue.")
        self._stop_flag = True
        self._thread.join()
        self._thread = None
        self._eq = None

    def _get_task(self, group: TaskGroup, path: str, total: int) -> Task:
        path_chunks = path.split("/")
        for p in path_chunks[:-1]:
            if p not in group.groups:
                group.groups[p] = TaskGroup(p)
            group = group.groups[p]
        task_name = path_chunks[-1]
        if task_name not in group.tasks:
            units = [False for _ in range(total)]
            group.tasks[task_name] = Task(task_name, units)
        return group.tasks[task_name]

    def _preorder(
        self, group: TaskGroup, depth: int
    ) -> list[tuple[int, Task | TaskGroup]]:
        next = depth + 1
        result: list[tuple[int, Task | TaskGroup]] = [(depth, group)]
        for x in group.groups.values():
            result.extend(self._preorder(x, depth=next))
        for x in group.tasks.values():
            result.append((next, x))
        return result

    def _curses(self, stdscr: window) -> None:
        curses.start_color()
        curses.use_default_colors()
        M, C = 1000, 10
        for i in range(C):
            c = int((float(i + 1) / C) * M)
            curses.init_color(i + 1, c, c, c)
            curses.init_pair(i + 1, i + 1, -1)
        em = self._eq
        assert em is not None
        root = TaskGroup("__root__")
        global_step = -1
        while not self._stop_flag:
            time.sleep(self._refresh_rate)
            global_step = global_step + 1 % 10000
            while (event := em.capture()) is not None:
                if isinstance(event, TaskCompleteEvent):
                    task = self._get_task(root, event.task_id, -1)
                    task.complete = True
                elif isinstance(event, TaskUpdateEvent):
                    task = self._get_task(root, event.task_id, event.total)
                    task.units[event.unit] = True

            list_of_tasks = self._preorder(root, -1)[1:]
            if len(list_of_tasks) == 0:
                continue

            stdscr.clear()
            TITLE_H = len(list_of_tasks)
            TITLE_W = 20
            PROG_H = TITLE_H
            PROG_W = stdscr.getmaxyx()[1] - TITLE_W
            BAR_W = (PROG_W - 10) // 2
            title_pad = curses.newpad(TITLE_H, TITLE_W)
            prog_pad = curses.newpad(PROG_H, PROG_W)
            for i, (depth, task) in enumerate(list_of_tasks):
                space = TITLE_W - 2 * depth - 1
                text = task.name
                if len(text) > space:
                    start = (global_step // 2) % (len(text) - space)
                    text = text[start : start + space]
                title_pad.addstr(i, 2 * depth, text)
                if isinstance(task, Task):
                    bars: list[list[int]]
                    j = 0
                    if BAR_W > 0:
                        if len(task.units) < BAR_W:
                            bars = []
                            div = BAR_W // len(task.units)
                            rem = BAR_W % len(task.units)
                            for ui, unit in enumerate(task.units):
                                bars.append([div + int(ui < rem), int(unit), 1])
                        elif len(task.units) == BAR_W:
                            bars = [[1, int(x), 1] for x in task.units]
                        else:
                            bars = [[1, 0, 0] for _ in range(BAR_W)]
                            for ui, unit in enumerate(task.units):
                                bar = bars[int(ui / len(task.units) * len(bars))]
                                bar[1] += int(unit)
                                bar[2] += 1
                        bar_elem = "██"
                        for size, comp, total in bars:
                            c = 1 + int((comp / total) * (C - 1))
                            prog_pad.addstr(i, j, bar_elem * size, curses.color_pair(c))
                            j += size * len(bar_elem)
                    perc = sum(task.units) / len(task.units)
                    perc = round(perc * 100, 2)
                    text = f"{perc}%"
                    if len(text) + 2 < PROG_W:
                        prog_pad.addstr(i, j + 2, text)

            # stdscr.refresh()
            H, W = stdscr.getmaxyx()
            title_pad.noutrefresh(max(0, TITLE_H - H), 0, 0, 0, H - 1, W - 1)
            prog_pad.noutrefresh(max(0, TITLE_H - H), 0, 0, TITLE_W, H - 1, W - 1)
            curses.doupdate()

    def _loop(self) -> None:
        wrapper(self._curses)
