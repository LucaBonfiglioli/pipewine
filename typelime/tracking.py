from abc import ABC, abstractmethod


class Task(ABC):
    def __init__(self, title: str, parent: "Task | None") -> None:
        self._title = title
        self._parent = parent

    @property
    def parent(self) -> "Task | None":
        return self._parent

    @property
    def title(self) -> str:
        return self._title

    @property
    def completion(self) -> int:
        return self._get_completion()

    @property
    def total(self) -> int | None:
        return self._get_total()

    @property
    def is_complete(self) -> bool:
        return self.completion == self.total

    @abstractmethod
    def _get_completion(self) -> int: ...

    @abstractmethod
    def _get_total(self) -> int: ...


class TaskLeaf(Task):
    def __init__(self, title: str, parent: "Task | None", total: int) -> None:
        super().__init__(title, parent)
        self._total = total
        self._completion = 0

    def _get_completion(self) -> int:
        return self._completion

    def _get_total(self) -> int:
        return self._total

    def advance(self, n: int = 1) -> None:
        self._completion += n


class TaskGroup(Task):
    def __init__(self, title: str, parent: "Task | None") -> None:
        super().__init__(title, parent)
        self._children: list[Task] = []

    def _get_completion(self) -> int:
        return sum(int(x.is_complete) for x in self._children)

    def _get_total(self) -> int:
        return len(self._children)

    def add_task(self, title: str, total: int) -> TaskLeaf:
        task = TaskLeaf(title, self, total)
        self._children.append(task)
        return task

    def add_group(self, title: str) -> "TaskGroup":
        tgroup = TaskGroup(title, self)
        self._children.append(tgroup)
        return tgroup
