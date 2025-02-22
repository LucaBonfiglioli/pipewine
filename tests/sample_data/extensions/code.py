from dataclasses import dataclass


class MyClass:
    def __init__(self, num: int) -> None:
        self._num = num

    def hello(self) -> list[int]:
        return list(range(self._num))


@dataclass
class MyDataclass:
    num: int

    def hello(self) -> list[int]:
        return list(range(self.num))


def my_func(num: int) -> list[int]:
    return list(range(num))


my_variable: list[int] = list(range(10))
