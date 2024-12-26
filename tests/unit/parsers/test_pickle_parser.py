from pipewine import PickleParser
import pytest


class MyClass:
    def __init__(self, *args, **kwargs) -> None:
        self._args = args
        self._kwargs = kwargs

    def print(self):
        print("hello")

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, MyClass):
            return False
        if len(self._args) != len(value._args):
            return False
        for x, y in zip(self._args, value._args):
            if x != y:
                return False
        if set(self._kwargs.keys()) != set(value._kwargs.keys()):
            return False
        for k in self._kwargs:
            x = self._kwargs[k]
            y = value._kwargs[k]
            if x != y:
                return False
        return True


class TestPickleParser:
    @pytest.mark.parametrize(
        "data",
        [
            [MyClass(a=10, b="hello", c=MyClass(a=20, f=MyClass()))],
        ],
    )
    def test_parse(self, data) -> None:
        parser: PickleParser = PickleParser()
        assert parser.parse(parser.dump(data)) == data
