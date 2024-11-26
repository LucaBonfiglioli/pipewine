from collections.abc import Iterable

from typelime import Parser, ParserRegistry


class TestParserRegistry:
    def test_registration(self) -> None:
        exts = ["myext", "myextension"]

        for x in exts:
            assert ParserRegistry.get(x) is None

        class MyParser(Parser[int]):
            def parse(self, data: bytes) -> int:
                return 10

            def dump(self, data: int) -> bytes:
                return b"10"

            @classmethod
            def extensions(cls) -> Iterable[str]:
                return exts

        for x in exts:
            assert ParserRegistry.get(x) is MyParser


class TestParser:
    def test_type(self) -> None:
        class MyParser(Parser[int]):
            def parse(self, data: bytes) -> int:
                return 10

            def dump(self, data: int) -> bytes:
                return b"10"

            @classmethod
            def extensions(cls) -> Iterable[str]:
                return []

        class MyInteger(int):
            pass

        assert MyParser().type_ is None
        assert MyParser(int).type_ is int
        assert MyParser(MyInteger).type_ is MyInteger
