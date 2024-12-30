from datetime import datetime
from typing import Any

import pytest
from pydantic import BaseModel

from pipewine import JSONParser, YAMLParser


class Entry(BaseModel):
    name: str
    age: int
    email: str


class Info(BaseModel):
    date: str
    author: str


class MyData(BaseModel):
    entries: list[Entry]
    info: Info


class TestJSONParser:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "entries": [
                    {
                        "name": "alice",
                        "age": 20,
                        "email": "alice@example.com",
                    },
                    {
                        "name": "bob",
                        "age": 25,
                        "email": "bob@example.com",
                    },
                    {
                        "name": "charlie",
                        "age": 15,
                        "email": "charlie@example.com",
                    },
                ],
                "info": {
                    "date": datetime.now().isoformat(),
                    "author": "luca",
                },
            },
        ],
    )
    def test_parse(self, data: dict) -> None:
        parser: JSONParser = JSONParser()
        re_data = parser.parse(parser.dump(data))
        assert isinstance(re_data, dict)
        assert MyData.model_validate(re_data) == MyData.model_validate(data)

    @pytest.mark.parametrize(
        "data",
        [
            MyData(
                entries=[
                    Entry(name="alice", age=20, email="alice@example.com"),
                    Entry(name="bob", age=20, email="bob@example.com"),
                    Entry(name="charlie", age=20, email="charlie@example.com"),
                ],
                info=Info(date=datetime.now().isoformat(), author="luca"),
            )
        ],
    )
    def test_parse_typed(self, data: MyData) -> None:
        parser = JSONParser(MyData)
        re_data = parser.parse(parser.dump(data))
        assert isinstance(re_data, MyData)
        assert re_data == data


class TestYAMLParser:
    @pytest.mark.parametrize(
        "data",
        [
            {
                "entries": [
                    {
                        "name": "alice",
                        "age": 20,
                        "email": "alice@example.com",
                    },
                    {
                        "name": "bob",
                        "age": 25,
                        "email": "bob@example.com",
                    },
                    {
                        "name": "charlie",
                        "age": 15,
                        "email": "charlie@example.com",
                    },
                ],
                "info": {
                    "date": datetime.now().isoformat(),
                    "author": "luca",
                },
            },
        ],
    )
    def test_parse(self, data: dict) -> None:
        parser: YAMLParser = YAMLParser()
        re_data = parser.parse(parser.dump(data))
        assert isinstance(re_data, dict)
        assert MyData.model_validate(re_data) == MyData.model_validate(data)

    @pytest.mark.parametrize(
        "data",
        [
            MyData(
                entries=[
                    Entry(name="alice", age=20, email="alice@example.com"),
                    Entry(name="bob", age=20, email="bob@example.com"),
                    Entry(name="charlie", age=20, email="charlie@example.com"),
                ],
                info=Info(date=datetime.now().isoformat(), author="luca"),
            )
        ],
    )
    def test_parse_typed(self, data: MyData) -> None:
        parser = YAMLParser(MyData)
        re_data = parser.parse(parser.dump(data))
        assert isinstance(re_data, MyData)
        assert re_data == data

    @pytest.mark.parametrize(
        "data",
        [
            10,
            5.0,
            "hello",
            [1, 2, 3, 4, 5],
            {
                "alice": 10,
                "bob": 15,
                "charlie": 20,
            },
        ],
    )
    def test_parse_builtin(self, data: Any) -> None:
        parser = YAMLParser(type(data))
        re_data = parser.parse(parser.dump(data))
        assert isinstance(re_data, type(data))
        assert re_data == data
