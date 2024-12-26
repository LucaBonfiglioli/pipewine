from pipewine import JSONParser

from pydantic import BaseModel
from datetime import datetime
import pytest


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
