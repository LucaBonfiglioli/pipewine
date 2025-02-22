"""Parsers for metadata files."""

import json
from collections.abc import Iterable
from typing import Any, Protocol, Self

import yaml

from pipewine.parsers.base import Parser


class PydanticLike(Protocol):
    """Protocol for classes that behave like Pydantic models."""

    @classmethod
    def model_validate(cls, obj: Any) -> Self: ...
    def model_dump(self) -> dict: ...


class JSONParser[T: str | int | float | bool | dict | list | PydanticLike](Parser[T]):
    """Parser for JSON data. Can parse and dump basic types (str, int, float, bool,
    dict, list) as well as Pydantic models.
    """

    def parse(self, data: bytes) -> T:
        json_data = json.loads(data.decode())
        if self._type is None:
            return json_data
        elif issubclass(self._type, (str, int, float, bool, dict, list)):
            return self._type(json_data)  # type: ignore
        else:
            return self._type.model_validate(json_data)

    def dump(self, data: T) -> bytes:
        if isinstance(data, (str, int, float, bool, dict, list)):
            json_data = data  # type: ignore
        else:
            json_data = data.model_dump()
        return json.dumps(json_data).encode()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["json"]


class YAMLParser[T: str | int | float | bool | dict | list | PydanticLike](Parser[T]):
    """Parser for YAML data. Can parse and dump basic types (str, int, float, bool,
    dict, list) as well as Pydantic models.
    """

    def parse(self, data: bytes) -> T:
        yaml_data = yaml.safe_load(data.decode())
        if self._type is None:
            return yaml_data
        elif issubclass(self._type, (str, int, float, bool, dict, list)):
            return self._type(yaml_data)  # type: ignore
        else:
            return self._type.model_validate(yaml_data)

    def dump(self, data: T) -> bytes:
        if isinstance(data, (str, int, float, bool, dict, list)):
            yaml_data = data
        else:
            yaml_data = data.model_dump()
        return yaml.safe_dump(yaml_data).encode()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return ["yaml", "yml"]
