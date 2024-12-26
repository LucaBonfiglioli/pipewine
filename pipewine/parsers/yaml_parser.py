from collections.abc import Iterable

import pydantic as pyd
import yaml

from pipewine.parsers.base import Parser


class YAMLParser[T: str | int | float | dict | list | pyd.BaseModel](Parser[T]):
    def parse(self, data: bytes) -> T:
        yaml_data = yaml.safe_load(data.decode())
        if self._type in (str, int, float, dict, list):
            return self._type(yaml_data)
        elif self._type is not None and issubclass(self._type, pyd.BaseModel):
            return self._type.model_validate(yaml_data)
        return yaml_data

    def dump(self, data: T) -> bytes:
        if isinstance(data, pyd.BaseModel):
            yaml_data = data.model_dump()
        else:
            yaml_data = data
        return yaml.safe_dump(yaml_data).encode()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".yaml", ".yml"]
