from collections.abc import Iterable

import pydantic as pyd
import yaml

from typelime.parsers.base import Parser


class YAMLParser[T: pyd.BaseModel](Parser[T]):
    def parse(self, data: bytes) -> T:
        yaml_data = yaml.safe_load(data.decode())
        if self._type:
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
