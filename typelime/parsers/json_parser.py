import json
from collections.abc import Iterable

import pydantic as pyd

from typelime.parsers.base import Parser


class JSONParser[T: pyd.BaseModel](Parser[T]):
    def parse(self, data: bytes) -> T:
        json_data = json.loads(data.decode())
        if self._type:
            return self._type.model_validate(json_data)
        return json_data

    def dump(self, data: T) -> bytes:
        if isinstance(data, pyd.BaseModel):
            json_data = data.model_dump()
        else:
            json_data = data
        return json.dumps(json_data).encode()

    @classmethod
    def extensions(cls) -> Iterable[str]:
        return [".json"]
