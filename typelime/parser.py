import json
import typing as t
from abc import ABC, abstractmethod

import numpy as np
import pydantic as pyd


class Parser[T: t.Any](ABC):
    @abstractmethod
    def parse(self, data: bytes) -> T:
        pass

    @abstractmethod
    def dump(self, data: T) -> bytes:
        pass


class NumpyParser(Parser[np.ndarray]):
    # TODO
    pass


class JSONParser[T: pyd.BaseModel](Parser[T]):
    def __init__(self, model: type[T] | None = None) -> None:
        self._model = model

    def parse(self, data: bytes) -> T:
        json_data = json.loads(data.decode())
        if self._model:
            return self._model.model_validate(json_data)
        return json_data

    def dump(self, data: T) -> bytes:
        if isinstance(data, pyd.BaseModel):
            json_data = data.model_dump()
        else:
            json_data = data
        return json.dumps(json_data).encode()


class YAMLParser[T_PYD: pyd.BaseModel](Parser[T_PYD]):
    #! TODO
    pass
