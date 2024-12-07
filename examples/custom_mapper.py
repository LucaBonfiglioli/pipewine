from typelime import *
import time


class SlowMapper(Mapper):
    def __call__[T](self, idx: int, x: T) -> T:
        time.sleep(0.1)
        return x
