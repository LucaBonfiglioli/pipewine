from typing_extensions import Self


class Setuppable:
    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def __enter__(self) -> Self:
        self.setup()
        return self

    def __exit__(self) -> None:
        self.teardown()
