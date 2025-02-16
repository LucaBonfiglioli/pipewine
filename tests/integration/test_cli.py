from collections.abc import Mapping, Sequence
from pathlib import Path
import sys
from typing import IO, Annotated, Any

from click.testing import Result
import numpy as np
import pytest
from pydantic import BaseModel
from typer import Option, Typer
from typer.testing import CliRunner

from pipewine import Dataset, DatasetOperator, Item, TypedSample, Bundle, Grabber
from pipewine.cli import pipewine_app, op_cli


class LetterMetadata(BaseModel):
    letter: str
    color: str


class LetterSample(TypedSample):
    image: Item[np.ndarray]
    metadata: Item[LetterMetadata]
    shared: Item


class MyBundle(Bundle[Dataset[LetterSample]]):
    a: Dataset[LetterSample]
    b: Dataset[LetterSample]
    c: Dataset[LetterSample]


class Dataset2Dataset(DatasetOperator[Dataset[LetterSample], Dataset[LetterSample]]):
    def __call__(self, x: Dataset[LetterSample]) -> Dataset[LetterSample]:
        return x


class List2List(
    DatasetOperator[Sequence[Dataset[LetterSample]], Sequence[Dataset[LetterSample]]]
):
    def __call__(
        self, x: Sequence[Dataset[LetterSample]]
    ) -> Sequence[Dataset[LetterSample]]:
        return x


class List2ListUnannotated(
    DatasetOperator[Sequence[Dataset[LetterSample]], Sequence[Dataset[LetterSample]]]
):
    def __call__(self, x: Sequence) -> Sequence:
        return x


class Tuple2Tuple(
    DatasetOperator[
        tuple[Dataset[LetterSample], Dataset[LetterSample], Dataset[LetterSample]],
        tuple[Dataset[LetterSample], Dataset[LetterSample], Dataset[LetterSample]],
    ]
):
    def __call__(
        self,
        x: tuple[Dataset[LetterSample], Dataset[LetterSample], Dataset[LetterSample]],
    ) -> tuple[Dataset[LetterSample], Dataset[LetterSample], Dataset[LetterSample]]:
        return x


class Tuple2TupleEllipsis(
    DatasetOperator[
        tuple[Dataset[LetterSample], ...], tuple[Dataset[LetterSample], ...]
    ]
):
    def __call__(
        self, x: tuple[Dataset[LetterSample], ...]
    ) -> tuple[Dataset[LetterSample], ...]:
        return x


class Mapping2Mapping(
    DatasetOperator[
        Mapping[str, Dataset[LetterSample]], Mapping[str, Dataset[LetterSample]]
    ],
):
    def __call__(
        self, x: Mapping[str, Dataset[LetterSample]]
    ) -> Mapping[str, Dataset[LetterSample]]:
        return x


class Mapping2MappingUnannotated(
    DatasetOperator[
        Mapping[str, Dataset[LetterSample]], Mapping[str, Dataset[LetterSample]]
    ],
):
    def __call__(self, x: Mapping) -> Mapping:
        return x


class Bundle2Bundle(DatasetOperator[MyBundle, MyBundle]):
    def __call__(self, x: MyBundle) -> MyBundle:
        return x


@op_cli("mock_dataset2dataset")
def dataset2dataset() -> Dataset2Dataset:
    return Dataset2Dataset()


@op_cli("mock_dataset2dataset_params")
def dataset2dataset_params(
    p1,
    p2: int,
    p3: Annotated[int, Option(..., "--p3")],
    p4: int = 30,
    p5: Annotated[int, Option(..., "--p5")] = 50,
) -> Dataset2Dataset:
    return Dataset2Dataset()


@op_cli("mock_list2list")
def list2list() -> List2List:
    return List2List()


@op_cli("mock_list2list_unannotated")
def list2list_unannotated() -> List2ListUnannotated:
    return List2ListUnannotated()


@op_cli("mock_tuple2tuple_ellipsis")
def tuple2tuple_ellipsis() -> Tuple2TupleEllipsis:
    return Tuple2TupleEllipsis()


@op_cli("mock_tuple2tuple")
def tuple2tuple() -> Tuple2Tuple:
    return Tuple2Tuple()


@op_cli("mock_mapping2mapping")
def mapping2mapping() -> Mapping2Mapping:
    return Mapping2Mapping()


@op_cli("mock_mapping2mapping_unannotated")
def mapping2mapping_unannotated() -> Mapping2MappingUnannotated:
    return Mapping2MappingUnannotated()


@op_cli("mock_bundle2bundle")
def bundle2bundle() -> Bundle2Bundle:
    return Bundle2Bundle()


class _PatchedRunner(CliRunner):
    def invoke(
        self,
        app: Typer,
        args: str | Sequence[str] | None = None,
        input: bytes | str | IO[Any] | None = None,
        env: Mapping[str, str] | None = None,
        catch_exceptions: bool = True,
        color: bool = False,
        **extra,
    ) -> Result:
        old_args = sys.argv
        sys.argv = args
        result = super().invoke(app, args, input, env, catch_exceptions, color, **extra)
        sys.argv = old_args
        return result


@pytest.fixture
def runner() -> CliRunner:
    return _PatchedRunner()


def test_help(runner: CliRunner) -> None:
    result = runner.invoke(pipewine_app, ["--help"])
    assert result.exit_code == 0


def test_version(runner: CliRunner) -> None:
    result = runner.invoke(pipewine_app, ["--version"])
    assert result.exit_code == 0


def test_op_help(runner: CliRunner) -> None:
    result = runner.invoke(pipewine_app, ["op", "--help"])
    assert result.exit_code == 0


def test_op_format_help(runner: CliRunner) -> None:
    result = runner.invoke(pipewine_app, ["op", "--format-help"])
    assert result.exit_code == 0


def test_fail_multiple_grabbers() -> None:
    with pytest.raises(ValueError):

        @op_cli()
        def multiple_grabbers(g1: Grabber, g2: Grabber) -> Dataset2Dataset:
            return Dataset2Dataset()


def test_fail_grabber_default() -> None:
    with pytest.raises(ValueError):

        @op_cli()
        def grabber_default(g1: Grabber = Grabber()) -> Dataset2Dataset:
            return Dataset2Dataset()


def test_dataset2dataset(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folder = str(tmp_path / "output")
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "mock_dataset2dataset",
            "--input",
            input_folder,
            "--output",
            output_folder,
        ],
    )
    assert Path(output_folder).is_dir()
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "cmd", ["mock_list2list", "mock_tuple2tuple_ellipsis", "mock_list2list_unannotated"]
)
def test_list2list(tmp_path, underfolder, runner: CliRunner, cmd: str) -> None:
    input_folder = str(underfolder.folder)
    output_folders = [str(tmp_path / f"output_{i}") for i in range(3)]
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            cmd,
            "--input",
            input_folder,
            "--input",
            input_folder,
            "--input",
            input_folder,
            "--output",
            output_folders[0],
            "--output",
            output_folders[1],
            "--output",
            output_folders[2],
        ],
    )
    for output_folder in output_folders:
        assert Path(output_folder).is_dir()
    assert result.exit_code == 0


def test_tuple2tuple(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folders = [str(tmp_path / f"output_{i}") for i in range(3)]
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "mock_tuple2tuple",
            "--input",
            input_folder,
            input_folder,
            input_folder,
            "--output",
            output_folders[0],
            output_folders[1],
            output_folders[2],
        ],
    )
    for output_folder in output_folders:
        assert Path(output_folder).is_dir()
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "cmd", ["mock_mapping2mapping", "mock_mapping2mapping_unannotated"]
)
def test_mapping2mapping(tmp_path, underfolder, runner: CliRunner, cmd: str) -> None:
    input_folder = str(underfolder.folder)
    output_folders = [str(tmp_path / f"output_{i}") for i in range(3)]
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            cmd,
            "-i.a",
            input_folder,
            "-i.b",
            input_folder,
            "-i.c",
            input_folder,
            "-o.a",
            output_folders[0],
            "-o.b",
            output_folders[1],
            "-o.c",
            output_folders[2],
        ],
    )
    for output_folder in output_folders:
        assert Path(output_folder).is_dir(), result.stdout
    assert result.exit_code == 0


def test_bundle2bundle(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folders = [str(tmp_path / f"output_{i}") for i in range(3)]
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "mock_bundle2bundle",
            "--input.a",
            input_folder,
            "--input.b",
            input_folder,
            "--input.c",
            input_folder,
            "--output.a",
            output_folders[0],
            "--output.b",
            output_folders[1],
            "--output.c",
            output_folders[2],
        ],
    )
    for output_folder in output_folders:
        assert Path(output_folder).is_dir()
    assert result.exit_code == 0


def test_clone(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folder = str(tmp_path / "output")
    result = runner.invoke(
        pipewine_app,
        ["op", "clone", "--input", input_folder, "--output", output_folder],
    )
    assert Path(output_folder).is_dir()
    assert result.exit_code == 0


@pytest.mark.parametrize("compare", ["eq", "neq", "gt", "lt", "ge", "le"])
def test_filter(tmp_path, underfolder, runner: CliRunner, compare: str) -> None:
    input_folder = str(underfolder.folder)
    output_folder = str(tmp_path / "output")
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "filter",
            "--input",
            input_folder,
            "--output",
            output_folder,
            "-k",
            "metadata.letter",
            "-c",
            compare,
            "-t",
            "e",
        ],
    )
    assert Path(output_folder).is_dir()
    assert result.exit_code == 0


def test_groupby(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folders = {
        "orange": str(tmp_path / "output" / "orange"),
        "cyan": str(tmp_path / "output" / "cyan"),
        "green": str(tmp_path / "output" / "green"),
    }
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "groupby",
            "--input",
            input_folder,
            "-o.orange",
            output_folders["orange"],
            "-o.cyan",
            output_folders["cyan"],
            "-o.green",
            output_folders["green"],
            "-k",
            "metadata.color",
        ],
    )
    for output_folder in output_folders.values():
        assert Path(output_folder).is_dir()
    assert result.exit_code == 0


def test_sort(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folder = str(tmp_path / "output")
    result = runner.invoke(
        pipewine_app,
        [
            "op",
            "sort",
            "--input",
            input_folder,
            "--output",
            output_folder,
            "-k",
            "metadata.color",
        ],
    )
    assert Path(output_folder).is_dir()
    assert result.exit_code == 0


if __name__ == "__main__":
    pipewine_app()
