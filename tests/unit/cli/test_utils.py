from contextlib import nullcontext
from typing import Any

import pytest
from pipewine.cli import (
    parse_source,
    parse_sink,
    parse_grabber,
    run_cli_workflow,
)
from pipewine.cli.utils import deep_get
from pipewine import (
    Sample,
    TypelessSample,
    TypedSample,
    Item,
    MemoryItem,
    PickleParser,
    Grabber,
    DatasetSource,
    DatasetSink,
)
from pipewine.workflows import Workflow


class MyMetadata:
    name: str
    values: list[int]


class MySample(TypedSample):
    metadata: Item[MyMetadata]


@pytest.mark.parametrize(
    ["sample", "key", "expected"],
    [
        [
            TypelessSample(
                metadata=MemoryItem({"a": 10, "b": 20, "c": [1, 2, 3]}, PickleParser())
            ),
            "metadata.c.2",
            3,
        ],
    ],
)
def test_deep_get(sample: Sample, key: str, expected: Any) -> None:
    assert deep_get(sample, key) == expected


@pytest.mark.parametrize(
    ["format_", "text", "fail"],
    [
        ["underfolder", 10, True],
        ["underfolder", "/path/to/something", False],
        ["__WRONG_FORMAT__", "/path/to/something", True],
    ],
)
@pytest.mark.parametrize("grabber", [Grabber(), Grabber(8, 20)])
def test_parse_source(format_: str, text: str, grabber: Grabber, fail: bool) -> None:
    cm = pytest.raises(SystemExit) if fail else nullcontext()
    with cm:
        source = parse_source(format_, text, grabber)
        assert isinstance(source, DatasetSource)


@pytest.mark.parametrize(
    ["format_", "text", "fail"],
    [
        ["__WRONG_FORMAT__", "/path/to/something", True],
        ["underfolder", 10, True],
        ["underfolder", "/path/to/something", False],
        ["underfolder", "/path/to/something,overwrite", False],
        ["underfolder", "/path/to/something,allow_if_empty,replicate", False],
    ],
)
@pytest.mark.parametrize("grabber", [Grabber(), Grabber(8, 20)])
def test_parse_sink(format_: str, text: str, grabber: Grabber, fail: bool) -> None:
    cm = pytest.raises(SystemExit) if fail else nullcontext()
    with cm:
        sink = parse_sink(format_, text, grabber)
        assert isinstance(sink, DatasetSink)


@pytest.mark.parametrize(
    ["text", "grabber"],
    [
        ["16", Grabber(16)],
        ["4,2", Grabber(4, 2)],
        ["8,20", Grabber(8, 20)],
    ],
)
def test_parse_grabber(text: str, grabber: Grabber) -> None:
    parsed = parse_grabber(text)
    assert isinstance(parsed, Grabber)
    assert parsed.num_workers == grabber.num_workers
    assert parsed.prefetch == grabber.prefetch


class MockRunWorkflow:
    def __init__(self, raises: Exception | None) -> None:
        self._raises = raises

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        if self._raises is not None:
            raise self._raises


@pytest.mark.parametrize("exc", [None, KeyboardInterrupt(), RuntimeError()])
def test_run_cli_workflow(monkeypatch, exc: Exception | None) -> None:
    wf = Workflow()
    import pipewine.cli.utils

    mock_runner = MockRunWorkflow(exc)
    monkeypatch.setattr(pipewine.cli.utils, "run_workflow", mock_runner)
    cm = pytest.raises(SystemExit) if exc is not None else nullcontext()
    with cm:
        run_cli_workflow(wf)
