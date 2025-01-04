import pytest
from pipewine.cli import pipewine_app
from typer.testing import CliRunner


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_clone(tmp_path, underfolder, runner: CliRunner) -> None:
    input_folder = str(underfolder.folder)
    output_folder = str(tmp_path / "output")
    result = runner.invoke(
        pipewine_app,
        ["op", "clone", "--input", input_folder, "--output", output_folder],
    )
    assert result.exit_code == 0
