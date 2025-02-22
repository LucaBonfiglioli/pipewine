import os
from pathlib import Path
import pytest
from pipewine.cli import import_module


def test_import_module_from_file(sample_data) -> None:
    import_module(sample_data / "extensions" / "code.py")


def test_import_module_from_file_cwd(sample_data) -> None:
    import_module(Path("extensions") / "code.py", cwd=sample_data)


def test_import_module_from_file_fail(sample_data) -> None:
    with pytest.raises(ImportError):
        import_module(sample_data / "ERROR.py")


def test_import_module_from_class_path() -> None:
    import_module("pipewine.bundle")


def test_import_module_from_class_path_fail() -> None:
    with pytest.raises(ImportError):
        import_module("something.that.does.not.exist")


def test_import_module_from_code() -> None:
    code = """
def my_func() -> None:
    print("hello")
"""

    import_module(code)
    import_module(code)


def test_import_module_from_code_fail() -> None:
    code = """
this is not valid python code;
"""

    with pytest.raises(ImportError):
        import_module(code)
