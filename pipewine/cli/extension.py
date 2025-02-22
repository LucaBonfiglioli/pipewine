"""Dynamic imports of Python modules from files, class paths, or source code."""

import hashlib
import importlib
import importlib.util
import sys
import threading
import weakref
from contextlib import ContextDecorator
from pathlib import Path
from types import ModuleType
from typing import Dict

_imp_lock = threading.Lock()
_module_locks: Dict[str, "weakref.ReferenceType[threading.Lock]"] = {}


# We follow the importlib implementation about module locking
# to lock when importing modules from python code strings
# NB: no need to check for deadlock, as it is used only for anonymous modules
def _get_module_lock(name):  # pragma: no cover
    """Get or create the module lock for a given module name."""
    # Acquire/release internally the global import lock to protect _module_locks.
    with _imp_lock:
        try:
            lock = _module_locks[name]()
        except KeyError:
            lock = None

        if lock is None:
            lock = threading.Lock()

            def cb(ref, name=name):
                with _imp_lock:
                    # bpo-31070: Check if another thread created a new lock
                    # after the previous lock was destroyed
                    # but before the weakref callback was called.
                    if _module_locks.get(name) is ref:
                        del _module_locks[name]

            _module_locks[name] = weakref.ref(lock, cb)

        return lock


class _ModuleLockManager:
    def __init__(self, name):
        self._name = name
        self._lock = None

    def __enter__(self):
        self._lock = _get_module_lock(self._name)
        self._lock.acquire()

    def __exit__(self, *args, **kwargs):
        self._lock.release()  # type: ignore


class _add_to_sys_path(ContextDecorator):
    """add_to_sys_path context decorator temporarily adds a path to sys.path"""

    def __init__(self, path: str) -> None:
        self._new_cwd = path

    def __enter__(self) -> None:
        sys.path.insert(0, self._new_cwd)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        sys.path.pop(0)


def _import_module_from_file(
    module_file_path: str | Path, cwd: Path | None = None
) -> ModuleType:
    """Import a python module from a file.

    Args:
        module_file_path (Union[str, Path]): the path to the `.py` module file.
        cwd (Optional[Path], optional): the folder to use for relative module import.
            Defaults to None.

    Raises:
        ImportError: if the module cannot be imported.

    Returns:
        ModuleType: the imported module.
    """
    module_path = Path(module_file_path)
    if not module_path.is_absolute() and cwd is not None:
        module_path = cwd / module_path

    if not module_path.exists():
        raise ModuleNotFoundError(f"Module not found: {module_file_path}")

    with _add_to_sys_path(str(module_path.parent)):
        return _import_module_from_class_path(module_path.stem)


def _import_module_from_class_path(module_class_path: str) -> ModuleType:
    """Import a python module from a python class path.

    Args:
        module_class_path (str): the dot-separated path to the module class.

    Returns:
        ModuleType: the imported module.
    """
    return importlib.import_module(module_class_path)


def _import_module_from_code(module_code: str) -> ModuleType:
    """Dynamically imports a Python module from its source code.

    Args:
        module_code (str): the python source code of the module to import.

    Raises:
        ImportError: if the module cannot be imported.

    Returns:
        ModuleType: the imported module.
    """

    hash_fn = hashlib.blake2b()
    hash_fn.update(module_code.encode("utf-8"))
    name = hash_fn.hexdigest()

    with _ModuleLockManager(name):
        # check if the module is already imported
        try:
            spec = importlib.util.find_spec(name)
        except Exception:  # pragma: no cover
            spec = None

        if spec is not None:
            module = importlib.import_module(name)
        else:
            # create a new module from code
            spec = importlib.util.spec_from_loader(name, loader=None)
            if spec is None:  # pragma: no cover
                raise ImportError(f"Cannot create spec for module `{module_code}`")
            module = importlib.util.module_from_spec(spec)

            # compile the code and put everything in the new module
            exec(module_code, module.__dict__)

            # add the module to sys.modules
            sys.modules[name] = module

        return module


def import_module(
    module_file_or_class_path_or_code: str | Path, cwd: Path | None = None
) -> ModuleType:
    """Import a module from a file, a class path, or its source code.

    Args:
        module_file_or_class_path_or_code (str): the path to the module file, the
            python class path, or its python code.
        cwd (Optional[Path], optional): the current working directory for relative
            imports. Defaults to None.

    Raises:
        ImportError: If the module cannot be imported.

    Returns:
        ModuleType: The imported module.
    """
    module_file_or_class_path_or_code = str(module_file_or_class_path_or_code)
    err_msgs = [""]

    if module_file_or_class_path_or_code.endswith(".py"):
        try:
            return _import_module_from_file(module_file_or_class_path_or_code, cwd)
        except Exception as e:
            err_msgs.append(f"from file: {e}")
    try:
        return _import_module_from_class_path(module_file_or_class_path_or_code)
    except Exception as e:
        err_msgs.append(f"from classpath: {e}")

    try:
        return _import_module_from_code(module_file_or_class_path_or_code)
    except Exception as e:
        err_msgs.append(f"from code: {e}")

    raise ImportError(
        "Cannot import:\n"
        f"  `{module_file_or_class_path_or_code}`\n"
        "Possible causes:" + "\n  ".join(err_msgs)
    )
