"""Helpers to install missing Python dependencies at notebook runtime."""

from __future__ import annotations

import importlib
import subprocess
import sys


def ensure_python_modules(modules: dict[str, str] | list[str] | tuple[str, ...]) -> None:
    """Ensure the requested modules are importable, installing packages if needed."""
    if isinstance(modules, dict):
        module_to_package = dict(modules)
    else:
        module_to_package = {module_name: module_name for module_name in modules}

    missing_packages: list[str] = []
    for module_name, package_name in module_to_package.items():
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError:
            if package_name not in missing_packages:
                missing_packages.append(package_name)

    if not missing_packages:
        return

    subprocess.check_call([sys.executable, "-m", "pip", "install", *missing_packages])
    importlib.invalidate_caches()

    for module_name in module_to_package:
        importlib.import_module(module_name)
