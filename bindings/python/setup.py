from __future__ import annotations

import importlib.util
import os
from pathlib import Path

from distutils.errors import DistutilsExecError
from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


def _load_build_native():
    module_path = Path(__file__).with_name("build_native.py")
    spec = importlib.util.spec_from_file_location("latticedb_build_native", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load build helper from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_native = _load_build_native()


class build_py(_build_py):
    """Bundle liblattice into wheel builds."""

    def initialize_options(self) -> None:
        super().initialize_options()
        self._bundled_outputs: list[str] = []

    def run(self) -> None:
        super().run()

        if os.environ.get("LATTICE_SKIP_NATIVE_BUNDLE") == "1":
            return

        output_dir = Path(self.build_lib) / "latticedb" / "lib"
        target = os.environ.get("LATTICE_NATIVE_TARGET") or build_native.get_current_platform()
        bundle_lib_path = os.environ.get("LATTICE_BUNDLE_LIB_PATH")
        bundle_lib_dir = os.environ.get("LATTICE_BUNDLE_LIB_DIR")

        try:
            source_path = build_native.resolve_library_source(
                target,
                bundle_lib_path=bundle_lib_path,
                bundle_lib_dir=bundle_lib_dir,
            )
            dest_path = build_native.copy_library(source_path, target, output_dir=output_dir)
        except Exception as exc:  # pragma: no cover - exercised by wheel builds
            raise DistutilsExecError(
                "Failed to bundle liblattice for the Python package. "
                "Set LATTICE_BUNDLE_LIB_DIR or LATTICE_BUNDLE_LIB_PATH to a prebuilt library, "
                "or install Zig so the package build can produce liblattice."
            ) from exc

        self._bundled_outputs.append(str(dest_path))

    def get_outputs(self, include_bytecode: bool = True) -> list[str]:
        return super().get_outputs(include_bytecode) + self._bundled_outputs


setup(cmdclass={"build_py": build_py})
