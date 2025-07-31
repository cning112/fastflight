#!/usr/bin/env -S uv run --script --quiet

# /// script
# dependencies = ["nox", "nox-uv"]
# ///

import shlex

import nox
from nox import Session, options
from nox_uv import session
from packaging import version

options.default_venv_backend = "uv"
options.reuse_existing_virtualenvs = True
options.stop_on_first_error = True  # Fail fast on first error

PYTHON_VERSIONS = ["3.10", "3.13"]
DEFAULT_PYTHON = "3.11"


def is_pyarrow_compatible(python_version: str, pyarrow_version: str) -> bool:
    py_ver = version.parse(python_version)
    pa_ver = version.parse(pyarrow_version)

    # Flight functionality became stable and feature-complete in PyArrow 14.x
    if pa_ver < version.parse("14.0.0"):
        return False

    if version.parse("14.0.0") <= pa_ver < version.parse("18.0.0"):
        # PyArrow 14.x-17.x: Python 3.8-3.12
        return version.parse("3.8") <= py_ver < version.parse("3.13")
    else:
        # PyArrow 18.x+: Python 3.9-3.13
        return version.parse("3.9") <= py_ver < version.parse("3.14")


@session(python=PYTHON_VERSIONS, name="tests")
@nox.parametrize("pyarrow_ver", ["14.0.2", "20.0.0"])
def tests(s: Session, pyarrow_ver) -> None:
    if not is_pyarrow_compatible(s.python, pyarrow_ver):
        s.skip("Python and pyarrow version are not compatible")

    s.run(
        *shlex.split(
            f"uv run --with pyarrow=={pyarrow_ver} "
            "pytest --cov=fastflight --cov-report=xml --cov-report=term --cov-branch --cov-fail-under=50 "
            "--junit-xml=pytest.xml -v"
        )
    )


@session(name="lint", uv_groups=["lint"], uv_all_extras=True)
def lint(s: Session) -> None:
    # Ruff linting
    s.run(*shlex.split("uv run ruff check --config=pyproject.toml --fix ."))

    # Ruff formatting check
    s.run(*shlex.split("uv run ruff format --config=pyproject.toml ."))

    # MyPy type checking
    s.run(*shlex.split("uv run mypy --config-file=pyproject.toml"))


@session(name="quality", uv_groups=["lint"])
def quality_analysis(s: Session):
    """Run comprehensive code quality analysis."""
    errors = []

    # Security analysis with Bandit
    try:
        s.run(*shlex.split("uv run --with 'bandit[toml]' bandit -r src/ -f txt --configfile pyproject.toml"))
    except Exception as e:
        errors.append(f"Bandit failed: {e}")

    # Dependency vulnerability scan
    try:
        s.run(*shlex.split("uv run --with pip-audit pip-audit --format=columns"))
    except Exception as e:
        errors.append(f"pip-audit failed: {e}")

    # Dead code detection
    try:
        s.run(*shlex.split("uv run --with vulture vulture --config pyproject.toml"))
    except Exception as e:
        errors.append(f"vulture failed: {e}")

    # Complexity analysis
    try:
        s.run(
            *shlex.split(
                "uv run --with radon radon cc src/ --show-complexity --exclude 'tests/*,examples/*,venv/*,.venv/*' "
                "--min=C"
            )
        )
        s.run(
            *shlex.split(
                "uv run --with xenon xenon --max-absolute C --max-modules B --max-average B "
                "--exclude 'tests,examples,venv,.venv' src/"
            )
        )
    except Exception as e:
        errors.append(f"complexity analysis failed: {e}")

    # Report all errors at the end
    if errors:
        error_msg = "Quality analysis failed with the following errors:\n" + "\n".join(f"  - {err}" for err in errors)
        s.error(error_msg)


@session(name="build", default=False)
def build_package(s: Session):
    """Build package and verify integrity."""
    s.run(*shlex.split("uv add --dev twine"))

    # Build package
    s.run(*shlex.split("uv build"))

    # Verify package integrity
    s.run(*shlex.split("uv run twine check dist/*"))

    # Basic installation test
    s.run(*shlex.split(f"uv venv test-env --python {DEFAULT_PYTHON}"))

    # Find the wheel file
    import glob

    wheel_files = glob.glob("dist/*.whl")
    if not wheel_files:
        s.error("No wheel files found in dist/")

    s.run("uv", "pip", "install", "--python", "test-env", wheel_files[0])
    s.run(
        *shlex.split(
            'uv run --python test-env python -c "import fastflight; '
            "print(f'FastFlight {fastflight.__version__} installed successfully')\""
        )
    )


@session(name="clean", default=False)
def clean(s: Session):
    """Clean build artifacts and cache files."""
    import pathlib
    import shutil

    # Directories to clean
    dirs_to_clean = [
        "dist",
        "build",
        ".nox",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".coverage",
        "htmlcov",
        "test-env",
    ]

    for dir_name in dirs_to_clean:
        path = pathlib.Path(dir_name)
        if path.exists():
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                    s.log(f"Removed directory: {dir_name}")
                else:
                    path.unlink()
                    s.log(f"Removed file: {dir_name}")
            except Exception as e:
                s.log(f"Failed to remove {dir_name}: {e}")

    # Clean Python cache files recursively
    for cache_file in pathlib.Path(".").rglob("__pycache__"):
        try:
            if cache_file.is_dir():
                shutil.rmtree(cache_file)
        except Exception as e:
            s.log(f"Failed to remove cache: {e}")

    for pyc_file in pathlib.Path(".").rglob("*.pyc"):
        try:
            pyc_file.unlink()
        except Exception as e:
            s.log(f"Failed to remove pyc: {e}")
