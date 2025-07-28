#!/usr/bin/env -S uv run --script --quiet

# /// script
# dependencies = ["nox", "nox-uv"]
# ///

import shlex

from nox import Session, options
from nox_uv import session

options.default_venv_backend = "uv"
options.reuse_existing_virtualenvs = True
options.stop_on_first_error = True  # Fail fast on first error

PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]
DEFAULT_PYTHON = "3.11"


@session(name="lint", uv_groups=["dev"], uv_all_extras=True)
def lint(s: Session) -> None:
    # Ruff linting
    s.run(*shlex.split("uv run ruff check --output-format=github ."))

    # Ruff formatting check
    s.run(*shlex.split("uv run ruff format --check --diff ."))

    # MyPy type checking
    s.run(*shlex.split("uv run mypy --config-file=pyproject.toml"))


@session(python=PYTHON_VERSIONS, name="tests", uv_groups=["dev"], uv_all_extras=True)
def tests(s: Session) -> None:
    """Run tests with coverage on multiple Python versions."""
    # Run tests with coverage
    s.run(
        "uv",
        "run",
        "pytest",
        "--cov=fastflight",
        "--cov-report=xml",
        "--cov-report=term",
        "--cov-branch",
        "--cov-fail-under=50",
        "--junit-xml=pytest.xml",
        "-v",
    )


@session(name="quality", uv_groups=["dev"])
def quality_analysis(s: Session):
    """Run comprehensive code quality analysis."""
    # Security analysis with Bandit
    s.run(*shlex.split("uv run --with 'bandit[toml]' bandit -r src/ -f txt --configfile pyproject.toml"))

    # Dependency vulnerability scan
    try:
        s.run(*shlex.split("uv run --with pip-audit pip-audit --format=columns"))
    except Exception:
        s.log("pip-audit failed, continuing...")

    # Dead code detection
    try:
        s.run(*shlex.split("uv run --with vulture vulture --config pyproject.toml"))
    except Exception:
        s.log("vulture failed, continuing...")

    # Dependency analysis with deptry
    try:
        s.run(*shlex.split("uv run --with deptry deptry src --config pyproject.toml"))
    except Exception:
        s.log("deptry analysis failed, continuing...")

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
    except Exception:
        s.log("complexity analysis failed, continuing...")


@session(name="build", uv_groups=["dev"], default=False)
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
