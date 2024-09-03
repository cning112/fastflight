import shlex

import nox

PY_VERSION = "3.11"


@nox.session(python=PY_VERSION)
def tests(session):
    session.install("pytest")
    session.run("pytest")


@nox.session(python=PY_VERSION, reuse_venv=True)
def style(session):
    session.install("ruff")
    fix = bool(session.posargs and session.posargs[0] == "fix")
    session.run(*shlex.split(f"ruff check --config=./pyproject.toml {'--fix' if fix else ''}"))
    session.run(*shlex.split(f"ruff format --config=./pyproject.toml {'' if fix else '--diff'}"))
