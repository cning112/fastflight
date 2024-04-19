import shlex

import nox


@nox.session(python="3.11")
def tests(session):
    session.install("pytest")
    session.run("pytest")


@nox.session(python="3.11")
def lint(session):
    session.install("ruff")
    fix = bool(session.posargs and session.posargs[0] == "fix")
    session.run(*shlex.split(f"ruff check --config=./pyproject.toml {'--fix' if fix else ''}"))
    session.run(*shlex.split(f"ruff format --config=./pyproject.toml {'' if fix else '--diff'}"))
