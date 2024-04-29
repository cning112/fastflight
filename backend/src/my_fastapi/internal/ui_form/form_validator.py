from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Iterator, Self, TypedDict

from pydantic import BaseModel, ConfigDict


class ValidateContext(TypedDict):
    rules: dict[str, Callable[[Any], None]]
    feedbacks: list[str]


_validate_context_var: ContextVar[None | ValidateContext] = ContextVar("_validate_rule_var", default=None)


@contextmanager
def validate_rules(rules: dict[str, Callable[[Any], None]]) -> Iterator[ValidateContext]:
    ctx: ValidateContext = dict(rules=rules, feedbacks=[])
    token = _validate_context_var.set(ctx)
    try:
        yield ctx
    finally:
        _validate_context_var.reset(token)


class FieldValidationError(ValueError):
    def __init__(self, message: str, field: str):
        super().__init__(message)
        self.field = field


class UiFormBase(BaseModel):
    def model_post_init(self, __context: Any) -> Self:
        super().model_post_init(__context)
        if not __context:
            return

        for name, check in __context["rules"].items():
            try:
                check(self)
            except FieldValidationError as e:
                __context["feedbacks"].append({"field": e.field, "msg": str(e)})
            except Exception as e:
                __context["feedbacks"].append({"msg": str(e)})

    model_config = ConfigDict(revalidate_instances="always")
