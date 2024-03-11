import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date
from enum import Enum
from typing import Annotated, Any, Callable, Iterator, Self, TypedDict, Union

from fastapi import APIRouter
from pydantic import AfterValidator, BaseModel, ConfigDict

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ui_form")


class ValidateContext(TypedDict):
    rules: dict[str, Callable[[Any], None]]
    feedbacks: list[str]


_validate_context_var: ContextVar[None | ValidateContext] = ContextVar(
    "_validate_rule_var", default=None
)


@contextmanager
def validate_rules(
    rules: dict[str, Callable[[Any], None]],
) -> Iterator[ValidateContext]:
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


def tenor_str_checker(pattern: str) -> Callable[[str], str]:
    def checker(v: str) -> str:
        v = v.strip().upper()
        assert v, "Tenor (after trimmed) cannot be empty"
        match = re.fullmatch(pattern, v)
        assert match is not None, f"Invalid tenor: {v}"
        return v

    return checker


class RegexPattern(str, Enum):
    tenor_year_month = r"[+-]?([1-9]\d*Y)?([1-9]\d*M)?"
    tenor_business_day = r"[+-]?[1-9]\d*B"


TenorYearMonth = Annotated[
    str, AfterValidator(tenor_str_checker(RegexPattern.tenor_year_month))
]
TenorBusDay = Annotated[
    str, AfterValidator(tenor_str_checker(RegexPattern.tenor_business_day))
]


class GeneralUiForm(BaseModel):
    """Only validates each field without taking into account of other fields."""

    # name: Annotated[str, Field(min_length=4)]
    # email: EmailStr
    start: Union[date | TenorYearMonth | TenorBusDay,]
    # if `start` is a Tenor, `end` must be a Tenor too
    end: Union[date | TenorYearMonth | TenorBusDay,]

    def model_post_init(self, __context: Any) -> Self:
        # if no context, just call the super method
        # this is useful for testing the model without the context
        # (i.e. without the `validate_rules` context manager)
        # and for testing the model with the context manager
        # (i.e. with the `validate_rules` context manager)
        # but not for testing the model with the context manager
        # (i.e. with the `validate_rules` context manager)
        # and without the `validate_rules` context manager
        # (i.e. without the `validate_rules` context manager)
        # because the `validate_rules` context manager will set the context variable
        # and the `model_post_init` method will check the context variable
        # and if the context variable is set, it will call the `model_post_init` method
        # and if the context variable is not set, it will not call the `model_post_init` method
        # and it will not set the context variable
        # and it will not check the context variable
        # and it will not set the context variable
        super().model_post_init(__context)
        if not __context:
            return

        for name, rule in __context["rules"].items():
            try:
                rule(self)
            except FieldValidationError as e:
                __context["feedbacks"].append({"field": e.field, "msg": str(e)})
            except Exception as e:
                __context["feedbacks"].append({"msg": str(e)})

    model_config = ConfigDict(revalidate_instances="always")


class ConditionalUiForm(GeneralUiForm):
    """Further validate each field with all the other available field values"""

    def model_post_init(self, __context: Any) -> None:
        issues = []
        start_is_date = isinstance(self.start, date)
        if start_is_date and not isinstance(self.end, date):
            issues.append(ValueError("`end` must be a date if `start` is a date"))
            self.end = None


def end_must_be_a_date_when_start_is_date(form: GeneralUiForm) -> None:
    if isinstance(form.start, date) and not isinstance(form.end, date):
        raise FieldValidationError("`end` must be a date if `start` is a date", "end")


@router.post("/validate")
def validate_ui_form(form: GeneralUiForm):
    # form is a valid `GeneralUiForm`, but we need to test it again the actual model
    with validate_rules(
        {"start_is_date": end_must_be_a_date_when_start_is_date}
    ) as ctx:
        validated = GeneralUiForm.model_validate(form, context=ctx)
        return {"validated": validated.model_dump(), "feedback": ctx["feedbacks"]}
