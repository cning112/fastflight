import logging
from datetime import date
from typing import Union

from fastapi import APIRouter

from demo.internal.ui_form.form_validator import FieldValidationError, UiFormBase, validate_rules
from demo.internal.ui_form.types import TenorBusDay, TenorYearMonth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ui_form")


class ExampleForm(UiFormBase):
    """Only validates each field without taking into account of other fields."""

    # name: Annotated[str, Field(min_length=4)]
    # email: EmailStr
    start: Union[date | TenorYearMonth | TenorBusDay,]
    # if `start` is a Tenor, `end` must be a Tenor too
    end: Union[date | TenorYearMonth | TenorBusDay,]


def end_must_be_a_date_when_start_is_date(form: ExampleForm) -> None:
    if isinstance(form.start, date) and not isinstance(form.end, date):
        raise FieldValidationError("`end` must be a date if `start` is a date", "end")


@router.post("/validate")
def validate_ui_form(form: ExampleForm):
    # form is a valid `GeneralUiForm`, but we need to test it again the actual model
    with validate_rules({"start_is_date": end_must_be_a_date_when_start_is_date}) as ctx:
        validated = ExampleForm.model_validate(form, context=ctx)
        return {"validated": validated.model_dump(), "feedback": ctx["feedbacks"]}
