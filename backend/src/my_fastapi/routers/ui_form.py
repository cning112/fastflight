import logging
import re
from datetime import date
from enum import Enum
from typing import Annotated, Any, Callable, Self, Union

from fastapi import APIRouter
from pydantic import AfterValidator, BaseModel, EmailStr, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ui_form")


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
    name: Annotated[str, Field(min_length=4)]
    email: EmailStr
    start: Union[date | TenorYearMonth | TenorBusDay,]
    # if `start` is a Tenor, `end` must be a Tenor too
    end: Union[date | TenorYearMonth | TenorBusDay,]


class ConditionalUiForm(GeneralUiForm):
    def model_post_init(self, __context: Any) -> Self:
        issues = []
        start_is_date = isinstance(self.start, date)
        if start_is_date and not isinstance(self.end, date):
            issues.append(ValueError("`end` must be a date if `start` is a date"))
            self.end = None
        return self


@router.get("/")
def get_ui_form():
    return
