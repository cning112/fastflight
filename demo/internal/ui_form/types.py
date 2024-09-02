import re
from enum import Enum
from typing import Annotated, Callable

from pydantic import AfterValidator


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


TenorYearMonth = Annotated[str, AfterValidator(tenor_str_checker(RegexPattern.tenor_year_month))]
TenorBusDay = Annotated[str, AfterValidator(tenor_str_checker(RegexPattern.tenor_business_day))]
