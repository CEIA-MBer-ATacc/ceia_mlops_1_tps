from datetime import date
from typing import List

from pydantic import BaseModel


class LineInput(BaseModel):
    line: str
    date_init: date  # ISO 8601 format, for example: "2025-08-22"

class DateValue(BaseModel):
    date: date  # ISO 8601 format, for example: "2025-08-22"
    value: int

class LineOutput(BaseModel):
    results: List[DateValue]
