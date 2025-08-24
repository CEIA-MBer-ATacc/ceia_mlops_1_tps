from datetime import date
from typing import List

from pydantic import BaseModel


class LineaInput(BaseModel):
    linea: str
    date_init: date # Formato ISO 8601, por ejemplo: "2025-08-22"

class FechaValor(BaseModel):
    fecha: date # Formato ISO 8601, por ejemplo: "2025-08-22"
    valor: int

class LineaOutput(BaseModel):
    resultados: List[FechaValor]
