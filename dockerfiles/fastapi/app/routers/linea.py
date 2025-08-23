from app.schemas.linea import LineaInput, LineaOutput
from app.services.linea import Linea
from fastapi import APIRouter, status

linea_router = APIRouter(prefix="/linea", tags=["Linea"])


@linea_router.post(
    "/predecir",
    response_model=LineaOutput,
    status_code=status.HTTP_201_CREATED,
)
async def callback(
    linea: LineaInput,
):
    return await Linea.predecir_por_linea(linea)