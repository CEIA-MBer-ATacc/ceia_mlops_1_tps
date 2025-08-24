from fastapi import APIRouter, status

from app.schemas.line import LineInput, LineOutput
from app.services.line import Line


line_router = APIRouter(prefix="/line", tags=["Line"])

@line_router.post(
    "/predict",
    response_model=LineOutput,
    status_code=status.HTTP_201_CREATED,
)
async def callback(
    line: LineInput,
):
    return await Line.predict_by_line(line)
