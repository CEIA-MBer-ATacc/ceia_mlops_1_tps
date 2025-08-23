from fastapi import FastAPI
from config import settings
from app.routers.linea import linea_router

app = FastAPI(title="Fast API service")

app.include_router(
    linea_router
)

@app.get("/")
async def index():
    """Root endpoint to check service status."""
 
    return {
        'api': {
            'name': settings.service_name,
            'version': settings.build_version,
        }
    }