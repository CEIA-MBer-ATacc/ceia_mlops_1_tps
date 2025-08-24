from fastapi import FastAPI

from app.config import settings
from app.routers.line import line_router

app = FastAPI(title="Fast API service")

app.include_router(
    line_router
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
