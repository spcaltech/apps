from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse
from starlette.staticfiles import StaticFiles

from .api.routes import router as api_router


def create_app() -> FastAPI:
    app = FastAPI(title="Model Prefetcher", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_router, prefix="/api")

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    index_file = static_dir / "index.html"

    @app.get("/")
    def read_root():
        return FileResponse(index_file)

    return app


app = create_app()