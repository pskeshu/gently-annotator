"""FastAPI app entry point.

    python -m annotator.server               # use config.yaml in repo root
    python -m annotator.server --port 9000   # override port
    python -m annotator.server --config foo  # alternate config
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from pathlib import Path

import yaml
from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from .annotations import AnnotationStore
from .catalog import Catalog
from .routes import annotations as annotations_routes
from .routes import catalog as catalog_routes
from .routes import volume as volume_routes

logger = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
STATIC = ROOT / "static"
DEFAULT_CONFIG = ROOT / "config.yaml"


def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def _prewarm_summaries(app: FastAPI) -> None:
    """Walk every session in every available dataset to populate summary cache.

    Runs in a thread because the work is blocking SMB I/O. The first user
    request to /api/datasets/{ds}/sessions blocks on the same cache, so
    if we've already finished it returns instantly; if not, we share the
    one in-flight scan instead of starting a second one.
    """
    catalog = app.state.catalog
    for name, ds in catalog.datasets.items():
        if ds.root is None:
            continue
        t0 = time.time()
        try:
            await asyncio.to_thread(catalog.list_session_summaries, name)
        except Exception:
            logger.exception("Pre-warm of dataset %s failed", name)
            continue
        summaries = catalog.list_session_summaries(name)
        non_empty = sum(1 for s in summaries if s.embryo_count > 0)
        logger.info(
            "Pre-warmed %s: %d sessions (%d with data) in %.1fs",
            name, len(summaries), non_empty, time.time() - t0,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = app.state.config

    logger.info("Building dataset catalog...")
    app.state.catalog = Catalog(
        datasets_config=cfg["datasets"],
        stages=cfg["stages"],
    )

    db_path = ROOT / "annotations.db"
    app.state.store = AnnotationStore(db_path)
    logger.info("Annotations DB: %s", db_path)

    app.state.volume_cache = OrderedDict()
    app.state.volume_cache_max = cfg.get("cache", {}).get("max_volumes", 8)

    # Fire-and-forget background scan. The first /api/sessions call will
    # piggyback on the same in-progress cache entry once filled.
    prewarm_task = asyncio.create_task(_prewarm_summaries(app))

    yield

    if not prewarm_task.done():
        prewarm_task.cancel()


def create_app(config_path: Path = DEFAULT_CONFIG) -> FastAPI:
    cfg = load_config(config_path)
    app = FastAPI(title="Gently Annotator", lifespan=lifespan)
    app.state.config = cfg

    app.include_router(catalog_routes.router)
    app.include_router(volume_routes.router)
    app.include_router(annotations_routes.router)

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Gently Annotator server")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    host = args.host or cfg.get("host", "127.0.0.1")
    port = args.port or cfg.get("port", 8090)

    import uvicorn
    uvicorn.run(create_app(args.config), host=host, port=port)


if __name__ == "__main__":
    main()
