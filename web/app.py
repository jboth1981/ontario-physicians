"""FastAPI application factory with lifespan management."""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db

WEB_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(WEB_DIR, "templates")
STATIC_DIR = os.path.join(WEB_DIR, "static")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify DB is accessible and schema is current
    conn = db.get_connection()
    conn.close()
    yield


app = FastAPI(title="Ontario Physician Search", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

from web.routes import router  # noqa: E402

app.include_router(router)
