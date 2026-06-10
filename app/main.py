from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.database import init_db
from app.api.topics import router as topics_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    print("Database connected!")
    yield
    print("Shutting down...")

app = FastAPI(
    title="PulseQueue",
    description="Distributed Message Queue & Analytics Platform",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(topics_router, prefix="/api/v1", tags=["Topics"])

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "PulseQueue"}
