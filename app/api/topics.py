from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.database import get_pool

router = APIRouter()

class TopicCreate(BaseModel):
    name: str
    partition_count: int = 1
    retention_ttl_s: int = 86400

@router.post("/topics")
async def create_topic(topic: TopicCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM topics WHERE name = $1", topic.name
        )
        if existing:
            raise HTTPException(status_code=400, detail="Topic already exists")
        
        row = await conn.fetchrow(
            """INSERT INTO topics (name, partition_count, retention_ttl_s)
               VALUES ($1, $2, $3) RETURNING id, name, partition_count""",
            topic.name, topic.partition_count, topic.retention_ttl_s
        )
        return {"id": str(row["id"]), "name": row["name"], "partition_count": row["partition_count"]}

@router.get("/topics")
async def list_topics():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, partition_count, created_at FROM topics")
        return [{"id": str(r["id"]), "name": r["name"], "partition_count": r["partition_count"]} for r in rows]
