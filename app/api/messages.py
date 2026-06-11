from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any
import uuid
from app.core.database import get_pool

router = APIRouter()

class PublishMessage(BaseModel):
    payload: dict[str, Any]
    idempotency_key: str | None = None

@router.post("/topics/{topic_name}/publish")
async def publish_message(topic_name: str, message: PublishMessage):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Topic exists ka?
        topic = await conn.fetchrow(
            "SELECT id, partition_count FROM topics WHERE name = $1", topic_name
        )
        if not topic:
            raise HTTPException(status_code=404, detail=f"Topic '{topic_name}' not found")

        # Idempotency check
        if message.idempotency_key:
            existing = await conn.fetchrow(
                "SELECT id FROM messages WHERE idempotency_key = $1", 
                message.idempotency_key
            )
            if existing:
                return {"status": "duplicate", "message_id": str(existing["id"])}

        # Partition select karo
        partition = hash(message.idempotency_key or str(uuid.uuid4())) % topic["partition_count"]

        # Message insert karo
        row = await conn.fetchrow(
            """INSERT INTO messages (topic_id, partition_num, payload, idempotency_key, status)
               VALUES ($1, $2, $3, $4, 'pending')
               RETURNING id, status, created_at""",
            topic["id"], partition, 
            str(message.payload).replace("'", '"'),
            message.idempotency_key
        )

        return {
            "message_id": str(row["id"]),
            "topic": topic_name,
            "partition": partition,
            "status": row["status"]
        }
