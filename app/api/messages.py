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

class ConsumeRequest(BaseModel):
    group_name: str
    max_messages: int = 10

@router.post("/topics/{topic_name}/consume")
async def consume_messages(topic_name: str, request: ConsumeRequest):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Topic check
        topic = await conn.fetchrow(
            "SELECT id FROM topics WHERE name = $1", topic_name
        )
        if not topic:
            raise HTTPException(status_code=404, detail=f"Topic '{topic_name}' not found")

        # Consumer group find or create
        group = await conn.fetchrow(
            "SELECT id FROM consumer_groups WHERE topic_id = $1 AND group_name = $2",
            topic["id"], request.group_name
        )
        if not group:
            group = await conn.fetchrow(
                """INSERT INTO consumer_groups (topic_id, group_name)
                   VALUES ($1, $2) RETURNING id""",
                topic["id"], request.group_name
            )

        # SKIP LOCKED — safe concurrent dequeue
        rows = await conn.fetch(
            """SELECT id, payload, partition_num, retry_count
               FROM messages
               WHERE topic_id = $1 AND status = 'pending'
               ORDER BY created_at ASC
               LIMIT $2
               FOR UPDATE SKIP LOCKED""",
            topic["id"], request.max_messages
        )

        if not rows:
            return {"messages": [], "count": 0}

        # Status update to processing
        message_ids = [row["id"] for row in rows]
        await conn.execute(
            "UPDATE messages SET status = 'processing' WHERE id = ANY($1::uuid[])",
            message_ids
        )

        # Offset update
        await conn.execute(
            """INSERT INTO offsets (group_id, partition_num, last_message_id, updated_at)
               VALUES ($1, 0, $2, NOW())
               ON CONFLICT (group_id, partition_num) 
               DO UPDATE SET last_message_id = $2, updated_at = NOW()""",
            group["id"], message_ids[-1]
        )

        return {
            "messages": [
                {
                    "id": str(r["id"]),
                    "payload": r["payload"],
                    "partition": r["partition_num"],
                    "retry_count": r["retry_count"]
                } for r in rows
            ],
            "count": len(rows)
        }

class AckMessage(BaseModel):
    message_id: str
    success: bool
    error_reason: str | None = None

@router.post("/messages/ack")
async def acknowledge_message(ack: AckMessage):
    pool = await get_pool()
    async with pool.acquire() as conn:
        message = await conn.fetchrow(
            "SELECT id, retry_count, topic_id FROM messages WHERE id = $1",
            ack.message_id
        )
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")

        if ack.success:
            # Success — consumed
            await conn.execute(
                "UPDATE messages SET status = 'consumed' WHERE id = $1",
                message["id"]
            )
            return {"status": "consumed", "message_id": ack.message_id}
        else:
            retry_count = message["retry_count"] + 1
            if retry_count >= 3:
                # Dead-letter queue
                await conn.execute(
                    "UPDATE messages SET status = 'failed' WHERE id = $1",
                    message["id"]
                )
                await conn.execute(
                    """INSERT INTO dead_letters (message_id, error_reason)
                       VALUES ($1, $2)""",
                    message["id"], ack.error_reason or "Max retries exceeded"
                )
                return {"status": "dead_lettered", "message_id": ack.message_id}
            else:
                # Retry
                await conn.execute(
                    """UPDATE messages SET status = 'pending', retry_count = $1
                       WHERE id = $2""",
                    retry_count, message["id"]
                )
                return {"status": "retrying", "retry_count": retry_count, "message_id": ack.message_id}
