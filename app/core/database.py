import asyncpg
import redis.asyncio as aioredis
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

pool = None
redis_client = None

async def get_db_pool():
    return await asyncpg.create_pool(DB_URL)

async def create_schema(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT UNIQUE NOT NULL,
            partition_count INTEGER DEFAULT 1,
            retention_ttl_s INTEGER DEFAULT 86400,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS messages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            topic_id UUID REFERENCES topics(id),
            partition_num INTEGER DEFAULT 0,
            payload TEXT,
            idempotency_key TEXT UNIQUE,
            status TEXT DEFAULT 'pending',
            retry_count INTEGER DEFAULT 0,
            retry_after TIMESTAMPTZ DEFAULT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS consumer_groups (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            topic_id UUID REFERENCES topics(id),
            group_name TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(topic_id, group_name)
        );

        CREATE TABLE IF NOT EXISTS offsets (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            group_id UUID REFERENCES consumer_groups(id),
            partition_num INTEGER DEFAULT 0,
            last_message_id UUID,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, partition_num)
        );

        CREATE TABLE IF NOT EXISTS dead_letters (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id UUID REFERENCES messages(id),
            error_reason TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

async def init_db():
    global pool, redis_client
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await create_schema(conn)
    redis_client = await aioredis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True
    )

async def close_db():
    global pool, redis_client
    if pool:
        await pool.close()
    if redis_client:
        await redis_client.aclose()

async def get_pool():
    return pool

async def get_redis():
    return redis_client
