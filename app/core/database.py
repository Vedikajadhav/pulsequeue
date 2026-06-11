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

async def init_db():
    global pool, redis_client
    pool = await get_db_pool()
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
