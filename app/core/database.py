import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

async def get_db_pool():
    return await asyncpg.create_pool(DB_URL)

pool = None

async def init_db():
    global pool
    pool = await get_db_pool()

async def get_pool():
    return pool
