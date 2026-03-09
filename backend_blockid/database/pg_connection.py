"""
PostgreSQL connection pool via asyncpg.
Uses DATABASE_URL from .env.
"""
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

pool = None


async def init_db():
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=10
        )


async def get_conn():
    global pool
    if pool is None:
        await init_db()
    return await pool.acquire()


async def release_conn(conn):
    global pool
    await pool.release(conn)
