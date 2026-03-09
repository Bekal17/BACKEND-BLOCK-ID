import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

async def main():

    print("Connecting to:", DATABASE_URL)

    conn = await asyncpg.connect(DATABASE_URL)

    result = await conn.fetch("SELECT NOW()")

    print(result)

    await conn.close()

asyncio.run(main())