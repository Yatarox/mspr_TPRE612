import aiomysql
import os

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "db": os.getenv("MYSQL_DATABASE", "rail_dw"),
    "charset": "utf8mb4",
    "autocommit": True,
}

pool = None


async def init_db_pool():
    global pool
    pool = await aiomysql.create_pool(**DB_CONFIG, minsize=5, maxsize=20)


async def close_db_pool():
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()


async def execute_query(query: str, params: tuple = ()):
    global pool
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(query, params)
            return await cursor.fetchall()
