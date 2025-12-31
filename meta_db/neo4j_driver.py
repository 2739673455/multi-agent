from contextlib import asynccontextmanager

from app.config import CONF
from neo4j import AsyncGraphDatabase


@asynccontextmanager
async def neo4j_session():
    URI = f"neo4j://{CONF.meta_db.neo4j.host}:{CONF.meta_db.neo4j.port}"
    AUTH = (f"{CONF.meta_db.neo4j.user}", f"{CONF.meta_db.neo4j.password}")
    driver = AsyncGraphDatabase.driver(uri=URI, auth=AUTH)
    try:
        async with driver.session() as session:
            yield session
    finally:
        await driver.close()
