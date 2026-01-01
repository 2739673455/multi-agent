from contextlib import asynccontextmanager

from config import CONF
from neo4j import AsyncGraphDatabase
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool


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


@asynccontextmanager
async def get_session(db_cfg):
    db_url = {
        "mysql": "mysql+asyncmy",
        "postgresql": "postgresql+asyncpg",
    }[db_cfg.db_type] + "://%s:%s@%s:%s/%s" % (
        db_cfg.user,
        db_cfg.password,
        db_cfg.host,
        db_cfg.port,
        db_cfg.database,
    )
    engine = create_async_engine(db_url, poolclass=NullPool)
    async with AsyncSession(engine) as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
    await engine.dispose()
