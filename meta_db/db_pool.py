from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool


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
