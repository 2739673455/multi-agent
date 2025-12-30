from app.config import CONF
from neo4j import AsyncGraphDatabase

URI = f"neo4j://{CONF.meta_db.neo4j.host}:{CONF.meta_db.neo4j.port}"
AUTH = (f"{CONF.meta_db.neo4j.user}", f"{CONF.meta_db.neo4j.password}")

driver = AsyncGraphDatabase.driver(
    uri=URI,
    auth=AUTH,
    max_connection_pool_size=20,  # 连接池大小
    connection_timeout=20.0,  # 获取连接的超时时间
    max_connection_lifetime=3600,  # 连接回收时间
)
