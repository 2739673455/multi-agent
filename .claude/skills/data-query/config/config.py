from pathlib import Path
from typing import Any

from dynaconf import Dynaconf
from pydantic import BaseModel

CONFIG_DIR = Path(__file__).parent


# ==================== db info config ====================
class ColumnInfoCfg(BaseModel):
    col_meaning: str
    field_meaning: dict[str, Any] | None
    col_alias: list[str] | None
    rel_col: str | None


class TableCfg(BaseModel):
    tb_name: str
    tb_meaning: str
    sync_col: list[str] | None
    no_sync_col: list[str] | None
    col_info: dict[str, ColumnInfoCfg] | None


class KnowledgeCfg(BaseModel):
    kn_name: str
    kn_desc: str
    kn_def: str
    rel_kn: list[int] | None
    rel_col: list[str] | None
    kn_alias: list[str] | None


class SkeletonCfg(BaseModel):
    query: str
    normal_query: str
    rel_kn: list[int]
    sql: str


class DBCfg(BaseModel):
    db_code: str
    db_name: str
    db_type: str
    host: str
    port: int
    user: str
    password: str
    database: str
    table: dict[str, TableCfg] | None = None
    knowledge: dict[int, KnowledgeCfg] | None = None
    skeleton: list[SkeletonCfg] | None = None


# ==================== base config ====================
class MetaDBCfg(BaseModel):
    neo4j: DBCfg


class LoggingConfig(BaseModel):
    level: str
    to_console: bool
    to_file: bool
    log_dir: str
    max_file_size: str


class ModelCfg(BaseModel):
    base_url: str
    api_key: str
    model: str
    params: dict[str, Any] = {}


class LLMCfg(BaseModel):
    embed_model: str
    extend_model: str
    filter_model: str
    nl2sql_models: list[str]
    vote_model: str
    models: dict[str, ModelCfg]


class BaseCfg(BaseModel):
    meta_db: MetaDBCfg
    logging: LoggingConfig
    llm: LLMCfg
    use_db_code: str
    max_tb_num: int
    max_col_per_tb: int
