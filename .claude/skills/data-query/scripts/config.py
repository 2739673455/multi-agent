from pathlib import Path
from typing import Any

from omegaconf import OmegaConf
from pydantic import BaseModel, computed_field

CONFIG_DIR = Path(__file__).parent


class LoggingCfg(BaseModel):
    level: str
    to_console: bool
    to_file: bool
    max_file_size: str


class ModelCfg(BaseModel):
    base_url: str
    api_key: str
    model: str
    params: dict[str, Any] = {}


class LLMCfg(BaseModel):
    extend_model: str
    filter_model: str
    nl2sql_models: list[str]
    vote_model: str
    models: dict[str, ModelCfg]


class MetaDBCfg(BaseModel):
    base_url: str
    retrieve_knowledge: str
    retrieve_column: str
    retrieve_cell: str

    @computed_field
    @property
    def retrieve_knowledge_url(self) -> str:
        return f"{self.base_url}{self.retrieve_knowledge}"

    @computed_field
    @property
    def retrieve_column_url(self) -> str:
        return f"{self.base_url}{self.retrieve_column}"

    @computed_field
    @property
    def retrieve_cell_url(self) -> str:
        return f"{self.base_url}{self.retrieve_cell}"


class BaseCfg(BaseModel):
    logging: LoggingCfg
    llm: LLMCfg
    meta_db: MetaDBCfg
    use_db_code: str
    max_tb_num: int
    max_col_per_tb: int


base_cfg = OmegaConf.load(CONFIG_DIR / "base_conf.yml")  # 加载
OmegaConf.resolve(base_cfg)  # 解析插值
CFG = BaseCfg.model_validate(base_cfg)  # 转换为配置类
