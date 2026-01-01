from pathlib import Path
from typing import Any

from omegaconf import OmegaConf
from pydantic import BaseModel

CONFIG_DIR = Path(__file__).parent


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
    extend_model: str
    filter_model: str
    nl2sql_models: list[str]
    vote_model: str
    models: dict[str, ModelCfg]


class BaseCfg(BaseModel):
    logging: LoggingConfig
    llm: LLMCfg
    use_db_code: str
    max_tb_num: int
    max_col_per_tb: int


base_cfg = OmegaConf.load(CONFIG_DIR / "base_conf.yml")  # 加载
OmegaConf.resolve(base_cfg)  # 解析插值
CONF = BaseCfg.model_validate(base_cfg)  # 转换为配置类
