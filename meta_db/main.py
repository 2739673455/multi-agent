from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from query_meta import (
    get_col_by_dbcode_tbname_colname,
    get_tb_info_by_dbcode,
    retrieve_cell,
    retrieve_column,
    retrieve_knowledge,
)
from save_meta import clear_meta, save_meta


class SaveMetaRequest(BaseModel):
    save: dict[str, dict[str, list[str] | None] | None] = Field(
        description="数据库元数据保存配置",
        examples=[
            {
                "db_code": {
                    "table": ["tb_code"],
                    "knowledge": ["kn_code"],
                    "cell": ["tb_code"],
                }
            }
        ],
    )


metadata_router = APIRouter()


@metadata_router.get("/health")
async def health():
    return 200


@metadata_router.post("/save_metadata")
async def api_save_meta(save_meta_request: SaveMetaRequest):
    await save_meta(save_meta_request.save)


@metadata_router.post("/clear_metadata")
async def api_clear_meta():
    await clear_meta()


@metadata_router.post("/get_table")
async def api_get_table(db_code: str):
    return await get_tb_info_by_dbcode(db_code)


@metadata_router.post("/get_column")
async def api_get_column(db_code: str, tb_col_tuple_list: list[tuple[str, str]]):
    return await get_col_by_dbcode_tbname_colname(db_code, tb_col_tuple_list)


@metadata_router.post("retrieve_knowledge")
async def api_retrieve_knowledge(db_code: str, query: str, keywords: list[str]):
    return await retrieve_knowledge(db_code, query, keywords)


@metadata_router.post("retrieve_column")
async def api_retrieve_column(db_code: str, texts: list[str]):
    return await retrieve_column(db_code, texts)


@metadata_router.post("retrieve_cell")
async def api_retrieve_cell(db_code: str, texts: list[str]):
    return await retrieve_cell(db_code, texts)


api_router = APIRouter(prefix="/api")
api_router.include_router(metadata_router, prefix="/metadata")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 服务启动时的初始化操作

    # 应用运行期间
    yield

    # 服务关闭时的清理操作


# 创建 FastAPI 应用
app = FastAPI(lifespan=lifespan)

# 添加 CORS(Cross-Origin Resource Sharing，跨域资源共享) 中间件，允许前端应用从不同域名访问API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源，生产环境应该指定具体域名
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有头部
)

app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
