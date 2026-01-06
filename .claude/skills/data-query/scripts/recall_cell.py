import argparse
import asyncio
from typing import Callable

import httpx
from callback import read_callback, write_callback
from config import CFG


async def recall_cell(
    db_code: str,
    r_callback: Callable | None = None,
    w_callback: Callable | None = None,
):
    """检索字段信息"""
    state = await r_callback() if r_callback else {}
    keywords: list[str] = state.get("extracted_cells") or state["keywords"]
    async with httpx.AsyncClient() as client:
        response = await client.post(
            CFG.meta_db.retrieve_cell_url,
            json={"db_code": db_code, "keywords": keywords},
        )
    if w_callback:
        await w_callback({"retrieved_cell_map": response.json()})


async def main():
    usage = "python recall_cell.py"
    argparse.ArgumentParser(description="检索单元格", usage=usage)

    await recall_cell(CFG.use_db_code, read_callback, write_callback)


if __name__ == "__main__":
    asyncio.run(main())
