import argparse
import asyncio
from typing import Callable

import httpx
from callback import read_callback, write_callback
from config import CFG


async def add_kn_col(
    db_code: str,
    r_callback: Callable | None = None,
    w_callback: Callable | None = None,
):
    """获取知识相关字段，并与之前检索出的字段合并"""
    state = await r_callback() if r_callback else {}
    col_map: dict[str, dict[str, dict]] = state["col_map"]
    kn_map: dict[int, dict] = state["kn_map"]
    if not kn_map or all(not kn["rel_col"] for kn in kn_map.values()):
        return

    # 获取知识的相关字段
    tb_col_tuple_set: set[tuple[str, str]] = set()
    for kn in kn_map.values():
        if kn["rel_col"]:
            for tb_col in kn["rel_col"]:
                tb_name, col_name = tb_col.split(".")
                tb_col_tuple_set.add((tb_name, col_name))
    async with httpx.AsyncClient() as client:
        response = await client.post(
            CFG.meta_db.retrieve_cell_url,
            json={"db_code": db_code, "tb_col_tuple_list": list(tb_col_tuple_set)},
        )
        kn_rel_col_map = response.json()

    # 将知识的相关字段与召回的字段合并
    for tb_code, n_c_map in kn_rel_col_map.items():
        col_map.setdefault(tb_code, {}).update(n_c_map)

    if w_callback:
        await w_callback(col_map)


async def main():
    usage = "python add_kn_col.py"
    argparse.ArgumentParser(
        description="获取知识相关字段，并与之前检索出的字段合并", usage=usage
    )

    await add_kn_col(CFG.use_db_code, read_callback, write_callback)


if __name__ == "__main__":
    asyncio.run(main())
