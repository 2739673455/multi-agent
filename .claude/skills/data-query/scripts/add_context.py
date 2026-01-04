import asyncio
from datetime import datetime

import httpx
from config import CFG


async def add_context(db_code: str):
    """补充上下文信息：获取日期，表信息等"""

    # 获取当前日期对象
    today = datetime.today()
    # 格式化当前日期为 YYYY-MM-DD 格式
    today_date = today.strftime("%Y-%m-%d")
    # 获取当前星期的英文名称
    today_weekday = today.strftime("%A")
    # 构建日期信息字符串
    cur_date_info = "当前日期信息:%s,%s" % (today_date, today_weekday)

    # 获取所有表信息列表
    async with httpx.AsyncClient() as client:
        response = await client.post(
            CFG.meta_db.get_table_url,
            json={"db_code": db_code},
        )
    tables = response.json()
    # 为每个表构建格式化的说明信息，包括表名、表含义
    tb_caption = "".join(
        [
            f"表名: {tb_info['tb_name']}，表含义: {tb_info['tb_meaning']}\n"
            for tb_info in tables
        ]
    )
    # 添加数据库信息
    tb_info = tables[0]
    tb_caption = f"数据库: {tb_info['db_name']}\n" + tb_caption

    return {
        "cur_date_info": cur_date_info,  # 当前日期时间信息
        "tb_map": {i["tb_code"]: i for i in tables},  # 表信息字典
        "tb_caption": tb_caption,  # 所有表的格式化说明信息
    }


async def main():
    res = await add_context(CFG.use_db_code)
    print(res)


if __name__ == "__main__":
    asyncio.run(main())
