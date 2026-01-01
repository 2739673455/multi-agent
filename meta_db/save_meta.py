import asyncio
import json
from typing import LiteralString, cast

import jieba.analyse
from config import DB_CONF
from db_session import get_session, neo4j_session
from loguru import logger
from neo4j import AsyncSession
from sqlalchemy import column, select, table
from util import embed


def is_numeric(s) -> bool:
    """判断字符串是否为数值"""
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


async def get_keywords(texts: list[str]) -> list[list[str]]:
    """获取关键词"""

    def sync_jieba() -> list[list[str]]:
        allow_pos = (
            "n",  # 名词: 数据、服务器、表格
            "nr",  # 人名: 张三、李四
            "ns",  # 地名: 北京、上海
            "nt",  # 机构团体名: 政府、学校、某公司
            "nz",  # 其他专有名词: Unicode、哈希算法、诺贝尔奖
            "v",  # 动词: 运行、开发
            "vn",  # 名动词: 工作、研究
            "a",  # 形容词: 美丽、快速
            "an",  # 名形词: 难度、合法性、复杂度
            "eng",  # 英文
            "i",  # 成语
            "l",  # 常用固定短语
        )
        return [
            jieba.analyse.extract_tags(t, withWeight=False, allowPOS=allow_pos)
            for t in texts
        ]

    return await asyncio.to_thread(sync_jieba)


async def save_meta(
    tb_info_list: list[dict],
    tb_col_list: list[dict],
    kn_list: list[dict],
):
    """保存元数据"""
    async with neo4j_session() as session:
        try:
            # 创建约束
            await create_constraints(session)
        except Exception as e:
            logger.exception(f"create constraints error: {e}")
            raise

        try:
            # 创建索引
            await create_indexes(session)
        except Exception as e:
            logger.exception(f"create indexes error: {e}")
            raise

        try:
            # 保存表信息
            await save_tb_info(session, tb_info_list)
        except Exception as e:
            logger.exception(f"save table info error: {e}")
            raise

        try:
            # 保存表字段信息
            await save_tb_column(session, tb_col_list)
        except Exception as e:
            logger.exception(f"save table column error: {e}")
            raise

        try:
            # 保存知识
            await save_knowledge(session, kn_list)
        except Exception as e:
            logger.exception(f"save knowledge error: {e}")
            raise

        try:
            # 保存字段向量化信息
            await save_col_embed(session, tb_col_list)
        except Exception as e:
            logger.exception(f"save column embedding error: {e}")
            raise

        try:
            # 保存知识向量化信息
            await save_kn_embed(session, kn_list)
        except Exception as e:
            logger.exception(f"save knowledge embedding error: {e}")
            raise

        try:
            tb_code2tb_info_map = {i["tb_code"]: i for i in tb_info_list}
            tb_code2cols_map: dict[str, list[dict]] = {}
            for col in tb_col_list:
                if (
                    (any(i in col["col_type"].lower() for i in ["varchar", "text"]))
                    and (col["tb_code"] in tb_code2tb_info_map)
                    and (
                        tb_code2tb_info_map[col["tb_code"]].get("sync_col") is None
                        or col["col_name"]
                        in tb_code2tb_info_map[col["tb_code"]]["sync_col"]
                    )  # 如果同步字段为空，则所有字段都需要同步；如果字段在同步字段里则同步
                    and (
                        tb_code2tb_info_map[col["tb_code"]].get("no_sync_col") is None
                        or col["col_name"]
                        not in tb_code2tb_info_map[col["tb_code"]]["no_sync_col"]
                    )  # 如果字段在同步字段里，且不在不同步字段里，则同步
                ):
                    tb_code2cols_map.setdefault(col["tb_code"], []).append(col)
            # 写入字段值信息
            await save_cell(session, tb_code2tb_info_map, tb_code2cols_map)
        except Exception as e:
            logger.exception(f"save cell error: {e}")
            raise


async def create_constraints(session: AsyncSession):
    """创建约束"""
    # DATABASE db_code 唯一约束
    await session.run(
        "CREATE CONSTRAINT database_db_code IF NOT EXISTS FOR (d:DATABASE) REQUIRE d.db_code IS UNIQUE"
    )
    # TABLE tb_code 唯一约束
    await session.run(
        "CREATE CONSTRAINT table_tb_code IF NOT EXISTS FOR (t:TABLE) REQUIRE t.tb_code IS UNIQUE"
    )
    # COLUMN (tb_code, col_name) 唯一约束
    await session.run(
        "CREATE CONSTRAINT column_tb_code_col_name IF NOT EXISTS FOR (c:COLUMN) REQUIRE (c.tb_code, c.col_name) IS UNIQUE"
    )
    # KNOWLEDGE (db_code, kn_code) 唯一约束
    await session.run(
        "CREATE CONSTRAINT knowledge_db_code_kn_code IF NOT EXISTS FOR (k:KNOWLEDGE) REQUIRE (k.db_code, k.kn_code) IS UNIQUE"
    )
    # EMBED_COL (content) 唯一约束
    await session.run(
        "CREATE CONSTRAINT embed_col IF NOT EXISTS FOR (e:EMBED_COL) REQUIRE (e.content) IS UNIQUE"
    )
    # EMBED_KN (content) 唯一约束
    await session.run(
        "CREATE CONSTRAINT embed_kn IF NOT EXISTS FOR (e:EMBED_KN) REQUIRE (e.content) IS UNIQUE"
    )
    # CELL (content) 唯一约束
    await session.run(
        "CREATE CONSTRAINT cell IF NOT EXISTS FOR (c:CELL) REQUIRE (c.content) IS UNIQUE"
    )
    logger.info("create constraints")


async def create_indexes(session):
    """创建索引"""
    # EMBED_COL embed 向量索引
    await session.run(
        """
        CREATE VECTOR INDEX embed_col_embed IF NOT EXISTS FOR (e:EMBED_COL) ON e.embed
        OPTIONS { indexConfig: {`vector.dimensions`: 1024,`vector.similarity_function`: 'cosine'} }
        """
    )
    # EMBED_KN embed 向量索引
    await session.run(
        """
        CREATE VECTOR INDEX embed_kn_embed IF NOT EXISTS FOR (e:EMBED_KN) ON e.embed
        OPTIONS { indexConfig: {`vector.dimensions`: 1024,`vector.similarity_function`: 'cosine'} }
        """
    )
    # EMBED_KN tscontent 全文索引
    await session.run(
        "CREATE FULLTEXT INDEX embed_kn_tscontent IF NOT EXISTS FOR (e:EMBED_KN) ON EACH [e.tscontent]"
    )
    # CELL embed 向量索引
    await session.run(
        """
        CREATE VECTOR INDEX cell_embed IF NOT EXISTS FOR (c:CELL) ON c.embed
        OPTIONS { indexConfig: {`vector.dimensions`: 1024,`vector.similarity_function`: 'cosine'} }
        """
    )
    # CELL tscontent 全文索引
    await session.run(
        "CREATE FULLTEXT INDEX cell_tscontent IF NOT EXISTS FOR (c:CELL) ON EACH [c.tscontent]"
    )
    logger.info("create indexes")


async def save_tb_info(session: AsyncSession, tb_info_list: list[dict]):
    """写入表信息"""
    if not tb_info_list:
        return

    db_dict: dict[str, dict] = {}
    tb_dict: dict[str, dict] = {}
    tb_db_rel: set[tuple[str, str]] = set()  # (tb_code, db_code)

    for tb_info in tb_info_list:
        # 收集数据库信息
        db_dict[tb_info["db_code"]] = {
            "db_code": tb_info["db_code"],
            "db_name": tb_info["db_name"],
            "db_type": tb_info["db_type"],
            "database": tb_info["database"],
        }
        # 收集表信息
        tb_dict[tb_info["tb_code"]] = {
            "tb_code": tb_info["tb_code"],
            "tb_name": tb_info["tb_name"],
            "tb_meaning": tb_info["tb_meaning"],
        }
        # 收集表与库的关系
        tb_db_rel.add((tb_info["db_code"], tb_info["tb_code"]))

    # 创建 DATABASE 节点
    await session.run(
        """
        UNWIND $dbs AS db
        MERGE (n:DATABASE {db_code: db.db_code})
        SET n += db
        """,
        dbs=list(db_dict.values()),
    )
    logger.info(f"save database ({len(db_dict)})")

    # 创建 TABLE 节点
    await session.run(
        """
        UNWIND $tbs AS tb
        MERGE (n:TABLE {tb_code: tb.tb_code})
        SET n += tb
        """,
        tbs=list(tb_dict.values()),
    )
    logger.info(f"save table ({len(tb_dict)})")

    # 创建 TABLE-[:BELONG]->DATABASE 关系
    await session.run(
        """
        UNWIND $tb_db_rel AS rel
        MATCH (db:DATABASE {db_code: rel[0]})
        MATCH (tb:TABLE {tb_code: rel[1]})
        MERGE (tb)-[:BELONG]->(db)
        """,
        tb_db_rel=list(tb_db_rel),
    )
    logger.info(f"save table-belong->database ({len(tb_db_rel)})")


async def save_tb_column(session: AsyncSession, tb_col_list: list[dict]):
    """写入表字段信息"""
    if not tb_col_list:
        return

    col_tb_rel: set[tuple[str, str]] = set()  # (tb_code, col_name)
    col_col_rel: set[tuple[str, str, str, str]] = (
        set()
    )  # (tb_code, col_name, rel_tb_code, rel_col_name)

    for col in tb_col_list:
        col["field_meaning"] = (
            json.dumps(col["field_meaning"], ensure_ascii=False)
            if col["field_meaning"]
            else None
        )
        # 收集表与字段的关系
        col_tb_rel.add((col["tb_code"], col["col_name"]))
        # 收集字段与字段的关系
        if col.get("rel_col"):
            rel_tb_name, rel_col_name = col["rel_col"].split(".")
            col_col_rel.add(
                (col["tb_code"], col["col_name"], rel_tb_name, rel_col_name)
            )

    # 创建 COLUMN 节点
    await session.run(
        """
        UNWIND $cols AS col
        MERGE (n:COLUMN {tb_code: col.tb_code, col_name: col.col_name})
        SET n += col
        """,
        cols=tb_col_list,
    )
    logger.info(f"save column ({len(tb_col_list)})")

    # 创建 COLUMN-[:BELONG]->TABLE 关系
    await session.run(
        """
        UNWIND $col_tb_rel AS rel
        MATCH (tb:TABLE {tb_code: rel[0]})
        MATCH (col:COLUMN {tb_code: rel[0], col_name: rel[1]})
        MERGE (col)-[:BELONG]->(tb)
        """,
        col_tb_rel=list(col_tb_rel),
    )
    logger.info(f"save column-belong->tb ({len(col_tb_rel)})")

    # 创建 COLUMN-[:REL]->COLUMN 关系
    if col_col_rel:
        await session.run(
            """
            UNWIND $col_col_rel AS rel
            MATCH (col:COLUMN {tb_code: rel[0], col_name: rel[1]})-[]-(:TABLE)-[]-(:DATABASE)-[]-(:TABLE {tb_name: rel[2]})-[]-(rel_col:COLUMN {col_name: rel[3]})
            MERGE (col)-[:REL]->(rel_col)
            """,
            col_col_rel=list(col_col_rel),
        )
    logger.info(f"save column-rel->column ({len(col_col_rel)})")


async def save_knowledge(session: AsyncSession, kn_list: list[dict]):
    """写入知识信息"""
    if not kn_list:
        return

    kn_db_rel: set[tuple[str, int]] = set()  # (db_code, kn_code)
    kn_kn_rel: set[tuple[str, int, int]] = set()  # (db_code, kn_code, rel_kn_code)
    kn_col_rel: set[tuple[str, int, str, str]] = (
        set()
    )  # (db_code, kn_code, tb_name, col_name)

    for kn in kn_list:
        # 收集知识与库的关系
        kn_db_rel.add((kn["db_code"], kn["kn_code"]))
        # 收集知识与知识的关系
        if kn["rel_kn"]:
            for rel_kn_code in kn["rel_kn"]:
                kn_kn_rel.add((kn["db_code"], kn["kn_code"], rel_kn_code))
        # 收集知识与字段的关系
        if kn["rel_col"]:
            for rel_col in kn["rel_col"]:
                rel_tb_name, rel_col_name = rel_col.split(".")
                kn_col_rel.add(
                    (kn["db_code"], kn["kn_code"], rel_tb_name, rel_col_name)
                )

    # 创建 KNOWLEDGE 节点
    await session.run(
        """
        UNWIND $kns AS kn
        MERGE (n:KNOWLEDGE {db_code: kn.db_code, kn_code: kn.kn_code})
        SET n += kn
        """,
        kns=kn_list,
    )
    logger.info(f"save knowledge ({len(kn_list)})")

    # 创建 KNOWLEDGE-[:BELONG]->DATABASE 关系
    await session.run(
        """
        UNWIND $kn_db_rel AS rel
        MATCH (db:DATABASE {db_code: rel[0]})
        MATCH (kn:KNOWLEDGE {db_code: rel[0], kn_code: rel[1]})
        MERGE (kn)-[:BELONG]->(db)
        """,
        kn_db_rel=list(kn_db_rel),
    )
    logger.info(f"save knowledge-belong->database ({len(kn_list)})")

    # 创建 KNOWLEDGE-[:CONTAIN]->KNOWLEDGE 关系
    if kn_kn_rel:
        await session.run(
            """
            UNWIND $kn_kn_rel AS rel
            MATCH (kn:KNOWLEDGE {db_code: rel[0], kn_code: rel[1]})
            MATCH (rel_kn:KNOWLEDGE {db_code: rel[0], kn_code: rel[2]})
            MERGE (kn)-[:CONTAIN]->(rel_kn)
            """,
            kn_kn_rel=list(kn_kn_rel),
        )
    logger.info(f"save knowledge-contain->knowledge ({len(kn_kn_rel)})")

    # 创建 KNOWLEDGE-[:REL]->COLUMN 关系
    if kn_col_rel:
        await session.run(
            """
            UNWIND $kn_col_rel AS rel
            MATCH (kn:KNOWLEDGE {db_code: rel[0], kn_code: rel[1]})-[]-(:DATABASE)-[]-(:TABLE)-[]-(rel_col:COLUMN {tb_name: rel[2], col_name: rel[3]})
            MERGE (kn)-[:REL]->(rel_col)
            """,
            kn_col_rel=list(kn_col_rel),
        )
    logger.info(f"save knowledge-rel->column ({len(kn_col_rel)})")


async def save_col_embed(session: AsyncSession, tb_col_list: list[dict]):
    """写入表字段向量化信息"""
    if not tb_col_list:
        return

    def flatten_dict_values(d: dict) -> list:
        """递归展平嵌套字典，获取所有叶子节点的值"""
        return sum(
            [
                flatten_dict_values(v) if isinstance(v, dict) else [v]
                for v in d.values()
            ],
            [],
        )

    EMBED_BATCH_SIZE = 128

    col_list: list[dict] = []
    for col in tb_col_list:
        col_dict = {"tb_code": col["tb_code"], "col_name": col["col_name"]}
        # 收集字段名
        col_list.append({**col_dict, "content": col["col_name"], "col": "col_name"})
        # 收集字段注释
        if col["col_comment"] is not None:
            col_list.append(
                {**col_dict, "content": col["col_comment"], "col": "col_comment"}
            )
        # 收集字段示例
        if col["fewshot"] is not None:
            col_list.extend(
                [
                    {**col_dict, "content": i, "col": "fewshot"}
                    for i in col["fewshot"]
                    if not is_numeric(i)
                ]
            )
        # 收集字段含义
        if col["col_meaning"] is not None:
            col_list.append(
                {**col_dict, "content": col["col_meaning"], "col": "col_meaning"}
            )
        # 收集字段JSON字段含义（展平嵌套字典的所有值）
        if col["field_meaning"] is not None:
            col_list.extend(
                [
                    {**col_dict, "content": i, "col": "field_meaning"}
                    for i in flatten_dict_values(json.loads(col["field_meaning"]))
                ]
            )
        # 收集字段别名
        if col["col_alias"] is not None:
            col_list.extend(
                [
                    {**col_dict, "content": i, "col": "col_alias"}
                    for i in col["col_alias"]
                ]
            )

    # 向量化
    tasks = []
    for i in range(0, len(col_list), EMBED_BATCH_SIZE):
        batch = col_list[i : i + EMBED_BATCH_SIZE]
        tasks.append(embed([item["content"] for item in batch]))
    embeds = await asyncio.gather(*tasks)
    flatten_embeds = [vec for batch in embeds for vec in batch]
    for col_dict, vec in zip(col_list, flatten_embeds):
        col_dict["embed"] = vec
    logger.info(f"embed column {len(col_list)}")

    # 创建 EMBED_COL 节点，创建 EMBED_COL-[:BELONG]->COLUMN 关系
    await session.run(
        """
        UNWIND $col_list AS col_item
        MERGE (ec:EMBED_COL {content: col_item.content})
        ON CREATE SET ec.embed = col_item.embed
        WITH ec, col_item
        MATCH (col:COLUMN {tb_code: col_item.tb_code, col_name: col_item.col_name})
        MERGE (ec)-[:BELONG]->(col)
        """,
        col_list=col_list,
    )
    logger.info(f"save embed_col ({len(col_list)})")
    logger.info(f"save embed_col_name->belong->column ({len(col_list)})")


async def save_kn_embed(session: AsyncSession, kn_list: list[dict]):
    """写入知识向量化信息"""
    if not kn_list:
        return

    async def handle_batch(batch: list[dict]):
        """处理批次"""
        async with semaphore:
            try:
                content_batch = [i["content"] for i in batch]
                embed_batch, tscontent_batch = await asyncio.gather(
                    embed(content_batch), get_keywords(content_batch)
                )
                logger.info(f"process knowledge {len(batch)}")
                for i, e, t in zip(batch, embed_batch, tscontent_batch):
                    i["embed"] = e
                    i["tscontent"] = t
                return batch
            except Exception as e:
                logger.exception(f"process knowledge error: {e}")
                return []

    semaphore = asyncio.Semaphore(5)
    kn_dict_list: list[dict] = []
    for kn in kn_list:
        kn_dict = {"db_code": kn["db_code"], "kn_code": kn["kn_code"]}
        # 收集知识名称
        kn_dict_list.append({**kn_dict, "content": kn["kn_name"], "col": "kn_name"})
        # 收集知识描述
        kn_dict_list.append({**kn_dict, "content": kn["kn_desc"], "col": "kn_desc"})
        # 收集知识别名
        if kn["kn_alias"] is not None:
            kn_dict_list.extend(
                [{**kn_dict, "content": i, "col": "kn_alias"} for i in kn["kn_alias"]]
            )

    # 嵌入与分词
    handled_batch = await handle_batch(kn_dict_list)

    # 创建 EMBED_KN 节点，创建 EMBED_KN-[:BELONG]->KNOWLEDGE 关系
    await session.run(
        """
        UNWIND $kn_list AS kn_item
        MERGE (ek:EMBED_KN {content: kn_item.content})
        ON CREATE SET ek.embed = kn_item.embed, ek.tscontent = kn_item.tscontent
        WITH ek, kn_item
        MATCH (kn:KNOWLEDGE {db_code: kn_item.db_code, kn_code: kn_item.kn_code})
        MERGE (ek)-[:BELONG]->(kn)
        """,
        kn_list=handled_batch,
    )
    logger.info(f"save embed_kn ({len(handled_batch)})")
    logger.info(f"save embed_kn_name->belong->kn ({len(handled_batch)})")


async def save_cell(
    session: AsyncSession,
    tb_code2tb_info_map: dict[str, dict],
    tb_code2sync_cols_map: dict[str, list[dict]],
):
    """写入单元格信息"""

    async def handle_batch(batch: list[dict]):
        """处理批次"""
        async with semaphore:
            if not batch:
                return []
            try:
                cell_batch = [i["content"] for i in batch]
                embed_batch, tscontent_batch = await asyncio.gather(
                    embed(cell_batch), get_keywords(cell_batch)
                )
                for i, e, t in zip(batch, embed_batch, tscontent_batch):
                    i["embed"] = e
                    i["tscontent"] = t
                return batch
            except Exception as e:
                logger.exception(f"process {tb_code} cell error: {e}")
                return []

    async def save_to_neo4j(batch: list[dict]):
        """写入 neo4j"""
        # 创建 CELL 节点，创建 CELL-[:BELONG]->COLUMN 关系
        await session.run(
            """
            UNWIND $batch AS cell
            MERGE (c:CELL {content: cell.content})
            ON CREATE SET c.embed = cell.embed, c.tscontent = cell.tscontent
            WITH c, cell
            MATCH (col:COLUMN {tb_code: cell.tb_code, col_name: cell.col_name})
            MERGE (c)-[:BELONG]->(col)
            """,
            batch=batch,
        )
        logger.info(f"save cell ({len(batch)})")
        logger.info(f"save cell-belong->column ({len(batch)})")

    semaphore = asyncio.Semaphore(20)
    SELECT_BATCH_SIZE = 5000
    PROCESS_BATCH_SIZE = 128
    for tb_code, tb_info in tb_code2tb_info_map.items():
        tb_col_list = tb_code2sync_cols_map.get(tb_code)
        if not tb_col_list:
            continue
        col_name_list = [i["col_name"] for i in tb_col_list]
        async with get_session(DB_CONF[tb_info["db_code"]]) as db_session:
            stmt = select(*[column(c) for c in col_name_list]).select_from(
                table(tb_info["tb_name"])
            )
            logger.info(f"execute sql statement: {stmt}")
            result = await db_session.stream(
                stmt.execution_options(yield_per=SELECT_BATCH_SIZE)
            )
            async for batch in result.partitions(SELECT_BATCH_SIZE):
                # 构造 col_name->[cell] 映射
                # 解包->转置->转换为集合->关联列名->转换为字典
                col_map = dict(zip(col_name_list, map(set, zip(*batch))))
                tasks = []
                _batch: list[dict] = []
                for col_name, cells in col_map.items():
                    for i in cells:
                        if (i) and (i.strip() != "") and (not is_numeric(i)):
                            _batch.append(
                                {
                                    "tb_code": tb_code,
                                    "col_name": col_name,
                                    "content": i,
                                }
                            )
                            if len(_batch) >= PROCESS_BATCH_SIZE:
                                tasks.append(handle_batch(_batch))
                                _batch = []
                if _batch:
                    tasks.append(handle_batch(_batch))
                logger.info(f"process {tb_code} cell")
                handled_batch = await asyncio.gather(*tasks)
                flatten_handled_batch = [i for ls in handled_batch for i in ls]
                await save_to_neo4j(flatten_handled_batch)


async def clear_neo4j():
    """清空 neo4j"""
    async with neo4j_session() as session:
        # 删除所有数据
        await session.run("MATCH (n) DETACH DELETE n")

        # 删除所有约束
        result = await session.run("SHOW CONSTRAINTS")
        records = await result.data()
        for record in records:
            query_str = f"DROP CONSTRAINT {record['name']}"
            await session.run(cast(LiteralString, query_str))

        # 删除所有索引
        result = await session.run("SHOW INDEXES")
        records = await result.data()
        for record in records:
            query_str = f"DROP INDEX {record['name']}"
            try:
                await session.run(cast(LiteralString, query_str))
            except Exception:
                ...
    logger.info("clear neo4j")


if __name__ == "__main__":
    from load_meta import load_meta

    async def main():
        await clear_neo4j()
        tb_info_list, tb_col_list, kn_list = await load_meta()
        await save_meta(tb_info_list, tb_col_list, kn_list)

    asyncio.run(main())
