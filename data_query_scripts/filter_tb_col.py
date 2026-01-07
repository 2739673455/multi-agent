async def filter_tb_col(state: TableRAGContext, runtime: Runtime[RuntimeContext]):
    """LLM筛选与查询相关的表和字段，进一步精简上下文信息"""

    async def filter_table(tb_code_list: list[str]) -> list[str]:
        """使用LLM过滤表，返回与查询相关的表编号列表"""
        async with table_filter_semaphore:
            try:
                _col_map = {tb_code: col_map[tb_code] for tb_code in tb_code_list}
                prompt = get_prompt(
                    "table_rag",
                    "table_filter_prompt",
                    time_info=cur_date_info,  # 时间信息
                    table_info=tb_info_xml_str(tb_map, _col_map),  # 表字段信息
                    query=query,  # 用户查询
                )
                logger.debug(f"table_filter_prompt:\n{prompt['user']}")
            except Exception as e:
                logger.exception(f"build table_filter_prompt error: {e}")
                raise
            try:
                resp = await ask_llm(
                    filter_model,
                    [
                        {"role": "system", "content": prompt["system"]},
                        {"role": "user", "content": prompt["user"]},
                    ],
                    1,
                    20,
                )
            except Exception as e:
                logger.exception(f"filter table error: {e}")
                raise
            try:
                filtered_tb_codes = parse_json(resp)
                logger.info(f"filtered table: {filtered_tb_codes}")
                return filtered_tb_codes
            except Exception as e:
                logger.exception(f"filter table error, can't parse json: {resp}\n{e}")
                raise

    async def filter_column(tb_code: str) -> tuple | None:
        """使用LLM过滤字段，返回单表所有和查询相关的字段"""
        async with column_filter_semaphore:
            try:
                _tb_map = {tb_code: tb_map[tb_code]}
                _col_map = {tb_code: col_map[tb_code]}
                prompt = get_prompt(
                    "table_rag",
                    "column_filter_prompt",
                    query=query,  # 用户查询
                    time_info=cur_date_info,  # 时间信息
                    table_info=tb_info_xml_str(_tb_map, _col_map),  # 字段信息
                )
                logger.debug(f"column_filter_prompt:\n{prompt['user']}")
            except Exception as e:
                logger.exception(f"build column_filter_prompt error: {e}")
                raise
            try:
                # LLM过滤字段
                resp = await ask_llm(
                    filter_model,
                    [
                        {"role": "system", "content": prompt["system"]},
                        {"role": "user", "content": prompt["user"]},
                    ],
                    1,
                    20,
                )
            except Exception as e:
                logger.exception(f"filter column error: {e}")
                raise
            try:
                res = parse_json(resp)
            except Exception as e:
                logger.error(f"filter column error, can't parse json: {resp}\n{e}")
                raise

            # 预期的返回格式：
            # {
            #     "related_flag": true,  # 表是否相关
            #     "column_names": [col_name1, col_name2] # 相关字段列表
            # }
            if "related_flag" not in res or "column_names" not in res:
                # 如果返回格式不合法，原样返回表字段信息
                logger.warning(
                    "filter column error, related_flag or column_names not in response"
                )
                return tb_code, col_map[tb_code]

            # 如果表不相关或没有选择任何字段，返回None
            if not res["related_flag"] or not res["column_names"]:
                return None

            # 字段筛选，根据LLM返回的字段名列表，筛选出相关字段
            filtered_col_dict = {
                col_name: col_map[tb_code][col_name]
                for col_name in res["column_names"]
                if col_name in col_map[tb_code]
            }

            logger.info(f"filtered column: {tb_code} {list(filtered_col_dict.keys())}")
            return tb_code, filtered_col_dict

    writer = get_stream_writer()  # 流式写入器，向客户端实时返回处理状态
    writer({"stage": "正在过滤表/字段信息"})

    query = state.query
    cur_date_info = state.cur_date_info
    tb_map = state.tb_map
    col_map = state.col_map
    filter_model = runtime.context.filter_model

    # 并发控制配置
    max_concurrent = 20  # 最大并发数，防止系统过载
    table_filter_semaphore = asyncio.Semaphore(max_concurrent)  # 使用信号量限制并发数
    column_filter_semaphore = asyncio.Semaphore(max_concurrent)  # 使用信号量限制并发数
    table_filter_batch_size = 5  # 每批次过滤的表数量，平衡处理效率和LLM输入长度

    # 第一阶段：过滤表
    _tb_code_list = list(col_map.keys())
    table_filter_tasks = [
        filter_table(_tb_code_list[i : i + table_filter_batch_size])  # 分批处理
        for i in range(0, len(col_map), table_filter_batch_size)
    ]  # 创建表过滤任务

    # 第二阶段：过滤字段
    column_filter_tasks = []
    for coro in asyncio.as_completed(table_filter_tasks):  # as_completed 流水线处理
        filtered_table_codes = await coro  # 等待表过滤任务完成
        # 跳过空的过滤结果
        if not filtered_table_codes:
            continue
        # 为每个过滤出的表创建字段过滤任务
        for tb_code in filtered_table_codes:
            if tb_code in col_map:
                column_filter_tasks.append(asyncio.create_task(filter_column(tb_code)))

    # 等待所有字段过滤任务完成，构建最终结果
    res: list[tuple | None] = await asyncio.gather(*column_filter_tasks)
    filtered_col_map: dict[str, dict[str, Column]] = dict(filter(None, res))
    return {"col_map": filtered_col_map}
