async def filter_knowledge(state: TableRAGContext, runtime: Runtime[RuntimeContext]):
    """LLM筛选指标知识"""
    writer = get_stream_writer()  # 流式写入器，向客户端实时返回处理状态
    writer({"stage": "正在筛选指标知识"})

    kn_map = state.kn_map
    query = state.query
    filter_model = runtime.context.filter_model

    if not kn_map:
        logger.info("knowledge is empty")
        return {"kn_map": kn_map}

    try:
        prompt = get_prompt(
            "table_rag",
            "knowledge_filter_prompt",
            knowledge_info=kn_info_xml_str(kn_map),  # 指标知识信息
            query=query,  # 用户查询
        )
        logger.debug(f"knowledge_filter_prompt:\n{prompt['user']}")
    except Exception as e:
        logger.exception(f"build knowledge_filter_prompt error: {e}")
        raise
    try:
        # LLM过滤相关指标知识
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
        logger.exception(f"filter knowledge error: {e}")
        raise
    try:
        filtered_kn_codes = parse_json(resp)
        logger.info(f"filtered knowledge: {filtered_kn_codes}")
    except Exception as e:
        logger.warning(f"filter knowledge error, can't parse json: {resp}\n{e}")
        raise

    filtered_kn_codes = {kn_code for kn_code in filtered_kn_codes if kn_code in kn_map}
    add_kn_codes: set[int] = {
        rel_code
        for kn_code in filtered_kn_codes
        for rel_code in (kn_map[kn_code].rel_kn or [])
    }  # 补充可能在过滤中遗漏掉的关联知识
    return {"kn_map": {k: kn_map[k] for k in filtered_kn_codes | add_kn_codes}}
