import re


async def retrieve_knowledge(db_code: str, query: str, keywords: list[str]):
    """混合检索知识"""
    # 子句切分,支持中英文标点
    sub_statements = re.split(r"[，。！？；,;!?\s]+", query)
    # 去掉太短的子句
    sub_statements = [s.strip() for s in sub_statements if len(s.strip()) >= 3]
    embeds = await embed(sub_statements)

    tsquery = " OR ".join(keywords)

    cypher = """
        CALL () {
            // 子句向量检索
            UNWIND $embeds AS vec
            CALL (vec) {
                CALL db.index.vector.queryNodes('embed_kn_embed', $search_num_per_vec, vec)
                YIELD node AS e_node, score AS v_score
                WHERE v_score > $vec_search_threshold

                // 关联到 KNOWLEDGE 并过滤库
                MATCH (e_node)-[:BELONG]->(kn:KNOWLEDGE)-[:BELONG]->(db:DATABASE {db_code: $db_code})
                RETURN kn, v_score, 0.0 AS f_score, 1 AS type
            }
            RETURN kn, v_score, f_score, type

            UNION ALL

            // 全文检索
            CALL db.index.fulltext.queryNodes('embed_kn_tscontent', $tsquery, {limit: $search_num_per_ft})
            YIELD node AS e_node, score AS f_score

            // 关联到 KNOWLEDGE 并过滤库
            MATCH (e_node)-[:BELONG]->(kn:KNOWLEDGE)-[:BELONG]->(db:DATABASE {db_code: $db_code})
            RETURN kn, 0.0 AS v_score, f_score, 2 AS type
        }

        // 取同一个 KNOWLEDGE 向量和全文的各自最高分
        WITH kn, max(v_score) AS mv, max(f_score) AS mf
        WITH collect({n: kn, v: mv, f: mf}) AS raw_data

        // 构造有序的 v_list (向量排名列表)
        UNWIND raw_data AS row
        WITH row, raw_data WHERE row.v > 0 
        ORDER BY row.v DESC
        WITH collect({node: row.n, score: row.v}) AS v_list, raw_data

        // 构造有序的 f_list (全文排名列表)
        UNWIND raw_data AS row
        WITH row, v_list WHERE row.f > 0 
        ORDER BY row.f DESC
        WITH collect({node: row.n, score: row.f}) AS f_list, v_list

        // RRF 融合重排序
        // 合并两个列表中的所有唯一节点
        WITH [x IN v_list | x.node] + [x IN f_list | x.node] AS all_nodes, v_list, f_list
        UNWIND all_nodes AS kn_node
        WITH DISTINCT kn_node, v_list, f_list

        // 分别计算向量排名分和全文排名
        WITH kn_node,
            [i IN range(0, size(v_list)-1) WHERE v_list[i].node = kn_node][0] AS v_rank,
            [i IN range(0, size(f_list)-1) WHERE f_list[i].node = kn_node][0] AS f_rank

        // 计算最终 RRF 得分：1/(60+rank)
        WITH kn_node,
            (CASE WHEN v_rank IS NOT NULL THEN 1.0/(60 + v_rank) ELSE 0 END) +
            (CASE WHEN f_rank IS NOT NULL THEN 1.0/(60 + f_rank) ELSE 0 END) AS rrf_score

        // 取 topk
        ORDER BY rrf_score DESC
        LIMIT $final_num

        // 递归查找子知识，从深度0开始
        OPTIONAL MATCH (kn_node)-[:CONTAIN*0..]->(kn:KNOWLEDGE)

        // 展平、去重并按 kn_code 排序
        WITH DISTINCT kn
        RETURN kn
        ORDER BY kn.kn_code ASC
    """

    results = await driver.execute_query(
        cypher,
        embeds=embeds,
        tsquery=tsquery,
        db_code=db_code,
        vec_search_threshold=0.7,
        search_num_per_vec=10,  # 每个子句向量检索的个数
        search_num_per_ft=20,  # 全文检索的个数
        final_num=5,  # 最终返回的个数
    )
    records = results.records
    kn_map = {
        record["kn"]["kn_code"]: Knowledge(**dict(record["kn"])) for record in records
    }
    return kn_map
