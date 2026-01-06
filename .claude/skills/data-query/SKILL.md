---
name: data-query
description: 这个技能用于根据用户的查询问题，从元数据存储中搜索相关信息，并基于元数据编写SQL进行查询
---

# Data Query
## 任务清单
1. 添加上下文信息
2.
    - LLM根据上下文扩展查询时可能需要的字段
    - LLM根据上下文扩展查询时可能需要的字段值
3.
    - 结合查询和关键词，检索相关知识
    - 结合关键词和扩展后的字段，检索相关字段
    - 结合关键词和扩展后的字段值，检索相关字段值
4. 合并字段与单元格信息，并根据检索分数截取topk表和字段
5.
    - LLM过滤掉不需要的表和字段
    - LLM过滤掉不需要的知识
6. 获取知识相关字段，并与之前检索出的字段合并
## 1. 添加上下文信息
执行 python 脚本 [scripts/add_context.py](scripts/add_context.py) 添加相关上下文信息
```bash
python scripts/add_context.py 查询文本
```
## 2. LLM根据上下文扩展查询时可能需要的字段 & LLM根据上下文扩展查询时可能需要的字段值
跳过

## 3. 结合查询和关键词，检索相关知识 & 结合关键词和扩展后的字段，检索相关字段 & 结合关键词和扩展后的字段值，检索相关字段值
执行 python 脚本 [scripts/recall_column.py](scripts/recall_column.py) 检索字段信息
执行 python 脚本 [scripts/recall_cell.py](scripts/recall_cell.py) 检索单元格信息
执行 python 脚本 [scripts/recall_knowledge.py](scripts/recall_knowledge.py) 检索知识信息
```bash
python scripts/recall_column.py
python scripts/recall_cell.py
python scripts/recall_knowledge.py
```
## 4. 合并字段与单元格信息，并根据检索分数截取topk表和字段
执行 python 脚本 [scripts/merge_col_cell.py](scripts/merge_col_cell.py) 合并并截取表与字段信息
```bash
python scripts/merge_col_cell.py
```
## 5. LLM过滤掉不需要的表和字段 & LLM过滤掉不需要的知识
跳过

## 6. 获取知识相关字段，并与之前检索出的字段合并
执行 python 脚本 [scripts/add_kn_col.py](scripts/add_kn_col.py) 获取知识相关字段并与先前字段信息合并
```bash
python scripts/add_kn_col.py
```
