---
license: cc-by-4.0
configs:
- config_name: livesqlbench
  data_files:
  - path: livesqlbench_data.jsonl
    split: dev
viewer: true
tags:
- text-to-sql
- database
---
# 🚀 LiveSQLBench-Base-Full-v1
*A dynamic, **contamination‑free** benchmark for evaluating LLMs on complex, real‑world ****text‑to‑SQL**** tasks.*

[🌐 Website/Leaderboard](https://livesqlbench.ai) • [📄 Paper (coming soon)](https://arxiv.org) • [💻 GitHub](https://github.com/bird-bench/livesqlbench) • [🗄️ LiveSQLBench-Base-Lite](https://huggingface.co/datasets/birdsql/livesqlbench-base-lite) 

Maintained by the **🦜 [BIRD Team @ HKU](https://bird-bench.github.io)** & **☁️ [Google Cloud](https://cloud.google.com/)**


## 📊 LiveSQLBench Overview

**LiveSQLBench** (BIRD-SQL Pro v0.5) is a **contamination-free**, **continuously evolving** benchmark designed to evaluate LLMs on **complex, real-world text-to-SQL tasks**, featuring **diverse real-world user queries**, including **Business Intelligence (BI)**, **CRUD operations**, and more. Each release will include **around 20 new, fully open-source DBs** curated by the BIRD team through expert collaboration and continuous improvement. It will cover a **wide range of database sizes**, from **end-user level** (around 127 columns) to **industrial level** (1340+ columns). Here are the features of the LiveSQLBench benchmark:

1. **🗄️ Live Databases:**
Constructed dynamically from extensive and regularly updated CSV datasets, with both base (user-end level) and large (industrial level) versions (1340+ columns each DB) to test scalability.

2. **💬 Live User Queries and SQL:**
Each task pairs unambiguous user queries with annotated, gold-standard SQL statements. The user queries are grounded in an external knowledge base, with medium to hard complexity solution SQL statements.

3. **🧠 Contextual Reasoning (HKB):**
Every DB includes a hierarchical knowledge base (HKB) where each knowledge may have dependencies to others, which requires the multi-hop reasoning ability. Two HKB formats are provided: (1) structured JSON format, and (2) unstructured Document format.

4. **🔍 The First Full SQL Spectrum:**
Supports not just SELECT (Business Intelligence) queries, but also CRUD (e.g., UPDATE, CREATE, and other database management operations) queries.

5. **⚡ Automated Evaluation:**
Support fast evaluation via PostgreSQL template & docker. Each question includes verifiable test cases for accurate, reproducible scoring. Soft EX metric is used to evaluate SELECT-ONLY tasks; customized test cases are designed for DBA tasks, such as CRUD (CREATE, READ, UPDATE, DELETE). 

6. **🔄 Truly Live & Hidden Test:**
New databases and tasks are added over time. Each release features both open development and hidden test phases. The hidden test set from each release becomes the open development set for the next release, ensuring continuous evolution and fair evaluation.



## Previous Releases: [LiveSQLBench-Base-Lite](https://huggingface.co/datasets/birdsql/livesqlbench-base-lite)
- [LiveSQLBench-Base-Lite](https://huggingface.co/datasets/birdsql/livesqlbench-base-lite)

## 🎯 Current Release: LiveSQLBench-Base-Full-v1
Currently, we are pleased to release a **LiveSQLBench-Base-Full-v1**, containing **22 NEW end-user level databases** with **600 NEW** tasks (410 SELECT-only, 190 Management tasks), **HKB-JSON** and the **JSON operation in SQL**.

Some **NEW features**:
- **More Natural User Tasks**: User tasks are more colloquial and natural, making it  implicit to mapping to the DB and KB. Some tasks are even reasoning-intensive. That means the model needs to reason more deeply and multi-hop to solve the task.
- **More Real and Complex DBs**: DBs are more real and complex, containing more N2M relationships and more noisy schema and data.




## 💻 How to Use the Dataset
### Get the Dataset and Ground Truth
Download the dataset containing data file `livesqlbench_data.jsonl` and DB metafiles (including schema, HKB, column meaning files) by:
```bash
git clone https://huggingface.co/datasets/birdsql/livesqlbench-base-full-v1
```
To prevent data leakage through automated crawling, please request access to the ground truth and test cases by emailing **[📧 bird.bench25@gmail.com](mailto:bird.bench25@gmail.com)** with the subject line `[livesqlbench-base-full-v1 GT&Test Cases]`. An automated response will provide these data fields.

### Get the Database DDL Dumps and Building Scripts
The complete PostgreSQL **database dumps** and **building scripts** (`init-databases_postgresql.sh`) can be download from [the Google Drive](https://drive.google.com/file/d/1V9SFIWebi27JtaDUAScG1xE9ELbYcWLR/view?usp=sharing). 


### Evaluation
The details of usage and evaluation can be referred to [livesqlbench repo](https://github.com/bird-bench/livesqlbench).



## 📁 Directory Structure
Each database has its own directory:

```
.
├── README.md
├── database_name
│   ├── database_name_column_meaning_base.json
│   ├── database_name_kb.jsonl
│   ├── database_name_schema.txt
...
├── livesqlbench_data.jsonl
```

### 📂 Directory Contents:


* `*_schema.txt`: Database schema.
* `*_kb.jsonl`: Hierarchical knowledge base entries required to solve the user task.
  * `id`: The unique identifier for the knowledge.
  * `knowledge`: The name of the knowledge.
  * `description`: The description of the knowledge.
  * `definition`: The clear definition of the knowledge.
  * `type`: The type of the knowledge.
  * `children_knowledge`: A list of knowledge IDs that the current knowledge is dependent on. -1 means no children.
* `*_column_meaning_base.json`: Explanation of database columns.


## 📋 Dataset Fields (`livesqlbench_data.jsonl`):

* **instance\_id**: Unique task identifier.
* **selected\_database**: Associated database name.
* **query**: More natural user query (which is used in evaluation and our leaderboard).
* **normal_query**: The normal user query, which is more concise and direct. Just for reference.
* **sol\_sql** 🔒: Ground truth SQL solution.
* **external\_knowledge** 🔒: IDs of required external knowledge to solve the user task.
* **preprocess\_sql**: SQL setup queries.
* **clean\_up\_sql**: SQL queries to reset database state.
* **test\_cases** 🔒: Test cases to validate the predicted corrected SQL.
* **category**: "Query" (SELECT-only) or "Management" (CRUD).
* **high\_level**: Boolean indicating whether the user query contains high-level description.
* **conditions**: Indicates decimal/distinct conditions in the user query.
* **difficulty\_tier**: Task difficulty (Simple, Moderate, Challenging). 


## 🔒 Accessing Complete Data

To avoid data leakage by auto-crawling, certain fields (e.g., `sol_sql`, `test_cases`, `external_knowledge`) are excluded from the public dataset. For the full dataset, please email: **[📧 bird.bench25@gmail.com](mailto:bird.bench25@gmail.com)** with subject tag `[livesqlbench-base-full-v1 GT&Test Cases]`, which will be sent automatically.

## 🏆 Model Performance on LiveSQLBench-Base-Full-v1 (2025-09-04)

Please refer to our homepage: [🌐 LiveSQLBench](https://livesqlbench.ai)

## 🔄 Stay Tuned!

Upcoming releases:

* **🔄 LiveSQLBench-Large-Lite:** Industrial-scale databases with 1340+ columns.
* **🔄 LiveSQLBench-Large-Full:** Comprehensive large-scale datasets.

Want new dialects? Vote for new SQL dialects [🗳️ here](https://docs.google.com/forms/d/e/1FAIpQLSfEogmsA7LObI13KOoiojdnYfW28KEqvEVtC9hXaZJ8O9aCpQ/viewform?usp=header)!





## 📄 License:

cc-by-sa-4.0
=======
---
license: cc-by-sa-4.0
---
