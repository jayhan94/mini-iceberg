# mini-iceberg

用纯本地文件演示 Iceberg v2 表最重要的工作方式。数据文件和位置删除文件使用 Parquet；manifest 和 manifest list 使用带 Iceberg v2 schema、字段 ID 和文件元数据的 Avro Object Container File；表元数据使用 Iceberg JSON 格式。

## 运行

需要 Python 3.11+。项目用 [uv](https://docs.astral.sh/uv/) 管理依赖：

```powershell
uv sync
uv run python -m mini_iceberg demo --path .\demo_table
```

`demo` 会创建表、追加两行、删除其中一行，并打印当前结果和删除前快照的结果。目标目录需要不存在或为空。查看已创建的表：

```powershell
uv run python -m mini_iceberg show .\demo_table
```

也可以直接在 Python 中操作：

```python
from mini_iceberg import MiniIceberg

table = MiniIceberg.create("demo_table", {"id": "long", "name": "string"})
table.append([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Lin"}])
before_delete = table.snapshots()[-1]["snapshot-id"]
table.delete_where("id", 2)

print(table.scan())                       # 当前快照：只剩 Ada
print(table.scan(before_delete))          # 时间旅行：两行都还在
```

## 用 DuckDB 读取

先按上面的命令创建示例表，再在 DuckDB 中运行：

```sql
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan('demo_table');
SELECT * FROM iceberg_snapshots('demo_table');
```

表目录的 `metadata/version-hint.text` 让 DuckDB 找到最新的 `vN.metadata.json`。如果 DuckDB 尚未安装 Iceberg 扩展，首次 `INSTALL` 需要联网。

## 从目录看一次提交

```text
demo_table/
├── data/
│   └── <id>.parquet                    # 不可变的数据文件
├── deletes/
│   └── <id>.parquet                    # 位置删除：数据文件 URI + 行号
└── metadata/
    ├── current                         # mini-iceberg 的本地 catalog 指针
    ├── version-hint.text               # DuckDB 用来定位 metadata 版本
    ├── v1.metadata.json                # 表元数据；每次提交产生新版本
    ├── snap-<snapshot>-<seq>.avro      # 当前快照的 manifest list
    └── <snapshot>-<kind>.avro          # 数据或删除 manifest
```

读表时，程序从 metadata 定位当前 snapshot，再沿 `snapshot → manifest list → manifest → Parquet 文件` 找数据。追加不会改写旧 Parquet 文件，而是写入新文件并发布一个新快照。删除也不改写数据文件：位置删除记录行所在文件和行号，扫描时再应用删除。旧 snapshot 仍引用原来的文件集合，因此可以按 snapshot id 回看旧状态。

manifest Avro schema、manifest list Avro schema 和 OCF metadata 位于 [`mini_iceberg/manifests.py`](mini_iceberg/manifests.py)；提交和扫描流程从 [`mini_iceberg/table.py`](mini_iceberg/table.py) 开始。提交会先写不可变的数据和 metadata 文件，最后原子更新本地 catalog 指针与 DuckDB 的版本提示。

## 实现范围

- 使用 Iceberg v2 的 table metadata、snapshot、manifest list、manifest entry、sequence number 和 position delete 概念及 Avro schema。
- 数据和删除文件只支持 Parquet，使用 PyArrow 读写；manifest 与 manifest list 只支持 Avro，使用 fastavro 写为标准 OCF 文件。
- 可由 DuckDB 的 Iceberg 扩展通过本地路径读取表和快照。
- 为保持代码易读，目前只支持本地未分区表、追加、按字段值删除、扫描和按 snapshot id 时间旅行。暂不支持分区、schema 演进、equality delete、并发 catalog 提交、compaction、分支/标签或云存储。

## 进一步阅读

- [Apache Iceberg 表格式规范](https://iceberg.apache.org/spec/)
- [Apache Arrow：用 PyArrow 读写 Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [DuckDB Iceberg 扩展](https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
