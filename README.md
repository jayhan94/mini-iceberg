# mini-iceberg

用纯本地文件演示 Iceberg v2 表最重要的工作方式。代码优先服务于阅读和调试：数据文件与位置删除文件是真正的 Parquet；表元数据是 JSON；manifest 和 manifest list 用易读的 JSON 表示，方便直接打开查看。

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

## 从目录看一次提交

```text
demo_table/
├── data/
│   └── <id>.parquet                 # 不可变的数据文件
├── deletes/
│   └── <id>.parquet                 # 位置删除：数据文件路径 + 行号
└── metadata/
    ├── current                      # 本地 catalog 指针
    ├── v1.metadata.json              # 表的初始元数据
    ├── v2.metadata.json              # 每次提交产生新的元数据版本
    ├── manifest-list-<snapshot>.json # 当前快照包含哪些 manifest
    └── manifest-<snapshot>-*.json    # 快照跟踪的数据文件 / 删除文件
```

读表时，程序从 `metadata/current` 找到当前元数据版本，再按 `snapshot → manifest list → manifest → Parquet 文件` 的关系定位数据。追加不会改写旧 Parquet 文件，而是写入新文件并发布一个新快照。删除也不改写数据文件：它记录匹配行所在的文件和行号，扫描时再应用位置删除。旧快照继续引用当时的文件集合，所以可以按 snapshot id 回看旧状态。

新快照的元数据文件和清单文件先写好，最后用原子文件替换更新 `metadata/current`。这样读者只会看到旧版本或新版本的指针，不会读到半写入的指针。这个本地指针只是教学用 catalog 简化版。

## 适合学习的范围与简化

- 表的 `format-version`、字段 ID、schema、snapshot、sequence number、manifest list、manifest 和 position delete 的概念按 Iceberg v2 建模。
- **数据文件和删除文件只支持 Parquet**，通过 PyArrow 读写；Parquet 列保存稳定的字段 ID；简单字段类型支持 `int`、`long`、`string`、`boolean`、`double`。
- 为降低理解门槛，manifest 和 manifest list 存成可读 JSON；真实 Iceberg 使用 Avro 编码 manifest 和 manifest list。这里生成的目录不是可被 Spark、Trino 等直接读取的完整 Iceberg 表。
- 只支持单机本地路径、未分区表、追加、按字段值删除、扫描和按 snapshot id 时间旅行。暂不支持 catalog 并发提交、schema/partition 演进、equality delete、compaction、分支/标签或云存储。

核心实现从 [`mini_iceberg/table.py`](mini_iceberg/table.py) 开始；Parquet 和 JSON 文件读写在 [`mini_iceberg/storage.py`](mini_iceberg/storage.py)。

## 进一步阅读

- [Apache Iceberg 表格式规范](https://iceberg.apache.org/spec/)
- [Apache Arrow：用 PyArrow 读写 Parquet](https://arrow.apache.org/docs/python/parquet.html)
