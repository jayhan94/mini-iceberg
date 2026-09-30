# mini-iceberg

用纯 Python 和本地文件 / S3 演示 Iceberg v2 表最重要的工作方式。数据文件和位置删除文件使用 Parquet；manifest 和 manifest list 使用带 Iceberg v2 schema、字段 ID 和文件元数据的 Avro Object Container File；表元数据使用 Iceberg JSON 格式。

文件读写由 PyArrow 的统一文件系统接口处理：`FileSystem.from_uri()` 根据表位置创建本地或 S3 文件系统，再用同一套读写方法访问所有文件。表位置支持普通本地路径、`file://` 和 `s3://`；S3 凭据使用 AWS 默认凭据链。

## 安装和本地运行

需要 Python 3.11+。项目用 [uv](https://docs.astral.sh/uv/) 管理依赖：

```powershell
uv sync
uv run python -m mini_iceberg demo --path .\demo_table
uv run python -m mini_iceberg show .\demo_table
```

`demo` 会创建表、追加两行、删除其中一行，并打印当前结果和删除前快照的结果。目标目录需要不存在或为空。

也可以从 Python 调用。普通路径和 `file://` URL 都支持：

```python
import pyarrow as pa

from mini_iceberg import MiniIceberg

table = MiniIceberg.create("demo_table", {"id": "long", "name": "string"})
table.append(pa.table({"id": [1, 2], "name": ["Ada", "Lin"]}))
before_delete = table.snapshots()[-1]["snapshot-id"]
table.delete_where("id", 2)

print(table.scan().to_pylist())                       # 当前快照：只剩 Ada
print(table.scan(before_delete).to_pylist())          # 时间旅行：两行都还在
```

表数据 API 只接受和返回 Arrow。`append()` 支持 PyArrow `Table`、`RecordBatch` 和 `RecordBatchReader`；`scan()` 返回保留列类型的 PyArrow `Table`。需要普通 Python 值时，可以在应用边界调用 Arrow 的 `.to_pylist()`：

```python
import pyarrow as pa

table.append(pa.table({"id": [3], "name": ["Grace"]}))
arrow_rows = table.scan()
python_rows = arrow_rows.to_pylist()
```

## 使用 S3

先创建 S3 bucket，再把 `s3://bucket/prefix` 作为表位置。PyArrow 会按 AWS 默认凭据链读取环境变量、AWS 配置文件或运行环境的角色凭据：

```python
import pyarrow as pa

table = MiniIceberg.create(
    "s3://my-bucket/warehouse/demo",
    {"id": "long", "name": "string"},
)
table.append(pa.table({"id": [1], "name": ["Ada"]}))

table = MiniIceberg.open(
    "s3://my-bucket/warehouse/demo",
)
print(table.scan().to_pylist())
```

S3 bucket 需要预先存在。请把凭据放在环境变量、凭据文件或密钥管理服务中。PyArrow 支持的 S3 region、endpoint 等连接设置可以放在 S3 URL 查询参数里；URL 交给 `FileSystem.from_uri()` 解析。

## 运行互操作测试

```powershell
uv sync --group test
uv run python -m pytest tests/test_duckdb_compat.py -p no:cacheprovider
```

测试包含一个真实 DuckDB 查询，会在本地表上检查当前 snapshot、snapshot 列表和删除前的历史 snapshot。首次运行时 DuckDB 可能需要联网下载 Iceberg 扩展。S3 测试只验证 PyArrow 根据 S3 URL 创建文件系统，不会访问真实 bucket。

## 用 DuckDB 读取

本地表示例：

```sql
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan('demo_table');
SELECT * FROM iceberg_snapshots('demo_table');
```

在 S3 上读取时，DuckDB 需要 `httpfs` 和 `iceberg` 扩展，并且需要配置好自己的 S3 凭据。DuckDB 可直接读取表元数据文件，例如：

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan(
    's3://my-bucket/warehouse/demo/metadata/v2.metadata.json'
);
```

将 `v2` 换成表目录 `metadata/version-hint.text` 中的版本号。DuckDB 的 S3 文档示例使用 metadata JSON 文件作为扫描入口。

## 从目录看一次提交

```text
demo_table/
├── data/
│   └── <id>.parquet                    # 不可变的数据文件
├── deletes/
│   └── <id>.parquet                    # 位置删除：表内相对数据文件路径 + 行号
└── metadata/
    ├── version-hint.text               # 当前 metadata 版本，mini-iceberg 和 DuckDB 都能读取
    ├── v1.metadata.json                # 表元数据；每次提交产生新版本
    ├── snap-<snapshot>-<seq>.avro      # 当前快照的 manifest list
    └── <snapshot>-<kind>.avro          # 数据或删除 manifest
```

读表时，程序从版本提示定位当前 metadata，再沿 `snapshot → manifest list → manifest → Parquet 文件` 找数据。追加不会改写旧 Parquet 文件，而是写入新文件并发布一个新快照。删除也不改写数据文件：位置删除记录行所在文件和行号，扫描时再应用删除。旧 snapshot 仍引用原来的文件集合，因此可以按 snapshot id 回看旧状态。

manifest Avro schema、manifest list Avro schema 和 OCF metadata 位于 [`mini_iceberg/manifests.py`](mini_iceberg/manifests.py)；存储 URL 与 Parquet 读写位于 [`mini_iceberg/storage.py`](mini_iceberg/storage.py)；提交和扫描流程从 [`mini_iceberg/table.py`](mini_iceberg/table.py) 开始。

## 实现范围

- 使用 Iceberg v2 的 table metadata、snapshot、manifest list、manifest entry、sequence number 和 position delete 概念及 Avro schema。
- 数据和删除文件只支持 Parquet，使用 PyArrow 读写；manifest 与 manifest list 只支持 Avro，使用 fastavro 写为标准 OCF 文件。
- 表位置支持本地路径、`file://` 和 `s3://`；PyArrow 文件系统工厂负责从 URI 创建底层文件系统。
- 为保持代码易读，目前只支持未分区表、追加、按字段值删除、扫描和按 snapshot id 时间旅行。暂不支持 schema 演进、equality delete、并发写入协调、compaction、分支/标签或其他云存储协议。

## 进一步阅读

- [Apache Iceberg 表格式规范](https://iceberg.apache.org/spec/)
- [Apache Arrow 文件系统和 S3FileSystem](https://arrow.apache.org/docs/python/filesystems.html)
- [DuckDB Iceberg 扩展](https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
