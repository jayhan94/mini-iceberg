"""用一个未分区的 Parquet 表，演示 Iceberg v2 的核心读写流程。

Iceberg 用元数据决定哪些文件属于表的某个版本，读取路径是：
    table metadata (JSON) → snapshot → manifest list (Avro)
    → manifest (Avro) → 数据文件 / 删除文件 (Parquet)

Parquet 保存行和列；Iceberg 在它上面管理表的 schema、文件集合和历史版本。
每次提交产生新的 snapshot；旧文件保持不变，因此旧 snapshot 仍然可以读取。
例如：追加 Ada、Lin 得到 S1；删除 Lin 得到 S2。S1、S2 引用同一个数据文件，
但只有 S2 引用删除文件，所以同一份 Parquet 可以呈现两个不同的数据状态。

建议先读 append() → _commit()，再读 scan() → _visible_file_tables()。
这里只实现教学所需的 v2 子集，不包含并发提交、schema 演进和分区裁剪。
"""

from __future__ import annotations

import copy
import json
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.compute as pc

from .manifests import manifest_list_schema, manifest_schema, read_avro, write_avro
from .storage import (
    TableStorage,
    iceberg_arrow_schema,
    read_json,
    read_parquet,
    read_text,
    write_json,
    write_parquet,
    write_text,
)

# content 描述文件的内容，而不是这次操作的类型；数值由 Iceberg v2 规范定义。
# v2 还有 content=2 的 equality delete，本例只实现按文件路径和行号删除。
_CONTENT_DATA = 0
_CONTENT_POSITION_DELETES = 1
_SUPPORTED_TYPES = {"int", "long", "string", "boolean", "double"}


class MiniIceberg:
    """管理一个表位置；行数据在内存中用 Arrow 表示，表元数据用字典表示。"""

    def __init__(self, location: str | Path):
        self.storage = TableStorage(location)
        self.location = self.storage.location

    @classmethod
    def create(
        cls,
        location: str | Path,
        schema: dict[str, str],
    ) -> "MiniIceberg":
        """创建空表，例如 schema={"id": "long", "name": "string"}。

        创建表只写元数据，不生成数据文件或 snapshot；首次追加才产生第一个 snapshot。
        schema 是列定义，Arrow table 是行数据，两者分别描述“长什么样”和“有哪些值”。
        """
        table = cls(location)
        if not table.storage.is_empty():
            raise FileExistsError(f"Table location is not empty: {table.location}")
        fields = cls._make_fields(schema)
        table.storage.create_dir()
        now = _now_ms()
        # metadata 是某一时刻的完整表定义。后续修改写新文件，保留旧文件。
        metadata = {
            "format-version": 2,
            # UUID 标识这张表；提交新 snapshot 时仍使用同一个 UUID。
            "table-uuid": str(uuid.uuid4()),
            "location": table.location,
            "last-updated-ms": now,
            "last-sequence-number": 0,
            "last-column-id": len(fields),
            # schema-id 标识一版列定义；field id 标识具体列，二者不是同一个概念。
            "schemas": [{"schema-id": 0, "type": "struct", "fields": fields}],
            "current-schema-id": 0,
            # 空的分区字段列表表示未分区；spec-id 为将来不同分区定义提供身份标识。
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            # 分区字段 ID 从 1000 开始，这里尚未分配，所以最后一个 ID 为 999。
            "last-partition-id": 999,
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": [],
            "snapshot-log": [],
            # order-id=0、fields=[] 表示未声明排序要求，不等于主键或唯一性约束。
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
        }
        write_json(table.storage, "metadata/v1.metadata.json", metadata)
        # 本例把 version-hint 当作当前 metadata 的定位入口，DuckDB 也能用它发现表。
        # 这是简化的发布机制；真实部署通常由 catalog 保存和更新 metadata 的位置，
        # version-hint 本身不能替代 catalog 的并发控制。
        write_text(table.storage, "metadata/version-hint.text", "1", atomic=True)
        return table

    @classmethod
    def open(
        cls,
        location: str | Path,
    ) -> "MiniIceberg":
        table = cls(location)
        if not table.storage.exists("metadata/version-hint.text") and not table.storage.exists(
            "metadata/current"
        ):
            raise FileNotFoundError(f"No mini-iceberg table at {table.location}")
        table._metadata()
        return table

    @staticmethod
    def _make_fields(schema: dict[str, str]) -> list[dict[str, Any]]:
        # 列的身份由 id 决定，而不是由名字或排列位置决定。
        # 真实 Iceberg 在列重命名、重排时仍保留 id；本例只在创建时分配 id。
        if not schema:
            raise ValueError("A table needs at least one column")
        fields = []
        for field_id, (name, field_type) in enumerate(schema.items(), start=1):
            if not name or field_type not in _SUPPORTED_TYPES:
                raise ValueError(f"Unsupported schema field: {name!r}: {field_type!r}")
            fields.append({"id": field_id, "name": name, "required": False, "type": field_type})
        return fields

    def _metadata(self) -> dict[str, Any]:
        # v1、v2、v3 是 metadata 文件的版本号，和 format-version=2 无关。
        # 格式版本定义协议；文件版本记录本表的提交进度。
        hint = "metadata/version-hint.text"
        if self.storage.exists(hint):
            version = read_text(self.storage, hint).strip()
            if not version.isdecimal():
                raise ValueError(f"Invalid Iceberg version hint: {version!r}")
            filename = f"v{version}.metadata.json"
        else:
            # Read tables created by the previous mini-iceberg version.
            filename = read_json(self.storage, "metadata/current")
        return read_json(self.storage, f"metadata/{filename}")

    def _schema(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        schema_id = metadata["current-schema-id"]
        for schema in metadata["schemas"]:
            if schema["schema-id"] == schema_id:
                return schema["fields"]
        raise ValueError(f"Unknown current schema id: {schema_id}")

    def _snapshot(self, metadata: dict[str, Any], snapshot_id: int | None) -> dict[str, Any] | None:
        # snapshot 是表数据的一致视图。指定 id 就能选定历史文件集合，完成时间旅行。
        # -1 是本例内部使用的“尚无 snapshot”标记。
        if snapshot_id is None:
            snapshot_id = metadata["current-snapshot-id"]
        if snapshot_id == -1:
            return None
        for snapshot in metadata["snapshots"]:
            if snapshot["snapshot-id"] == snapshot_id:
                return snapshot
        raise KeyError(f"Snapshot {snapshot_id} does not exist")

    def _file_location(self, relative_path: str) -> str:
        return self.storage.uri(relative_path)

    def _files(
        self, snapshot: dict[str, Any] | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """沿 snapshot 的清单索引找到文件，不扫描 data/ 目录。

        文件存在于磁盘不等于它属于这个 snapshot：未提交文件、旧文件都可能存在。
        读取元数据中记录的文件集合，才能获得一致的数据版本。
        """
        if snapshot is None:
            return [], []
        listing = read_avro(self.storage, snapshot["manifest-list"])
        data_files: list[dict[str, Any]] = []
        delete_files: list[dict[str, Any]] = []
        for manifest_info in listing:
            manifest = read_avro(self.storage, manifest_info["manifest_path"])
            for entry in manifest:
                # status: 0=已存在，1=本次新增，2=已从表的文件集合移除。
                # status=2 描述整个文件的移除，与“删除文件中某一行”是两个概念。
                if entry["status"] == 2:
                    continue
                data_file = entry["data_file"]
                # v2 允许新增条目的 snapshot/sequence 信息为空，从 manifest list 继承。
                # 返回前把这些值补齐，后续删除匹配就能使用明确的文件序号。
                item = {
                    "content": data_file["content"],
                    "file-path": data_file["file_path"],
                    "file-format": data_file["file_format"],
                    "partition": data_file["partition"],
                    "record-count": data_file["record_count"],
                    "file-size-in-bytes": data_file["file_size_in_bytes"],
                    "status": {0: "EXISTING", 1: "ADDED"}[entry["status"]],
                    "snapshot-id": (
                        entry["snapshot_id"]
                        if entry["snapshot_id"] is not None
                        else manifest_info["added_snapshot_id"]
                    ),
                    "sequence-number": (
                        entry["sequence_number"]
                        if entry["sequence_number"] is not None
                        else manifest_info["sequence_number"]
                    ),
                    "file-sequence-number": (
                        entry["file_sequence_number"]
                        if entry["file_sequence_number"] is not None
                        else manifest_info["sequence_number"]
                    ),
                }
                target = data_files if item["content"] == _CONTENT_DATA else delete_files
                target.append(item)
        return data_files, delete_files

    def _visible_file_tables(
        self, snapshot: dict[str, Any] | None
    ) -> Iterable[tuple[pa.Table, str, pa.Array]]:
        """读取数据文件，再应用 position delete，产生这个 snapshot 可见的 Arrow 行。"""
        data_files, delete_files = self._files(snapshot)
        deletes_by_file: dict[str, set[int]] = {}
        data_sequences = {item["file-path"]: item["sequence-number"] for item in data_files}
        for delete in delete_files:
            position_deletes = read_parquet(self.storage, delete["file-path"])
            for source, position in zip(
                position_deletes["file_path"].to_pylist(),
                position_deletes["pos"].to_pylist(),
            ):
                data_sequence = data_sequences.get(source)
                # 比较的是 data sequence number，即文件内容的相对年龄。
                # position delete 可以作用于序号 <= 自己的数据文件；未来追加的文件不受影响。
                # 本表未分区，因此这里只需再按完整文件路径匹配。
                if data_sequence is not None and delete["sequence-number"] >= data_sequence:
                    deletes_by_file.setdefault(source, set()).add(position)
        for data_file in data_files:
            path = data_file["file-path"]
            table = read_parquet(self.storage, path)
            # 行号从 0 开始，指原始 Parquet 文件里的位置；过滤后不能重新编号。
            # 例如删掉第 0 行后，原来的第 1 行仍应记作位置 1，供下一次删除使用。
            positions = pa.array(range(table.num_rows), type=pa.int64())
            hidden = deletes_by_file.get(path, set())
            if hidden:
                keep = pc.invert(pc.is_in(positions, value_set=pa.array(sorted(hidden))))
                yield pc.filter(table, keep), path, pc.filter(positions, keep)
            else:
                yield table, path, positions

    def scan(self, snapshot_id: int | None = None) -> pa.Table:
        """读当前或指定历史 snapshot，返回保留列类型的 Arrow table。"""
        metadata = self._metadata()
        fields = self._schema(metadata)
        snapshot = self._snapshot(metadata, snapshot_id)
        tables = [table for table, _, _ in self._visible_file_tables(snapshot)]
        if not tables:
            # 空表也保留 schema，使调用者知道列名和类型。
            return pa.Table.from_batches([], schema=iceberg_arrow_schema(fields))
        return pa.concat_tables(tables, promote_options="default")

    def append(
        self,
        rows: pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    ) -> int:
        """写入新的不可变 Parquet 文件，再通过提交使这些行对读者可见。"""
        metadata = self._metadata()
        fields = self._schema(metadata)
        table = _as_arrow_table(rows, fields)
        if not table.num_rows:
            return 0
        file_path = self._file_location(f"data/{secrets.token_hex(8)}.parquet")
        count = write_parquet(self.storage, file_path, table)
        # 新文件写完时，当前 snapshot 还不知道它。_commit 最后发布新状态才使它可见。
        new_file = self._file_entry(file_path, _CONTENT_DATA, count)
        self._commit([new_file], [], operation="append", added_rows=count)
        return count

    def delete_where(self, field: str, value: Any) -> int:
        """写位置删除文件，读取时再过滤目标行，不改写原来的数据 Parquet。"""
        metadata = self._metadata()
        known_fields = {item["name"] for item in self._schema(metadata)}
        if field not in known_fields:
            raise KeyError(f"Unknown column: {field}")
        snapshot = self._snapshot(metadata, None)
        positions = []
        for visible, path, source_positions in self._visible_file_tables(snapshot):
            # WHERE 的匹配结果是 Arrow 布尔列；NULL 不等于普通值，这里视为不匹配。
            matches = pc.fill_null(pc.equal(visible[field], pa.scalar(value)), False)
            matched_positions = pc.filter(source_positions, matches).to_pylist()
            positions.extend({"file_path": path, "pos": pos} for pos in matched_positions)
        positions.sort(key=lambda item: (item["file_path"], item["pos"]))
        if not positions:
            return 0
        file_path = self._file_location(f"deletes/{secrets.token_hex(8)}.parquet")
        delete_fields = [
            # 这两个大整数是规范为 position delete 预留的字段 ID，不是业务列 ID。
            # 每条记录 (file_path, pos) 精确定位一个原始数据文件中的一行。
            {"id": 2147483546, "name": "file_path", "type": "string", "required": True},
            {"id": 2147483545, "name": "pos", "type": "long", "required": True},
        ]
        delete_table = pa.Table.from_pylist(
            positions, schema=iceberg_arrow_schema(delete_fields)
        )
        count = write_parquet(self.storage, file_path, delete_table)
        new_delete = self._file_entry(file_path, _CONTENT_POSITION_DELETES, count)
        self._commit([], [new_delete], operation="delete", deleted_rows=count)
        return count

    def _file_entry(self, path: str, content: int, record_count: int) -> dict[str, Any]:
        return {
            "content": content,
            "file-path": path,
            "file-format": "PARQUET",
            "partition": {},
            "record-count": record_count,
            "file-size-in-bytes": self.storage.file_size(path),
        }

    def _commit(
        self,
        new_data: list[dict[str, Any]],
        new_deletes: list[dict[str, Any]],
        *,
        operation: str,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> None:
        """先写完整的新版本依赖，最后发布指向新 metadata 的入口。

        顺序是：数据/删除文件 → manifest → manifest list → metadata → 发布入口。
        读者只通过入口发现表状态，因此入口必须等其他文件全部写完后才更新。
        本例只有单写者；真实 Iceberg 还需 catalog 原子提交及冲突检测来协调多写者。
        """
        metadata = self._metadata()
        parent = self._snapshot(metadata, None)
        old_data, old_deletes = self._files(parent)
        # sequence 单调递增，用于判断先后；snapshot_id 只是唯一标识，不表示顺序。
        sequence = metadata["last-sequence-number"] + 1
        snapshot_id = secrets.randbits(63) or 1
        # 新 snapshot 包含仍然有效的旧文件和本次新增文件，并非只记录本次改动。
        # 数据文件被多个 snapshot 共享；本例为易读每次重写清单，真实实现可复用 manifest。
        data = [self._as_existing(item) for item in old_data] + [
            self._as_added(item, snapshot_id, sequence) for item in new_data
        ]
        deletes = [self._as_existing(item) for item in old_deletes] + [
            self._as_added(item, snapshot_id, sequence) for item in new_deletes
        ]
        manifest_infos = []
        # 一个 manifest 只记录数据文件或删除文件，因此两类文件分别写清单。
        for content, entries in ((_CONTENT_DATA, data), (_CONTENT_POSITION_DELETES, deletes)):
            if entries:
                manifest_infos.append(
                    self._write_manifest(metadata, snapshot_id, sequence, content, entries)
                )
        list_path = self._file_location(f"metadata/snap-{snapshot_id}-{sequence}.avro")
        write_avro(
            self.storage,
            list_path,
            manifest_list_schema(),
            manifest_infos,
            {
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent["snapshot-id"] if parent else "null"),
                "sequence-number": str(sequence),
                "format-version": "2",
            },
        )
        timestamp = _now_ms()
        # snapshot 引用本次 manifest list；parent 记录历史链条，不要求读表时逐层回放。
        snapshot = {
            "snapshot-id": snapshot_id,
            "parent-snapshot-id": parent["snapshot-id"] if parent else None,
            "sequence-number": sequence,
            "timestamp-ms": timestamp,
            "manifest-list": list_path,
            "schema-id": metadata["current-schema-id"],
            "summary": {
                "operation": operation,
                "added-records": str(added_rows),
                "deleted-records": str(deleted_rows),
            },
        }
        metadata["snapshots"].append(snapshot)
        metadata["snapshot-log"].append({"snapshot-id": snapshot_id, "timestamp-ms": timestamp})
        metadata["current-snapshot-id"] = snapshot_id
        metadata["last-sequence-number"] = sequence
        metadata["last-updated-ms"] = timestamp
        hint = "metadata/version-hint.text"
        if self.storage.exists(hint):
            version = int(read_text(self.storage, hint).strip()) + 1
        else:
            current_name = read_json(self.storage, "metadata/current")
            version = int(current_name[1:].split(".", 1)[0]) + 1
        next_name = f"v{version}.metadata.json"
        write_json(self.storage, f"metadata/{next_name}", metadata)
        # 这是本例的可见性切换点。本地用替换文件发布，S3 用完整对象写入发布；
        # 两者均未检查另一个写者是否同时提交，不能据此推导出多写者事务安全。
        write_text(self.storage, hint, str(version), atomic=True)

    @staticmethod
    def _as_existing(entry: dict[str, Any]) -> dict[str, Any]:
        """旧文件保留最初的序号，避免重新写清单时被误认为是刚产生的内容。"""
        return {
            "status": "EXISTING",
            "snapshot-id": entry["snapshot-id"],
            "sequence-number": entry["sequence-number"],
            "file-sequence-number": entry["file-sequence-number"],
            "data-file": {key: value for key, value in entry.items() if key not in {
                "status", "snapshot-id", "sequence-number", "file-sequence-number"
            }},
        }

    @staticmethod
    def _as_added(file: dict[str, Any], snapshot_id: int, sequence: int) -> dict[str, Any]:
        return {
            "status": "ADDED",
            "snapshot-id": snapshot_id,
            "sequence-number": sequence,
            "file-sequence-number": sequence,
            "data-file": file,
        }

    @staticmethod
    def _avro_entry(entry: dict[str, Any]) -> dict[str, Any]:
        # 外层 entry 描述文件的加入状态和序号，内层 data_file 描述文件自身。
        file = entry["data-file"]
        return {
            "status": {"EXISTING": 0, "ADDED": 1, "DELETED": 2}[entry["status"]],
            "snapshot_id": entry["snapshot-id"],
            "sequence_number": entry["sequence-number"],
            "file_sequence_number": entry["file-sequence-number"],
            "data_file": {
                "content": file["content"],
                "file_path": file["file-path"],
                "file_format": file["file-format"],
                "partition": {},
                "record_count": file["record-count"],
                "file_size_in_bytes": file["file-size-in-bytes"],
                # 真实实现可写列级统计（计数、上下界），让查询跳过不可能命中的文件。
                # 本例留空，所以扫描会读取全部有效数据文件。
                "column_sizes": None,
                "value_counts": None,
                "null_value_counts": None,
                "nan_value_counts": None,
                "lower_bounds": None,
                "upper_bounds": None,
                "key_metadata": None,
                "split_offsets": None,
                "equality_ids": None,
                "sort_order_id": None,
            },
        }

    def _write_manifest(
        self,
        metadata: dict[str, Any],
        snapshot_id: int,
        sequence: int,
        content: int,
        entries: list[dict[str, Any]],
    ) -> dict[str, Any]:
        kind = "data" if content == _CONTENT_DATA else "deletes"
        manifest_path = self._file_location(f"metadata/{snapshot_id}-{kind}.avro")
        schema = next(
            item for item in metadata["schemas"]
            if item["schema-id"] == metadata["current-schema-id"]
        )
        write_avro(
            self.storage,
            manifest_path,
            manifest_schema(),
            [self._avro_entry(entry) for entry in entries],
            # Avro 文件头携带写入时的 schema/spec，供读取方解释文件中的字段和分区值。
            {
                "schema": json.dumps(schema, separators=(",", ":")),
                "schema-id": str(metadata["current-schema-id"]),
                "partition-spec": "[]",
                "partition-spec-id": "0",
                "format-version": "2",
                "content": kind,
            },
        )
        # manifest list 是“清单的索引”：除了位置，还记录计数和分区摘要，帮助扫描规划。
        # 这里未分区，partitions 为空；计数描述文件记录数，不代表应用删除后的可见行数。
        added = [entry for entry in entries if entry["status"] == "ADDED"]
        existing = [entry for entry in entries if entry["status"] == "EXISTING"]
        return {
            "manifest_path": manifest_path,
            "manifest_length": self.storage.file_size(manifest_path),
            "partition_spec_id": 0,
            "content": content,
            "sequence_number": sequence,
            "min_sequence_number": min(entry["sequence-number"] for entry in entries),
            "added_snapshot_id": snapshot_id,
            "added_files_count": len(added),
            "existing_files_count": len(existing),
            "deleted_files_count": 0,
            "partitions": [],
            "added_rows_count": sum(entry["data-file"]["record-count"] for entry in added),
            "existing_rows_count": sum(entry["data-file"]["record-count"] for entry in existing),
            "deleted_rows_count": 0,
            "key_metadata": None,
        }

    def snapshots(self) -> list[dict[str, Any]]:
        """返回保留的 snapshot 描述；副本避免调用者意外修改内存中的元数据。"""
        return copy.deepcopy(self._metadata()["snapshots"])


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _as_arrow_table(
    data: pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    fields: list[dict[str, Any]],
) -> pa.Table:
    """把 Arrow 输入对齐到表的列定义，并附上 Iceberg 字段 ID。"""
    schema = iceberg_arrow_schema(fields)
    if isinstance(data, pa.RecordBatchReader):
        # 为便于本地调试，本例一次读完整个 reader，没有实现流式分批写文件。
        data = data.read_all()
    elif isinstance(data, pa.RecordBatch):
        data = pa.Table.from_batches([data])
    if not isinstance(data, pa.Table):
        raise TypeError("append() accepts only PyArrow Table, RecordBatch, or RecordBatchReader")

    names = set(data.column_names)
    expected = [field["name"] for field in fields]
    unknown = names - set(expected)
    if unknown:
        raise ValueError(f"Unknown columns: {sorted(unknown)}")
    arrays = []
    # 按表 schema 排列列，缺失的可选列补 NULL；safe cast 拒绝会损失精度的转换。
    for field in schema:
        if field.name not in names:
            arrays.append(pa.nulls(data.num_rows, type=field.type))
            continue
        column = data[field.name]
        arrays.append(column.cast(field.type, safe=True))
    return pa.Table.from_arrays(arrays, schema=schema)
