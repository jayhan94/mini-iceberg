"""统一访问本地/S3 文件，并在 Iceberg 元数据、Arrow 与文件格式之间搭桥。

Arrow 是进程内的列式数据；Parquet 是持久化行数据的列式文件格式。
Avro 保存文件清单，JSON 保存表定义，它们服务于元数据管理，不替代 Parquet。
上层代码只操作 TableStorage，因此读写算法不需要分别实现本地和 S3 分支。
"""

from __future__ import annotations

import json
import os
import posixpath
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

_ARROW_TYPES = {
    "int": pa.int32(),
    "long": pa.int64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "double": pa.float64(),
}


class TableStorage:
    """把表目录视为统一文件系统的根目录。

    FileSystem.from_uri 负责解析位置并选择本地/S3 实例；SubTreeFileSystem
    使 metadata/v1.metadata.json 这样的相对路径自动落到指定表目录下。
    这是路径视图，不是操作系统级的安全沙箱。
    """

    def __init__(self, location: str | Path):
        requested = os.fspath(location)
        if isinstance(location, Path):
            requested = str(location.expanduser().resolve())

        try:
            base_fs, root_path = pafs.FileSystem.from_uri(requested)
        except pa.ArrowInvalid as error:
            # PyArrow's URI factory expects local paths to be absolute. A
            # scheme-less relative string is therefore normalized as a path;
            # malformed URLs with a scheme are left for PyArrow to reject.
            if "URI has empty scheme" not in str(error):
                raise
            requested = str(Path(requested).expanduser().resolve())
            base_fs, root_path = pafs.FileSystem.from_uri(requested)

        self.base_fs = base_fs
        self.fs = pafs.SubTreeFileSystem(root_path, base_fs)
        self.root_path = root_path

        # 清单中的文件位置需要完整 URI，便于 DuckDB 等其他引擎定位文件。
        # 实际读写则使用相对于表根目录的路径；这里同时维护两种表示。
        self.location = (
            Path(root_path).resolve().as_uri()
            if isinstance(base_fs, pafs.LocalFileSystem)
            else self._remote_uri(base_fs, root_path, requested)
        )

    @staticmethod
    def _remote_uri(base_fs: pafs.FileSystem, root_path: str, requested: str) -> str:
        # S3's factory returns its path as bucket/key; rebuild a canonical root
        # URI from that result instead of splitting the caller's URL ourselves.
        if isinstance(base_fs, pafs.S3FileSystem):
            return "s3://" + quote(root_path, safe="/-_.~")
        return requested.rstrip("/")

    def resolve(self, location: str | Path) -> str:
        """将本表的完整文件 URI 转成内部相对路径；本例仅访问表目录内的文件。"""
        raw = os.fspath(location).replace("\\", "/")
        root_uri = self.location.rstrip("/")
        if raw.startswith(root_uri + "/"):
            raw = raw[len(root_uri) + 1 :]
        path = posixpath.normpath(raw)
        if path in ("..", ".") or path.startswith("../") or posixpath.isabs(path):
            raise ValueError(f"Expected a path inside the table, got: {location!r}")
        return path

    def uri(self, location: str | Path) -> str:
        """生成写入 Iceberg metadata/manifest 的完整文件 URI。"""
        return f"{self.location.rstrip('/')}/{self.resolve(location)}"

    def create_dir(self, location: str | Path = "") -> None:
        if os.fspath(location) == "":
            self.base_fs.create_dir(self.root_path, recursive=True)
        else:
            self.fs.create_dir(self.resolve(location), recursive=True)

    def exists(self, location: str | Path) -> bool:
        info = self.fs.get_file_info(self.resolve(location))
        return info.type != pafs.FileType.NotFound

    def is_empty(self) -> bool:
        selector = pafs.FileSelector(self.root_path, allow_not_found=True, recursive=False)
        return not self.base_fs.get_file_info(selector)

    def file_size(self, location: str | Path) -> int:
        return self.fs.get_file_info(self.resolve(location)).size

    def open_input_file(self, location: str | Path):
        return self.fs.open_input_file(self.resolve(location))

    def open_output_stream(self, location: str | Path):
        path = self.resolve(location)
        parent = posixpath.dirname(path)
        if parent:
            self.fs.create_dir(parent, recursive=True)
        return self.fs.open_output_stream(path)

    def read_bytes(self, location: str | Path) -> bytes:
        with self.open_input_file(location) as stream:
            return stream.readall()

    def write_bytes(self, location: str | Path, value: bytes, *, atomic: bool = False) -> None:
        path = self.resolve(location)
        if atomic and isinstance(self.base_fs, pafs.LocalFileSystem):
            temporary = f"{path}.{uuid.uuid4().hex}.tmp"
            try:
                with self.open_output_stream(temporary) as stream:
                    stream.write(value)
                # 先写临时文件再替换入口，避免读者读到只写了一半的版本号。
                # 这是单个文件的发布步骤，没有执行 catalog 的比较并交换（CAS）。
                self.fs.move(temporary, path)
            finally:
                if self.exists(temporary):
                    self.fs.delete_file(temporary)
            return

        # S3 单对象写入完成后才可见；atomic 参数在这里不增加锁或冲突检测。
        # “读到完整对象”与“多个写者不会互相覆盖提交”是不同的保证。
        with self.open_output_stream(path) as stream:
            stream.write(value)


def write_json(storage: TableStorage, path: str, value: Any, *, atomic: bool = False) -> None:
    """JSON 用于表定义和 snapshot 引用；行数据仍只写入 Parquet。"""
    data = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    storage.write_bytes(path, data, atomic=atomic)


def read_json(storage: TableStorage, path: str) -> Any:
    return json.loads(storage.read_bytes(path))


def write_text(storage: TableStorage, path: str, value: str, *, atomic: bool = False) -> None:
    """Write plain text; Iceberg's version hint has no trailing newline."""
    storage.write_bytes(path, value.encode("utf-8"), atomic=atomic)


def read_text(storage: TableStorage, path: str) -> str:
    return storage.read_bytes(path).decode("utf-8")


def iceberg_arrow_schema(fields: list[dict[str, Any]]) -> pa.Schema:
    """把 Iceberg 的列类型、是否必填和字段 ID 映射到 Arrow schema。"""
    return pa.schema([_arrow_field(field) for field in fields])


def write_parquet(storage: TableStorage, path: str, table: pa.Table) -> int:
    """Arrow → Parquet：持久化列数据，并返回 manifest 需要的物理记录数。"""
    with storage.open_output_stream(path) as stream:
        pq.write_table(table, stream)
    return table.num_rows


def _arrow_field(field: dict[str, Any]) -> pa.Field:
    metadata = None
    if "id" in field:
        # PyArrow 会把这个元数据键写成 Parquet 字段 ID，使文件列与 Iceberg 列对应。
        # 重命名不应改变列 ID；因此仅保存列名不足以支持真实的 schema 演进。
        metadata = {b"PARQUET:field_id": str(field["id"]).encode()}
    return pa.field(
        field["name"],
        _ARROW_TYPES[field["type"]],
        nullable=not field.get("required", False),
        metadata=metadata,
    )


def read_parquet(storage: TableStorage, path: str) -> pa.Table:
    """Parquet → Arrow：仅解码文件；所属 snapshot 和删除规则由 table.py 处理。"""
    return pq.read_table(storage.resolve(path), filesystem=storage.fs)
