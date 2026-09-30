"""A small, URL-selected filesystem shared by all Iceberg file formats."""

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
    """A table-root view over the filesystem chosen by its location URI.

    ``FileSystem.from_uri`` creates the concrete filesystem (local, S3, etc.).
    A SubTreeFileSystem then makes all table paths relative to this table, so
    Parquet, Avro, and JSON code can use the same small filesystem interface.
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

        # For local paths, publish a stable absolute URI in Iceberg metadata.
        # For remote filesystems, preserve the URI accepted by PyArrow.
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
        """Normalize a path inside the table without interpreting a URL."""
        raw = os.fspath(location).replace("\\", "/")
        root_uri = self.location.rstrip("/")
        if raw.startswith(root_uri + "/"):
            raw = raw[len(root_uri) + 1 :]
        path = posixpath.normpath(raw)
        if path in ("..", ".") or path.startswith("../") or posixpath.isabs(path):
            raise ValueError(f"Expected a path inside the table, got: {location!r}")
        return path

    def uri(self, location: str | Path) -> str:
        """Return an absolute URI, suitable for Iceberg metadata and manifests."""
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
                # Local rename publishes the new version hint in one step.
                self.fs.move(temporary, path)
            finally:
                if self.exists(temporary):
                    self.fs.delete_file(temporary)
            return

        # Object stores publish the complete object when the stream closes.
        with self.open_output_stream(path) as stream:
            stream.write(value)


def write_json(storage: TableStorage, path: str, value: Any, *, atomic: bool = False) -> None:
    """Write readable table metadata JSON."""
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
    """Build Arrow schema while retaining Iceberg's stable field IDs."""
    return pa.schema([_arrow_field(field) for field in fields])


def write_parquet(storage: TableStorage, path: str, table: pa.Table) -> int:
    """Write an Arrow table as Parquet using the table's selected filesystem."""
    with storage.open_output_stream(path) as stream:
        pq.write_table(table, stream)
    return table.num_rows


def _arrow_field(field: dict[str, Any]) -> pa.Field:
    metadata = None
    if "id" in field:
        # Parquet field IDs preserve Iceberg's stable column identity.
        metadata = {b"PARQUET:field_id": str(field["id"]).encode()}
    return pa.field(
        field["name"],
        _ARROW_TYPES[field["type"]],
        nullable=not field.get("required", False),
        metadata=metadata,
    )


def read_parquet(storage: TableStorage, path: str) -> pa.Table:
    """Read one Parquet file as Arrow using the table's selected filesystem."""
    return pq.read_table(storage.resolve(path), filesystem=storage.fs)
