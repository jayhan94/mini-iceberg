"""A deliberately small local table that demonstrates Iceberg v2 concepts.

The metadata relationships and Avro manifest schemas follow Iceberg v2. The
implementation is deliberately limited to local, unpartitioned Parquet tables.
"""

from __future__ import annotations

import copy
import json
import os
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlsplit

from .avro import manifest_list_schema, manifest_schema, read_avro, write_avro
from .storage import read_json, read_parquet, write_json, write_parquet, write_text

# These numeric values are defined by the Iceberg v2 manifest schema.
_CONTENT_DATA = 0
_CONTENT_POSITION_DELETES = 1
_SUPPORTED_TYPES = {"int", "long", "string", "boolean", "double"}


class MiniIceberg:
    """Manage one unpartitioned, local table using a tiny subset of Iceberg v2."""

    def __init__(self, location: Path):
        self.location = location.resolve()
        self.metadata_dir = self.location / "metadata"

    @classmethod
    def create(cls, location: str | Path, schema: dict[str, str]) -> "MiniIceberg":
        """Create an empty table. Schema is a mapping from column name to simple type."""
        root = Path(location).resolve()
        if root.exists() and any(root.iterdir()):
            raise FileExistsError(f"Table location is not empty: {root}")
        fields = cls._make_fields(schema)
        root.mkdir(parents=True, exist_ok=True)
        table = cls(root)
        now = _now_ms()
        metadata = {
            "format-version": 2,
            "table-uuid": str(uuid.uuid4()),
            "location": root.as_uri(),
            "last-updated-ms": now,
            "last-sequence-number": 0,
            "last-column-id": len(fields),
            "schemas": [{"schema-id": 0, "type": "struct", "fields": fields}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "current-snapshot-id": -1,
            "snapshots": [],
            "snapshot-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
        }
        table.metadata_dir.mkdir(parents=True, exist_ok=True)
        write_json(table.metadata_dir / "v1.metadata.json", metadata)
        # This tiny pointer stands in for a catalog such as Hive or REST.
        write_json(table.metadata_dir / "current", "v1.metadata.json", atomic=True)
        write_text(table.metadata_dir / "version-hint.text", "1", atomic=True)
        return table

    @classmethod
    def open(cls, location: str | Path) -> "MiniIceberg":
        table = cls(Path(location))
        if not (table.metadata_dir / "current").exists():
            raise FileNotFoundError(f"No mini-iceberg table at {table.location}")
        table._metadata()
        return table

    @staticmethod
    def _make_fields(schema: dict[str, str]) -> list[dict[str, Any]]:
        if not schema:
            raise ValueError("A table needs at least one column")
        fields = []
        for field_id, (name, field_type) in enumerate(schema.items(), start=1):
            if not name or field_type not in _SUPPORTED_TYPES:
                raise ValueError(f"Unsupported schema field: {name!r}: {field_type!r}")
            fields.append({"id": field_id, "name": name, "required": False, "type": field_type})
        return fields

    def _metadata(self) -> dict[str, Any]:
        filename = read_json(self.metadata_dir / "current")
        return read_json(self.metadata_dir / filename)

    def _schema(self, metadata: dict[str, Any]) -> list[dict[str, Any]]:
        schema_id = metadata["current-schema-id"]
        for schema in metadata["schemas"]:
            if schema["schema-id"] == schema_id:
                return schema["fields"]
        raise ValueError(f"Unknown current schema id: {schema_id}")

    def _snapshot(self, metadata: dict[str, Any], snapshot_id: int | None) -> dict[str, Any] | None:
        if snapshot_id is None:
            snapshot_id = metadata["current-snapshot-id"]
        if snapshot_id == -1:
            return None
        for snapshot in metadata["snapshots"]:
            if snapshot["snapshot-id"] == snapshot_id:
                return snapshot
        raise KeyError(f"Snapshot {snapshot_id} does not exist")

    def _table_path(self, relative_path: str) -> Path:
        """Resolve an Iceberg file URI or table-relative path inside this local table."""
        parsed = urlsplit(relative_path)
        if parsed.scheme == "file":
            path_text = unquote(parsed.path)
            if os.name == "nt" and path_text.startswith("/") and path_text[2:3] == ":":
                path_text = path_text[1:]
            path = Path(path_text).resolve()
        elif parsed.scheme:
            raise ValueError(f"Only local file paths are supported: {relative_path}")
        else:
            path = (self.location / relative_path).resolve()
        if not path.is_relative_to(self.location):
            raise ValueError(f"Metadata path escapes the table: {relative_path}")
        return path

    def _file_location(self, relative_path: str) -> str:
        return self._table_path(relative_path).as_uri()

    def _files(
        self, snapshot: dict[str, Any] | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if snapshot is None:
            return [], []
        listing = read_avro(self._table_path(snapshot["manifest-list"]))
        data_files: list[dict[str, Any]] = []
        delete_files: list[dict[str, Any]] = []
        for manifest_info in listing:
            manifest = read_avro(self._table_path(manifest_info["manifest_path"]))
            for entry in manifest:
                # Iceberg status 2 means this file was removed from the snapshot.
                if entry["status"] == 2:
                    continue
                data_file = entry["data_file"]
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

    def _iter_rows(
        self, snapshot: dict[str, Any] | None
    ) -> Iterable[tuple[dict[str, Any], str, int]]:
        data_files, delete_files = self._files(snapshot)
        deletes_by_file: dict[str, set[int]] = {}
        for delete in delete_files:
            for position_delete in read_parquet(self._table_path(delete["file-path"])):
                source = position_delete["file_path"]
                data_sequence = self._entry_sequence(data_files, source)
                # In v2, a delete file only applies to data files no newer than itself.
                if data_sequence is not None and delete["sequence-number"] >= data_sequence:
                    deletes_by_file.setdefault(source, set()).add(position_delete["pos"])
        for data_file in data_files:
            path = data_file["file-path"]
            hidden_positions = deletes_by_file.get(path, set())
            for position, row in enumerate(read_parquet(self._table_path(path))):
                if position not in hidden_positions:
                    yield row, path, position

    @staticmethod
    def _entry_sequence(data_files: list[dict[str, Any]], path: str) -> int | None:
        for data_file in data_files:
            if data_file["file-path"] == path:
                return data_file["sequence-number"]
        return None

    def scan(self, snapshot_id: int | None = None) -> list[dict[str, Any]]:
        """Read visible rows now, or from an older snapshot for time travel."""
        metadata = self._metadata()
        snapshot = self._snapshot(metadata, snapshot_id)
        return [row for row, _, _ in self._iter_rows(snapshot)]

    def append(self, rows: Iterable[dict[str, Any]]) -> int:
        """Write immutable Parquet data files and publish a new snapshot."""
        metadata = self._metadata()
        fields = self._schema(metadata)
        names = [field["name"] for field in fields]
        normalized = []
        for row in rows:
            unknown = row.keys() - set(names)
            if unknown:
                raise ValueError(f"Unknown columns: {sorted(unknown)}")
            normalized.append({name: row.get(name) for name in names})
        if not normalized:
            return 0
        file_path = self._file_location(f"data/{secrets.token_hex(8)}.parquet")
        count = write_parquet(self._table_path(file_path), normalized, fields)
        new_file = self._file_entry(file_path, _CONTENT_DATA, count)
        self._commit([new_file], [], operation="append", added_rows=count)
        return count

    def delete_where(self, field: str, value: Any) -> int:
        """Delete matching rows using a v2-style position delete file."""
        metadata = self._metadata()
        known_fields = {item["name"] for item in self._schema(metadata)}
        if field not in known_fields:
            raise KeyError(f"Unknown column: {field}")
        snapshot = self._snapshot(metadata, None)
        positions = sorted(
            (
                {"file_path": path, "pos": pos}
                for row, path, pos in self._iter_rows(snapshot)
                if row.get(field) == value
            ),
            key=lambda item: (item["file_path"], item["pos"]),
        )
        if not positions:
            return 0
        file_path = self._file_location(f"deletes/{secrets.token_hex(8)}.parquet")
        delete_fields = [
            {"id": 2147483546, "name": "file_path", "type": "string", "required": True},
            {"id": 2147483545, "name": "pos", "type": "long", "required": True},
        ]
        count = write_parquet(self._table_path(file_path), positions, delete_fields)
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
            "file-size-in-bytes": self._table_path(path).stat().st_size,
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
        """Write immutable snapshot metadata, then atomically publish its pointer."""
        metadata = self._metadata()
        parent = self._snapshot(metadata, None)
        old_data, old_deletes = self._files(parent)
        sequence = metadata["last-sequence-number"] + 1
        snapshot_id = secrets.randbits(63) or 1
        data = [self._as_existing(item) for item in old_data] + [
            self._as_added(item, snapshot_id, sequence) for item in new_data
        ]
        deletes = [self._as_existing(item) for item in old_deletes] + [
            self._as_added(item, snapshot_id, sequence) for item in new_deletes
        ]
        manifest_infos = []
        # Data and position-delete files live in separate manifests.
        for content, entries in ((_CONTENT_DATA, data), (_CONTENT_POSITION_DELETES, deletes)):
            if entries:
                manifest_infos.append(
                    self._write_manifest(metadata, snapshot_id, sequence, content, entries)
                )
        list_path = self._file_location(f"metadata/snap-{snapshot_id}-{sequence}.avro")
        write_avro(
            self._table_path(list_path),
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
        current_name = read_json(self.metadata_dir / "current")
        version = int(current_name[1:].split(".", 1)[0]) + 1
        next_name = f"v{version}.metadata.json"
        write_json(self.metadata_dir / next_name, metadata)
        write_json(self.metadata_dir / "current", next_name, atomic=True)
        write_text(self.metadata_dir / "version-hint.text", str(version), atomic=True)

    @staticmethod
    def _as_existing(entry: dict[str, Any]) -> dict[str, Any]:
        """Keep original sequence numbers; only the manifest status changes."""
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
            self._table_path(manifest_path),
            manifest_schema(),
            [self._avro_entry(entry) for entry in entries],
            {
                "schema": json.dumps(schema, separators=(",", ":")),
                "schema-id": str(metadata["current-schema-id"]),
                "partition-spec": "[]",
                "partition-spec-id": "0",
                "format-version": "2",
                "content": kind,
            },
        )
        # Counts in the manifest list let readers skip manifests without reading them.
        added = [entry for entry in entries if entry["status"] == "ADDED"]
        existing = [entry for entry in entries if entry["status"] == "EXISTING"]
        return {
            "manifest_path": manifest_path,
            "manifest_length": self._table_path(manifest_path).stat().st_size,
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
        """Return the snapshot log in creation order."""
        return copy.deepcopy(self._metadata()["snapshots"])


def _now_ms() -> int:
    return time.time_ns() // 1_000_000
