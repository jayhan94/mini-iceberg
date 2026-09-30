"""A deliberately small local/S3 table that demonstrates Iceberg v2 concepts.

The metadata relationships and Avro manifest schemas follow Iceberg v2. The
implementation is deliberately limited to unpartitioned Parquet tables.
"""

from __future__ import annotations

import copy
import json
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

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

# These numeric values are defined by the Iceberg v2 manifest schema.
_CONTENT_DATA = 0
_CONTENT_POSITION_DELETES = 1
_SUPPORTED_TYPES = {"int", "long", "string", "boolean", "double"}


class MiniIceberg:
    """Manage one unpartitioned local or S3 table using a tiny Iceberg v2 subset."""

    def __init__(self, location: str | Path):
        self.storage = TableStorage(location)
        self.location = self.storage.location

    @classmethod
    def create(
        cls,
        location: str | Path,
        schema: dict[str, str],
    ) -> "MiniIceberg":
        """Create an empty table. Schema is a mapping from column name to simple type."""
        table = cls(location)
        if not table.storage.is_empty():
            raise FileExistsError(f"Table location is not empty: {table.location}")
        fields = cls._make_fields(schema)
        table.storage.create_dir()
        now = _now_ms()
        metadata = {
            "format-version": 2,
            "table-uuid": str(uuid.uuid4()),
            "location": table.location,
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
        write_json(table.storage, "metadata/v1.metadata.json", metadata)
        # DuckDB and this mini implementation use the same simple version pointer.
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
        if not schema:
            raise ValueError("A table needs at least one column")
        fields = []
        for field_id, (name, field_type) in enumerate(schema.items(), start=1):
            if not name or field_type not in _SUPPORTED_TYPES:
                raise ValueError(f"Unsupported schema field: {name!r}: {field_type!r}")
            fields.append({"id": field_id, "name": name, "required": False, "type": field_type})
        return fields

    def _metadata(self) -> dict[str, Any]:
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
        if snapshot is None:
            return [], []
        listing = read_avro(self.storage, snapshot["manifest-list"])
        data_files: list[dict[str, Any]] = []
        delete_files: list[dict[str, Any]] = []
        for manifest_info in listing:
            manifest = read_avro(self.storage, manifest_info["manifest_path"])
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

    def _visible_file_tables(
        self, snapshot: dict[str, Any] | None
    ) -> Iterable[tuple[pa.Table, str, pa.Array]]:
        """Yield visible Arrow rows and their original positions for each file."""
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
                # In v2, a delete file only applies to data files no newer than itself.
                if data_sequence is not None and delete["sequence-number"] >= data_sequence:
                    deletes_by_file.setdefault(source, set()).add(position)
        for data_file in data_files:
            path = data_file["file-path"]
            table = read_parquet(self.storage, path)
            positions = pa.array(range(table.num_rows), type=pa.int64())
            hidden = deletes_by_file.get(path, set())
            if hidden:
                keep = pc.invert(pc.is_in(positions, value_set=pa.array(sorted(hidden))))
                yield pc.filter(table, keep), path, pc.filter(positions, keep)
            else:
                yield table, path, positions

    def scan_arrow(self, snapshot_id: int | None = None) -> pa.Table:
        """Return visible rows as an Arrow table, preserving column types."""
        metadata = self._metadata()
        fields = self._schema(metadata)
        snapshot = self._snapshot(metadata, snapshot_id)
        tables = [table for table, _, _ in self._visible_file_tables(snapshot)]
        if not tables:
            return pa.Table.from_batches([], schema=iceberg_arrow_schema(fields))
        return pa.concat_tables(tables, promote_options="default")

    def scan(self, snapshot_id: int | None = None) -> list[dict[str, Any]]:
        """Read visible rows as ordinary Python dictionaries."""
        return self.scan_arrow(snapshot_id).to_pylist()

    def append(
        self,
        rows: Iterable[Mapping[str, Any]] | pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    ) -> int:
        """Append Python row mappings or Arrow data, then publish a snapshot."""
        metadata = self._metadata()
        fields = self._schema(metadata)
        table = _as_arrow_table(rows, fields)
        if not table.num_rows:
            return 0
        file_path = self._file_location(f"data/{secrets.token_hex(8)}.parquet")
        count = write_parquet(self.storage, file_path, table)
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
        positions = []
        for visible, path, source_positions in self._visible_file_tables(snapshot):
            matches = pc.fill_null(pc.equal(visible[field], pa.scalar(value)), False)
            matched_positions = pc.filter(source_positions, matches).to_pylist()
            positions.extend({"file_path": path, "pos": pos} for pos in matched_positions)
        positions.sort(key=lambda item: (item["file_path"], item["pos"]))
        if not positions:
            return 0
        file_path = self._file_location(f"deletes/{secrets.token_hex(8)}.parquet")
        delete_fields = [
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
        write_text(self.storage, hint, str(version), atomic=True)

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
            self.storage,
            manifest_path,
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
        """Return the snapshot log in creation order."""
        return copy.deepcopy(self._metadata()["snapshots"])


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _as_arrow_table(
    data: Iterable[Mapping[str, Any]] | pa.Table | pa.RecordBatch | pa.RecordBatchReader,
    fields: list[dict[str, Any]],
) -> pa.Table:
    """Normalize supported public inputs to the table's internal Arrow schema."""
    schema = iceberg_arrow_schema(fields)
    if isinstance(data, pa.RecordBatchReader):
        data = data.read_all()
    elif isinstance(data, pa.RecordBatch):
        data = pa.Table.from_batches([data])
    if not isinstance(data, pa.Table):
        rows = list(data)
        names = {field["name"] for field in fields}
        for row in rows:
            unknown = row.keys() - names
            if unknown:
                raise ValueError(f"Unknown columns: {sorted(unknown)}")
        return pa.Table.from_pylist(rows, schema=schema)

    names = set(data.column_names)
    expected = [field["name"] for field in fields]
    unknown = names - set(expected)
    if unknown:
        raise ValueError(f"Unknown columns: {sorted(unknown)}")
    arrays = []
    for field in schema:
        if field.name not in names:
            arrays.append(pa.nulls(data.num_rows, type=field.type))
            continue
        column = data[field.name]
        arrays.append(column.cast(field.type, safe=True))
    return pa.Table.from_arrays(arrays, schema=schema)
