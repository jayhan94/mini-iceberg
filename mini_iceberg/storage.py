"""Tiny file helpers used by the table implementation."""

import json
import os
import uuid
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

_ARROW_TYPES = {
    "int": pa.int32(),
    "long": pa.int64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "double": pa.float64(),
}


def write_json(path: Path, value: Any, *, atomic: bool = False) -> None:
    """Write readable JSON; atomic replacement is used for the catalog pointer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path
    if atomic:
        target = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with target.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        if atomic:
            os.replace(target, path)
    finally:
        if atomic and target.exists():
            target.unlink()


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def write_text(path: Path, value: str, *, atomic: bool = False) -> None:
    """Write plain text; unlike JSON, a version hint must not add a newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path
    if atomic:
        target = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        target.write_text(value, encoding="utf-8", newline="")
        if atomic:
            os.replace(target, path)
    finally:
        if atomic and target.exists():
            target.unlink()


def write_parquet(
    path: Path, rows: Iterable[dict[str, Any]], fields: list[dict[str, Any]]
) -> int:
    """Write records as a real Parquet file with the supplied simple schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([_arrow_field(field) for field in fields])
    table = pa.Table.from_pylist(list(rows), schema=schema)
    pq.write_table(table, path)
    return table.num_rows


def _arrow_field(field: dict[str, Any]) -> pa.Field:
    metadata = None
    if "id" in field:
        # PyArrow writes this metadata as the Parquet schema field ID used by Iceberg.
        metadata = {b"PARQUET:field_id": str(field["id"]).encode()}
    return pa.field(
        field["name"],
        _ARROW_TYPES[field["type"]],
        nullable=not field.get("required", False),
        metadata=metadata,
    )


def read_parquet(path: Path) -> Iterable[dict[str, Any]]:
    """Read records from one Parquet file as ordinary Python dictionaries."""
    return pq.read_table(path).to_pylist()
