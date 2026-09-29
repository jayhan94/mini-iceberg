"""Iceberg v2 Avro container files for manifests and manifest lists.

Field IDs and field names here follow the Iceberg v2 manifest schemas. Keeping
the schemas visible makes the metadata files easier to compare with the spec.
"""

from __future__ import annotations

import copy
from typing import Any, Iterable

from fastavro import reader, writer

from .storage import TableStorage


def _optional_map(
    name: str, field_id: int, key_id: int, value_id: int, value_type: str
) -> dict[str, Any]:
    # Iceberg Avro maps use this array-of-key/value-record encoding so their
    # key and value can each carry stable Iceberg field IDs.
    return {
        "name": name,
        "type": [
            "null",
            {
                "type": "array",
                "logicalType": "map",
                "items": {
                    "type": "record",
                    "name": f"k{key_id}_v{value_id}",
                    "fields": [
                        {"name": "key", "type": "int", "field-id": key_id},
                        {"name": "value", "type": value_type, "field-id": value_id},
                    ],
                },
            },
        ],
        "default": None,
        "field-id": field_id,
    }


def manifest_schema() -> dict[str, Any]:
    """The v2 manifest_entry schema for an unpartitioned table.

    The ``field-id`` annotations are Iceberg's stable identifiers. They let
    readers match fields even if a writer changes their Avro names or order.
    """
    # Keep Iceberg's numeric content values: 0=data, 1=position deletes.
    data_file = {
        "type": "record",
        "name": "data_file",
        "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {
                "name": "partition",
                "type": {"type": "record", "name": "r102", "fields": []},
                "field-id": 102,
            },
            {"name": "record_count", "type": "long", "field-id": 103},
            {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
            _optional_map("column_sizes", 108, 117, 118, "long"),
            _optional_map("value_counts", 109, 119, 120, "long"),
            _optional_map("null_value_counts", 110, 121, 122, "long"),
            _optional_map("nan_value_counts", 137, 138, 139, "long"),
            _optional_map("lower_bounds", 125, 126, 127, "bytes"),
            _optional_map("upper_bounds", 128, 129, 130, "bytes"),
            {
                "name": "key_metadata",
                "type": ["null", "bytes"],
                "default": None,
                "field-id": 131,
            },
            {
                "name": "split_offsets",
                "type": ["null", {"type": "array", "items": "long", "element-id": 133}],
                "default": None,
                "field-id": 132,
            },
            {
                "name": "equality_ids",
                "type": ["null", {"type": "array", "items": "int", "element-id": 136}],
                "default": None,
                "field-id": 135,
            },
            {
                "name": "sort_order_id",
                "type": ["null", "int"],
                "default": None,
                "field-id": 140,
            },
        ],
    }
    return {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": None,
                "field-id": 1,
            },
            {
                "name": "sequence_number",
                "type": ["null", "long"],
                "default": None,
                "field-id": 3,
            },
            {
                "name": "file_sequence_number",
                "type": ["null", "long"],
                "default": None,
                "field-id": 4,
            },
            {"name": "data_file", "type": data_file, "field-id": 2},
        ],
    }


def manifest_list_schema() -> dict[str, Any]:
    """The v2 manifest_file schema shared by every snapshot's manifest list."""
    field_summary = {
        "type": "record",
        "name": "field_summary",
        "fields": [
            {"name": "contains_null", "type": "boolean", "field-id": 509},
            {
                "name": "contains_nan",
                "type": ["null", "boolean"],
                "default": None,
                "field-id": 518,
            },
            {
                "name": "lower_bound",
                "type": ["null", "bytes"],
                "default": None,
                "field-id": 510,
            },
            {
                "name": "upper_bound",
                "type": ["null", "bytes"],
                "default": None,
                "field-id": 511,
            },
        ],
    }
    return {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "field-id": 500},
            {"name": "manifest_length", "type": "long", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "field-id": 502},
            {"name": "content", "type": "int", "field-id": 517},
            {"name": "sequence_number", "type": "long", "field-id": 515},
            {"name": "min_sequence_number", "type": "long", "field-id": 516},
            {"name": "added_snapshot_id", "type": "long", "field-id": 503},
            {
                "name": "added_files_count",
                "type": ["null", "int"],
                "default": None,
                "field-id": 504,
            },
            {
                "name": "existing_files_count",
                "type": ["null", "int"],
                "default": None,
                "field-id": 505,
            },
            {
                "name": "deleted_files_count",
                "type": ["null", "int"],
                "default": None,
                "field-id": 506,
            },
            {
                "name": "partitions",
                "type": [
                    "null",
                    {"type": "array", "items": field_summary, "element-id": 508},
                ],
                "default": None,
                "field-id": 507,
            },
            {
                "name": "added_rows_count",
                "type": ["null", "long"],
                "default": None,
                "field-id": 512,
            },
            {
                "name": "existing_rows_count",
                "type": ["null", "long"],
                "default": None,
                "field-id": 513,
            },
            {
                "name": "deleted_rows_count",
                "type": ["null", "long"],
                "default": None,
                "field-id": 514,
            },
            {
                "name": "key_metadata",
                "type": ["null", "bytes"],
                "default": None,
                "field-id": 519,
            },
        ],
    }


def write_avro(
    storage: TableStorage,
    path: str,
    schema: dict[str, Any],
    records: Iterable[dict[str, Any]],
    metadata: dict[str, str],
) -> None:
    """Write an Avro OCF through the selected local or object-store filesystem."""
    with storage.open_output_stream(path) as stream:
        # OCF stores both Avro records and key/value metadata in one file.
        writer(
            stream,
            copy.deepcopy(schema),
            records,
            codec="null",
            metadata=metadata,
            strict=True,
        )


def read_avro(storage: TableStorage, path: str) -> list[dict[str, Any]]:
    """Read records from an Avro object container file."""
    with storage.open_input_file(path) as stream:
        return list(reader(stream))
