"""用标准 Avro Object Container File (OCF) 保存 Iceberg v2 的两层清单。

manifest：每条记录描述一个数据/删除文件，包含路径、行数、状态和序号等信息。
manifest list：每条记录描述一个 manifest；一个 snapshot 引用一份 manifest list。
这种两层结构让大表可先按清单摘要筛选，再按文件统计筛选，最后才读 Parquet。
本例保留标准字段及字段 ID，但省略统计采集和查询裁剪，方便与规范逐项对照。

这里的 Avro schema 是“清单记录”的结构；文件头中另存的 Iceberg schema
是“业务表的列定义”。不要把两种 schema 混为一谈。
"""

from __future__ import annotations

import copy
from typing import Any, Iterable

from fastavro import reader, writer

from .storage import TableStorage


def _optional_map(
    name: str, field_id: int, key_id: int, value_id: int, value_type: str
) -> dict[str, Any]:
    # 列统计按业务列的字段 ID 索引，所以 key 是整数，不能用只支持字符串 key 的 Avro map。
    # Iceberg 将它编码为 key/value record 的数组，也能给 key 和 value 附加字段 ID。
    # ["null", ...] 表示可选字段；default=None 表示文件可以不提供这个统计信息。
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
    """未分区表的 v2 manifest_entry schema。

    field-id 是协议规定的稳定标识，不按本程序的字典顺序生成。
    即使 Avro 字段位置不同，读取方仍可用 ID 识别同一个字段。
    """
    # content=0 是数据；content=1 是位置删除。data_file 同样可描述删除文件。
    data_file = {
        "type": "record",
        "name": "data_file",
        "fields": [
            {"name": "content", "type": "int", "field-id": 134},
            {"name": "file_path", "type": "string", "field-id": 100},
            {"name": "file_format", "type": "string", "field-id": 101},
            {
                "name": "partition",
                # 未分区时也保留 partition 字段，但其 struct 没有任何子字段。
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
            # status 跟踪整个文件：EXISTING=0、ADDED=1、DELETED=2。
            {"name": "status", "type": "int", "field-id": 0},
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": None,
                "field-id": 1,
            },
            {
                "name": "sequence_number",
                # data sequence number：文件内容的相对年龄，用于匹配删除文件。
                "type": ["null", "long"],
                "default": None,
                "field-id": 3,
            },
            {
                "name": "file_sequence_number",
                # file sequence number：这个物理文件加入表时的序号。
                # 内容年龄和物理文件年龄可以不同；本例每次追加的新文件二者相同。
                "type": ["null", "long"],
                "default": None,
                "field-id": 4,
            },
            {"name": "data_file", "type": data_file, "field-id": 2},
        ],
    }


# 一份 manifest list 中可以同时出现数据 manifest 和删除 manifest。
# 它的 content=0/1 表示“数据清单/删除清单”，和 data_file.content 的枚举层级不同。
# 分区摘要记录 manifest 内所有文件的范围，便于先跳过整个 manifest；本表未使用它。
def manifest_list_schema() -> dict[str, Any]:
    """snapshot 所引用的 manifest_file 列表的 v2 schema。"""
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
    """通过统一文件系统写自包含的 Avro 文件，DuckDB 可按标准格式读取。"""
    with storage.open_output_stream(path) as stream:
        # OCF 文件头保存 Avro schema 和 key/value metadata，文件体保存清单记录。
        # codec="null" 表示不压缩，便于学习；strict=True 检查记录是否符合声明的结构。
        writer(
            stream,
            copy.deepcopy(schema),
            records,
            codec="null",
            metadata=metadata,
            strict=True,
        )


def read_avro(storage: TableStorage, path: str) -> list[dict[str, Any]]:
    """从 OCF 内嵌的 Avro schema 解码记录，不需要外部 schema 文件。"""
    with storage.open_input_file(path) as stream:
        return list(reader(stream))
