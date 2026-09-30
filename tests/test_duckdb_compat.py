"""Interoperability checks for the files written by mini-iceberg."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from mini_iceberg import MiniIceberg
from mini_iceberg.storage import TableStorage


@pytest.fixture
def workspace_path():
    """Use a unique workspace directory so the test works in restricted temp dirs."""
    root = Path(__file__).resolve().parents[1] / ".test-tmp"
    root.mkdir(exist_ok=True)
    path = root / uuid.uuid4().hex
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def test_filesystem_is_selected_from_location_url(workspace_path):
    local = TableStorage(workspace_path / "local")
    assert isinstance(local.base_fs, pafs.LocalFileSystem)
    assert isinstance(local.fs, pafs.SubTreeFileSystem)

    file_url = TableStorage((workspace_path / "file-url").as_uri())
    assert isinstance(file_url.base_fs, pafs.LocalFileSystem)
    assert isinstance(file_url.fs, pafs.SubTreeFileSystem)

    # Fake URI credentials let Arrow build an S3 filesystem without contacting
    # instance metadata or needing a real account. This test checks selection,
    # not network access to a bucket.
    s3 = TableStorage(
        "s3://test-access-key:test-secret-key@example-bucket/warehouse/demo"
        "?region=us-east-1&endpoint_override=localhost%3A9000&scheme=http"
    )
    assert isinstance(s3.base_fs, pafs.S3FileSystem)
    assert isinstance(s3.fs, pafs.SubTreeFileSystem)
    assert s3.resolve("metadata/v1.metadata.json") == "metadata/v1.metadata.json"

    # URL errors come from PyArrow's URI factory instead of our own parser.
    with pytest.raises(pa.ArrowInvalid):
        TableStorage("unknown://bucket/table")


def test_python_and_arrow_data_apis_share_an_arrow_model(workspace_path):
    # Python row mappings and Arrow batches should append to one table, while
    # scan() returns Python rows and scan_arrow() returns typed Arrow columns.
    table = MiniIceberg.create(
        workspace_path / "arrow-api-table", {"id": "long", "name": "string"}
    )
    table.append([{"id": 1, "name": "Ada"}])
    table.append(pa.table({"name": ["Lin"], "id": pa.array([2], type=pa.int64())}))

    assert table.scan() == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Lin"},
    ]
    arrow_rows = table.scan_arrow()
    assert isinstance(arrow_rows, pa.Table)
    assert arrow_rows.schema.field("id").type == pa.int64()
    assert arrow_rows.to_pylist() == table.scan()


def test_duckdb_reads_current_and_historical_snapshots(workspace_path):
    table_path = workspace_path / "duckdb-table"
    table = MiniIceberg.create(table_path, {"id": "long", "name": "string"})
    table.append([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Lin"}])
    before_delete = table.snapshots()[-1]["snapshot-id"]
    table.delete_where("id", 2)

    # Keep the downloaded native extension as a local cache: Windows can lock
    # a loaded .duckdb_extension until the test process exits.
    extension_dir = Path(__file__).resolve().parents[1] / ".duckdb-extensions"
    extension_dir.mkdir(exist_ok=True)
    connection = duckdb.connect()
    try:
        # The Iceberg extension is fetched once if it is not cached yet.
        extension_path = extension_dir.as_posix().replace("'", "''")
        connection.execute(f"SET extension_directory = '{extension_path}'")
        connection.execute("INSTALL iceberg")
        connection.execute("LOAD iceberg")

        location = table.location
        current_rows = connection.execute(
            "SELECT id, name FROM iceberg_scan(?) ORDER BY id", [location]
        ).fetchall()
        assert current_rows == [(1, "Ada")]

        snapshots = connection.execute(
            "SELECT sequence_number FROM iceberg_snapshots(?) ORDER BY sequence_number",
            [location],
        ).fetchall()
        assert snapshots == [(1,), (2,)]

        historical_rows = connection.execute(
            "SELECT id, name FROM iceberg_scan(?, snapshot_from_id => ?) ORDER BY id",
            [location, before_delete],
        ).fetchall()
        assert historical_rows == [(1, "Ada"), (2, "Lin")]
    finally:
        connection.close()
