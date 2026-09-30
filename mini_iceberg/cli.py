"""Small command line entry points for trying the table locally."""

import argparse
import json

import pyarrow as pa

from .table import MiniIceberg


def _demo(location: str) -> None:
    table = MiniIceberg.create(location, {"id": "long", "name": "string"})
    table.append(pa.table({"id": [1, 2], "name": ["Ada", "Lin"]}))
    before_delete = table.snapshots()[-1]["snapshot-id"]
    table.delete_where("id", 2)
    print(f"Table files: {table.location}")
    print("Current rows:", json.dumps(table.scan().to_pylist(), ensure_ascii=False))
    print(
        "Rows at the earlier snapshot:",
        json.dumps(table.scan(before_delete).to_pylist(), ensure_ascii=False),
    )
    print("Snapshots:", json.dumps(table.snapshots(), ensure_ascii=False, indent=2))


def _show(location: str) -> None:
    table = MiniIceberg.open(location)
    print(
        json.dumps(
            {"rows": table.scan().to_pylist(), "snapshots": table.snapshots()},
            ensure_ascii=False,
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore a tiny Iceberg v2 teaching model")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="create a sample table, append, then delete a row")
    demo.add_argument("--path", default="mini_iceberg_demo")
    show = commands.add_parser("show", help="print a table's current rows and snapshots")
    show.add_argument("path")
    args = parser.parse_args()
    if args.command == "demo":
        _demo(args.path)
    else:
        _show(args.path)
