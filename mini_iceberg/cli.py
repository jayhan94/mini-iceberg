"""Small command line entry points for trying the table locally."""

import argparse
import json
from pathlib import Path

from .table import MiniIceberg


def _demo(location: Path) -> None:
    table = MiniIceberg.create(location, {"id": "long", "name": "string"})
    table.append([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Lin"}])
    before_delete = table.snapshots()[-1]["snapshot-id"]
    table.delete_where("id", 2)
    print(f"Table files: {table.location}")
    print("Current rows:", json.dumps(table.scan(), ensure_ascii=False))
    print("Rows at the earlier snapshot:", json.dumps(table.scan(before_delete), ensure_ascii=False))
    print("Snapshots:", json.dumps(table.snapshots(), ensure_ascii=False, indent=2))


def _show(location: Path) -> None:
    table = MiniIceberg.open(location)
    print(json.dumps({"rows": table.scan(), "snapshots": table.snapshots()}, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Explore a tiny local Iceberg v2 teaching model")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="create a sample table, append, then delete a row")
    demo.add_argument("--path", type=Path, default=Path("mini_iceberg_demo"))
    show = commands.add_parser("show", help="print a table's current rows and snapshots")
    show.add_argument("path", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        _demo(args.path)
    else:
        _show(args.path)
