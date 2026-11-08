from argparse import ArgumentError
from argparse import ArgumentParser
from importlib.metadata import version
import json
from pkgutil import resolve_name

from pytred.helpers.metadata import get_datahub_metadata


def build_parser():
    parser = ArgumentParser()

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version('pytred')}",
    )

    subparsers = parser.add_subparsers()

    # metadata取得
    parser_metadata = subparsers.add_parser("metadata")
    parser_metadata.add_argument("target", type=str)
    parser_metadata.add_argument(
        "--compact", action="store_true", help="Show a short text outline with source locations"
    )
    parser_metadata.add_argument(
        "--table", metavar="NAME", help="Show only the named input or generated table"
    )
    parser_metadata.set_defaults(func=print_datahub_metadata)

    return parser


def print_datahub_metadata(target: str, compact: bool = False, table: str | None = None) -> None:
    """Print JSON metadata or a text outline, optionally selecting one table."""
    metadata = get_datahub_metadata(resolve_name(target))
    if table is not None:
        metadata["tables"] = [entry for entry in metadata["tables"] if entry["name"] == table]
        if not metadata["tables"]:
            raise ArgumentError(None, f"unknown table {table!r} in {target}")
    if compact:
        hub = metadata["DataHub"]
        print(f"{hub['name']} @ {_format_source(hub['source'])}")
        for entry in metadata["tables"]:
            if entry["is_optional"] is None:
                label = f"input {entry['name']}"
            else:
                label = f"{entry['order']} {entry['name']}({', '.join(entry['depends'])})"
                if entry["is_optional"]:
                    label += " [optional]"
            print(f"{label} @ {_format_source(entry['source'])}")
        return
    print(json.dumps(metadata, indent=4, ensure_ascii=True))


def _format_source(source: dict | None) -> str:
    if source is None:
        return "source unavailable"
    return f"{source['file']}:{source['line']}"


def cli():

    parser = build_parser()
    args = vars(parser.parse_args())

    if func := args.pop("func", None):
        try:
            func(**args)
        except ArgumentError as error:
            parser.error(str(error))
    else:
        parser.print_help()
