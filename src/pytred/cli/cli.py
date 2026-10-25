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
    parser_metadata.set_defaults(func=print_datahub_metadata)

    return parser


def print_datahub_metadata(target: str) -> None:
    """Resolve a DataHub class and print its metadata as JSON."""
    metadata = get_datahub_metadata(resolve_name(target))
    print(json.dumps(metadata, indent=4, ensure_ascii=True))


def cli():

    parser = build_parser()
    args = vars(parser.parse_args())

    if func := args.pop("func", None):
        func(**args)
    else:
        parser.print_help()
