from __future__ import annotations

import argparse
import importlib.util
import json
from logging import getLogger

import pytred
from pytred.data_node import EmptyDataNode
from pytred.helpers import visualize


logger = getLogger(__name__)


def get_parser():
    parser = argparse.ArgumentParser(prog="pytred cli")
    parser.add_argument("--version", action="version", version=f"%(prog)s {pytred.__version__}")

    subparsers = parser.add_subparsers()

    # make report
    parser_report = subparsers.add_parser("report", help="see 'pytred report -h'")
    parser_report.add_argument("file_path")
    parser_report.add_argument("class_name")
    parser_report.add_argument("--input-table", action="append", dest="inputs_table")
    parser_report.set_defaults(func=cli_report)

    return parser


def cli():

    parser = get_parser()

    args = vars(parser.parse_args())
    if func := args.pop("func", None):
        func(**args)
    else:
        parser.parse_args(["--help"])


def cli_report(file_path: str, class_name: str, inputs_table: list[str]):

    # parse inputs_table and make EmptyDataNode
    data_nodes = []
    for input_table_str in inputs_table:
        try:
            table = json.loads(input_table_str)
        except json.decoder.JSONDecodeError as e:
            logger.exception(f"Failed to parse {input_table_str}")
            raise e

        data_nodes.append(
            EmptyDataNode(
                name=table["name"],
                join=table.get("join", None),
                keys=table.get("keys", None),
            )
        )

    # import target class
    spec = importlib.util.spec_from_file_location("visualize_datahub", file_path)
    if spec is None:
        raise FileNotFoundError(f"{file_path} is not found.")
    module = importlib.util.module_from_spec(spec)
    if module is None:
        raise RuntimeError(f"Failed to convert {file_path} to module.")
    spec.loader.exec_module(module)  # type: ignore

    target_datahub_class = getattr(module, class_name)

    # print report
    report = visualize.report_datahub(target_datahub_class, *data_nodes)
    print(report)
