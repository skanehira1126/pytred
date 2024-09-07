from __future__ import annotations

import pathlib
import subprocess
import sys
import tempfile
from typing import Literal
from typing import Type

from pytred.data_hub import DataHub
from pytred.data_node import DataflowGraph
from pytred.data_node import DataflowNode
from pytred.data_node import EmptyDataNode

TEMPLATE = """## {datahub_name}
{datahub_description}

### {datahub_name} detail
| order | name | table type | join | keys | descriotion |
| :-: | :-: | :-: | :-: | :-: | :-- |
{detail}

### Dataflow image
```mermaid
{mermaid}
```
"""


ROW_TAMPLATE = "| {order} | {name} | {table_type} | {join} | {keys} | {description} |"


def report_datahub(datahub_class: Type[DataHub], *tables: EmptyDataNode) -> str:
    """
    Make report of datahub with markdown format.
    This report has contents below.
    1. docstrings of datahub class
    2. image of dataflow
    3. tables about each tables in datahub

    Parameters
    -----
    datahub_class: DataHub class
        make report of this datahub
    *tables: EmptyDataNode
        EmptyDataNode used in input DataHub class

    Returns
    -------
    str
        report of this datahub
    """

    template_variables = {
        "datahub_name": datahub_class.__name__,
        "datahub_description": trim(datahub_class.__doc__).replace("\n", "  \n"),
    }

    # make graph
    graph, dataflow_nodes = make_dataflow_graph_from_datahub(datahub_class, *tables)
    template_variables["mermaid"] = str(graph)

    # make table detail
    detail = ""
    for node in dataflow_nodes:
        params = {
            "order": node.level,
            "name": node.name,
        }
        if node.level == -1:
            params["table_type"] = "input"
            params["description"] = ""
        else:
            params["table_type"] = "function"
            params["description"] = trim(getattr(datahub_class, node.name).__doc__).replace(
                "\n", "<br>"
            )

        if node.join is None:
            params["join"] = ""
            params["keys"] = ""
        else:
            params["join"] = node.join
            params["keys"] = ", ".join(node.keys) if node.keys is not None else ""

        detail += ROW_TAMPLATE.format(**params) + "\n"

    template_variables["detail"] = detail

    return TEMPLATE.format(**template_variables)


def make_dataflow_graph_from_datahub(
    datahub_class: Type[DataHub],
    *tables: EmptyDataNode,
    output: str | pathlib.Path | None = None,
    direction: Literal["TD", "LR"] = "TD",
) -> tuple[DataflowGraph, list[DataflowNode]]:
    """
    Visualize the hierarchical structure of data preprocessing nodes from DataHub.

    Parameters
    ----------
    datahub: DataHub class
        visualizing dataflow of this DataHub
    output: str or pathlib.Path, optional
        file path of dataflow image
    direction: {'TD', 'LR'}, default 'TD'
        graph direction
    *tables: EmptyDataNode
        DataNode used in input DataHub class

    """
    nodes = datahub_class.search_tables(*tables)
    graph = datahub_class.get_dataflow_graph(nodes, direction)

    if output is not None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = pathlib.Path(tmpdir, "temp.mmd")
            with file_path.open("w") as f:
                f.write(str(graph))

            subprocess.run(["mmdc", "-i", file_path.as_posix(), "-o", str(output)])

    return graph, nodes


def trim(docstring: str | None) -> str:
    """
    ref.: https://peps.python.org/pep-0257/
    """
    if not docstring:
        return ""
    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = sys.maxsize
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < sys.maxsize:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())
    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)
    # Return a single string:
    return "\n".join(trimmed)
