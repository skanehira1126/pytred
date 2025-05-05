import pathlib
import subprocess

import pytest

import pytred


def test__cli_make_report():
    """
    Check cli does not raise error
    """

    current_file_path = pathlib.Path(__file__)
    datahub_file_path = current_file_path.parent / "fixtures" / "data_hub.py"
    class_name = "ComplecatedDataHub"
    cmd = ["pytred", "report", datahub_file_path.as_posix(), class_name]
    cmd += ["--input-table", '{"name": "input_table1", "keys": ["id"], "join": "left"}']
    cmd += ["--input-table", '{"name": "input_table2"}']

    result = subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=True)
    _ = result.stdout


def test__raise_JosnDecodeError_with_invalid_json_str():

    current_file_path = pathlib.Path(__file__)
    datahub_file_path = current_file_path.parent / "fixtures" / "data_hub.py"
    class_name = "ComplecatedDataHub"
    cmd = ["pytred", "report", datahub_file_path.as_posix(), class_name]
    cmd += ["--input-table", '{"name": "input_table2",}']  # invalid json format

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(cmd, stdout=subprocess.PIPE, text=True, check=True)


def test__cli_show_version():

    result = subprocess.run(["pytred", "--version"], stdout=subprocess.PIPE, text=True, check=True)
    actual = result.stdout

    expected = f"pytred cli {pytred.__version__}\n"

    assert actual == expected
