# Command Line Interface


## DataFlow Reporting CLI
`pytred` has reporting cli from script file.

```
$ pytred report --help
usage: pytred cli report [-h] [--input-table INPUTS_TABLE] file_path class_name

positional arguments:
  file_path
  class_name

options:
  -h, --help            show this help message and exit
  --input-table INPUTS_TABLE
```

### Example
#### 1. Create datahub class
??? "sample_datahub.py"

    ```python
    from pytred import DataHub, DataNode
    from pytred.decorators import polars_table


    class ComplecatedDataHub(DataHub):
        """
        This is example of visualizing data processing flow
        """

        @polars_table(0, "id", join="inner")
        def table1_1(self, input_table2):
            """
            Description of table1_1
            """
            ...

        @polars_table(0, None, join=None)
        def table1_2(self, input_table2):
            # this function has not docstrings
            ...

        @polars_table(0, "id1", "id2", join="left")
        def table1_3(self):
            """
            Description of table1_3
            """
            ...

        @polars_table(0, None, join=None)
        def table1_4(self):
            """
            multi lines description of table1_4
            multi lines description of table1_4
            """
            ...

        @polars_table(1, None, join=None)
        def table2_1(self, input_table2, table1_1):
            # this function has not docstrings
            ...

        @polars_table(1, None, join=None)
        def table2_2(self, table1_1, table1_2):
            # this function has not docstrings
            ...

        @polars_table(1, None, join=None)
        def table2_3(self, input_table2, table1_3):
            """
            Description of table2_3
            """
            ...

        @polars_table(1, None, join=None)
        def table2_4(self, table1_3):
            """
            Description of table2_4
            """
            ...

        @polars_table(2, "id", join="left")
        def table3(self, table1_4, table2_3, table2_4):
            """
            Description of table3
            """
            ...
    ```

#### 2. Run cli and create report.

```
$ pytred report sample_datahub.py ComplecatedDataHub \
  --input-table '{"name": "input_table1", "keys": ["id"], "join": "left"}' \
  --input-table '{"name": "input_table2"}' \
  > report_dataflow.md
```

???+ "Preview output report"
    
    ## ComplecatedDataHub
    This is example of visualizing data processing flow

    ### ComplecatedDataHub detail
    | order | name | table type | join | keys | descriotion |
    | :-: | :-: | :-: | :-: | :-: | :-: |
    | -1 | input_table1 | input | left | id |  |
    | -1 | input_table2 | input |  |  |  |
    | 0 | table1_1 | function | inner | id | Description of table1_1 |
    | 0 | table1_2 | function |  |  |  |
    | 0 | table1_3 | function | left | id1, id2 | Description of table1_3 |
    | 0 | table1_4 | function |  |  | multi lines description of table1_4<br>multi lines description of table1_4 |
    | 1 | table2_1 | function |  |  |  |
    | 1 | table2_2 | function |  |  |  |
    | 1 | table2_3 | function |  |  | Description of table2_3 |
    | 1 | table2_4 | function |  |  | Description of table2_4 |
    | 2 | table3 | function | left | id | Description of table3 |


    ### Dataflow image
    ```mermaid
    graph TD
        input_table1[(input_table1)]
        input_table2[(input_table2)]
        table1_1([table1_1])
        table1_2[table1_2]
        table1_3([table1_3])
        table1_4[table1_4]
        table2_1[table2_1]
        table2_2[table2_2]
        table2_3[table2_3]
        table2_4[table2_4]
        table3([table3])
        root_df[[root_df]]
        input_table1 ----->|left<br>- id| root_df
        input_table2 --> table1_1
        input_table2 --> table1_2
        input_table2 ---> table2_1
        input_table2 ---> table2_3
        table1_1 --> table2_1
        table1_1 --> table2_2
        table1_1 ---->|inner<br>- id| root_df
        table1_2 --> table2_2
        table1_3 --> table2_3
        table1_3 --> table2_4
        input_table2 ~~~ table1_3
        table1_3 ---->|left<br>- id1<br>- id2| root_df
        table1_4 ---> table3
        input_table2 ~~~ table1_4
        table2_3 --> table3
        table2_4 --> table3
        table3 -->|left<br>- id| root_df

    ```
    ``` 
