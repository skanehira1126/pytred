# pytred 1.0.0 に向けたテスト項目

| 対象 | 変更時に守る挙動 | 主なテスト |
| --- | --- | --- |
| `Root`、`Input`、`DataHub.__init_subclass__` | クラスごとに必要な入力と起点を識別できる。起点の重複や同じ入力に対する複数の前処理は宣言時に分かる。 | [入力宣言](test_data_hub.py)、[フック](test_input_hooks.py) |
| `DataHub.execute`、`__call__`、`root_df`、`get`、`DataNode` | 呼び出しごとに入力が入れ替わる。位置引数のノードは結合条件とメタデータを保つ。処理に失敗しても、その回に完了したテーブルを取得できる。 | [実行と再実行](test_data_hub.py) |
| `validate_raw_inputs`、`prepare_input` | 検証と前処理は生の入力を読む。すべての前処理が成功してから結果が反映される。失敗した回の入力を調べ、別の入力で再実行できる。 | [入力フックの実行と失敗](test_input_hooks.py) |
| `create_tables`、依存引数の収集、`polars_optional_table`、`EmptyDataNode` | 分岐した結果を指定順に合流できる。任意ステップは依存の欠落時にスキップし、0行の入力は処理する。スキップ後もテーブル名、結合条件、メタデータを調べられる。 | [依存解決](test_data_hub.py) |
| `steps`、`post_step`、出力フィルター | 前の結合で得た列を次の結合キーに使える。同名列を区別でき、最終フィルターは後処理で追加した列を参照できる。 | [最終出力](test_data_hub.py) |
| `table`、`polars_table`、`get_metadata`、公開例外 | 各デコレータの一意性検査の既定値を守る。複合キー全体で重複を判定し、一意性検査を無効にしてもキーの欠落を検出する。`full` 結合は両側の未一致行を残す。 | [宣言・実行・エラー](test_decorators.py) |
| `ColumnMetadata`、`TableMetadata` | 列・複合主キーの宣言順を保つ。重複した列名、型の不一致、未知のフィールド、定義後の書き換えを検出する。 | [メタデータの定義](test_metadata.py) |
| `get_datahub_metadata`、`pytred metadata` | パイプラインを生成・実行せずに宣言を読み、入力と生成テーブルを区別する。インストールしたコマンドの標準出力を JSON として読める。 | [定義の収集](helpers/test_metadata.py)、[CLI](test_cli.py) |
| `collect_dataflow_elements`、`visualize_datahub`、`render_datahub_html`、`DataflowView` | 表示しても処理は走らない。入力・前処理・結合の接続先が正しく、各表示は作成時点の状態を保つ。入力名や関数ソースに含まれる HTML はデータとして保持する。 | [グラフとノートブック表示](helpers/test_visualize.py) |

実行順のソートやデコレータの登録などの内部処理は、依存するステップの戻り値や表示される接続を通して検査する。ノートブックの自動テストは IPython が出力する HTML までを対象とする。[ブラウザーでの確認範囲](../docs/development/visualization.md#verify)
