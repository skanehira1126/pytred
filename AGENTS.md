# pytred の開発ガイド

このファイルはリポジトリ全体の開発方針を扱う。パッケージの使い方は
`plugins/pytred/skills/pytred-usage/SKILL.md` にまとめる。
対象に適用される下位ディレクトリの指示がある場合は、その固有ルールを優先する。

## 作業と報告

- 明示された依頼を優先し、スキルの一般的な推奨から追加の承認手順を作らない。既存コードで判断できる細部は調べて進め、結果を左右する未確定の要件だけ確認する。
- 指示を理由に作業を止める場合は、根拠のファイルと該当箇所、影響する作業を示す。独立して進められる作業は続ける。
- レビューで発見した問題のレポートは必ず日本語で記載する。問題の場所、発生条件、影響を具体的に示す。
- 完了報告は変更点と検証結果を簡潔に伝える。未実施の確認は実施済みの結果と区別する。

## 実装と検証

- 実装は `src/pytred/`、テストは `tests/`、利用例は `examples/` と `docs/tutorials/` を参照する。
- Pythonの対応範囲・依存関係は `pyproject.toml`、検証コマンドは `tox.ini` と `.github/workflows/` を根拠にする。
- 挙動を変える場合は対応する公開APIと既存テストを確認し、必要な回帰テストと利用説明を更新する。
- 既存環境の `python -m pytest tests/<対象ファイル>.py` などで変更に関係する挙動を確認する。全体の互換性・静的検査が必要な変更では `tox.ini` の該当環境を使う。
- 文書・スキルだけの変更は内容、参照先、変更した実行例、利用可能なスキル・プラグイン検証器を確認する。関連する確認が通った後は、新しい失敗や未解決の懸念がなければ検証を広げない。

## リリースとインストール案内

- `latest` タグは最新の安定版リリースと同じコミットを指す。安定版を公開するときに更新し、開発途中のコミットやプレリリースには向けない。バージョン付きタグは固定して保持する。
- README、GitHub Pages、配布ガイドの通常のインストール案内は、リリースごとに書き換えなくてよい `latest` 経由の方法に統一する。再現性のためのバージョン固定例は、その目的を明記する。
- wheelのファイル名にはバージョンが含まれるため、URLのタグ部分を `latest` に置き換えるだけで更新不要になったと扱わない。案内する方法で最新の配布物を取得できることを確認する。
- リリース完了前に、`latest` の参照先と公開済み配布物を確認し、ドキュメントのインストールコマンドをクリーンな環境で実行する。`pytred --version` が公開した安定版と一致することまで検証する。

## スキルの保守

- `pytred-usage` の編集元は `plugins/pytred/skills/pytred-usage/`。`.agents/skills/pytred-usage` と `.codex/skills/pytred-usage` は同じ編集元へのリンクとして扱う。
- リポジトリ固有の開発・Knowledge Refineryルールはここで管理する。配布スキルは利用先のプロジェクトでも使えるよう、同梱ガイドと利用先のpytred環境を根拠にする。
- 適用条件は `description`、作業の進め方は `SKILL.md`、APIの詳細と例は `references/` に置き、同じ説明を複数箇所で維持しない。

<!-- knowledge-refinery:agents:start lang=jp -->
## Knowledge Refinery

- `.refinery.yaml` が `enabled: true` の場合だけ利用する。OFFを検索・記録の依頼だけで再有効化しない。
- 利用前に `knowledge-refinery project status --target <このrepoの絶対パス> --json` を確認し、`ready`・`enabled`・`vault_match` がすべてtrueの場合だけrepo-scoped toolsを使う。`project_path` には同じ絶対パスを渡す。
- vault不一致時はKnowledge Refineryの操作を止め、active vaultを報告する。`vault_id` の手編集で回避しない。
- 開発作業の開始時は [運用手順の「検索と設定」](docs/development/knowledge-refinery.md#検索と設定) に沿ってcurrent project/shared memory、current project experienceの順で検索する。
- 作業終了前に再利用できる発見があったか確認する。記録・更新・保守を行うときは [運用手順](docs/development/knowledge-refinery.md) の該当節と対応するスキルを読む。
<!-- knowledge-refinery:agents:end -->
