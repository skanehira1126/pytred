# Knowledge Refinery 運用手順

この文書は `AGENTS.md` から必要な場面で参照するリポジトリの運用ルール。
利用可否とvault照合は `AGENTS.md` の条件を満たすこと。

## 検索と設定

- 現在project memoryとshared memory、現在project experienceの順に検索する。足りない場合だけ `project_ids` で選んだproject、さらに必要な場合だけ `all_projects: true` へ広げる。`project_ids` と `all_projects: true` は併用しない。
- `enabled: false` は意図的なOFFとして扱う。再有効化は利用者の明示依頼または確認がある場合だけ行う。
- 設定を修復するときは、存在する `refinery-project` スキルと文書化されたCLIを使う。
- projectの名前、概要、検索用tag、主要技術が変わった場合は、現在revisionを取得して中央vaultのproject metadataを部分更新する。目的・領域のtagはlowercase kebab-case、技術名はtechnologiesだけに保存する。

## Experienceの記録

- 意味のある検証、比較、不採用判断、失敗から知見を得た場合は `refinery-experience` スキルを使う。
- 将来のagentの選択、回避、検証、診断を変える結果だけを記録する。定型作業の完了報告、進捗log、明白なtypo修正、新しい根拠のない反復は記録しない。
- 目的、試したこと、分かったこと、微妙だった点、次の可能性を一つのexperienceにまとめる。
- 実装へ採用しなかったことやevidenceがuntrackedであることを理由に記録を捨てない。
- 新規experienceは安定したlowercase slugの `experience_id` を先に決める。結果不明のcreateをretryする前にexact getまたはID検索で保存済みか確認する。

| status | 条件 |
| --- | --- |
| `completed` | 成否を問わず、評価可能な結果が得られた |
| `inconclusive` | 根拠不足や矛盾で答えが出ない |
| `abandoned` | 評価前に停止した |
| `superseded` | 後続experienceが結論を置換した |

| confidence | 条件 |
| --- | --- |
| `high` | 条件を明記した再現可能な直接根拠がある |
| `medium` | 直接根拠はあるが反復や適用範囲が限定的 |
| `low` | 部分的・間接的な根拠、または重要な未解決点がある |

## Memoryの作成

- 複数experienceから繰り返し使える原則を抽出するときは `refinery-memory` スキルを使う。
- project memoryは原則として反復または相補的な2件以上のexperienceを根拠にする。利用者が明示依頼した場合だけ1件を許し、scopeを狭め、未検証の限界を本文へ書き、confidenceを `high` にしない。
- shared memoryは異なる2 project以上の独立した根拠があっても自動作成しない。候補の原則、scope、限界、反例、confidence、source IDを提示し、利用者の明示承認後だけ作成・昇格する。

## 更新と保守

- 既存experience/memoryの更新は現在revisionを使う。optional fieldの省略は保持、空listは明示clear、confidenceのclearは `clear_confidence: true` とする。
- 日次棚卸しでは `refinery-maintenance` スキルを使う。
- evidenceを保存するためだけにプロダクトrepoへcommitしない。プロダクトrepoとrefinery repoの変更を同じcommitやPRへ混ぜない。

## 保存する情報

secret、credential、access token、PII、顧客data、redactしていない機密logをvaultへ保存しない。logやevidenceは機密値を除去し、安全にできない場合は非機密の説明と限界だけを残す。
