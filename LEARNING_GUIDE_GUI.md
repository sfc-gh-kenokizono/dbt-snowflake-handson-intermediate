# dbt on Snowflake 中級編【GUI版】🚀

**基礎編（GUI版）を完了した人が、実務レベルのdbtスキルを身につけるハンズオン**

---

## ⚠️ 重要: GUI版の制約事項

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  GUI版 dbt プロジェクトの制約                                      │
│  ─────────────────────────────                                      │
│                                                                     │
│  ❌ EXECUTE DBT PROJECT が使えない                                 │
│     → GUI版プロジェクトはSQLコマンドから参照できません             │
│     → TASKでのスケジュール実行も不可                               │
│                                                                     │
│  ❌ CI/CD（GitHub Actions）が使えない                              │
│     → ソースコードがGitHubにないため                               │
│     → Part 5 は「理解編」として読むのみ                            │
│                                                                     │
│  ✅ packages（dbt_utils等）は使える                                │
│  ✅ マクロは作成・使用できる                                       │
│  ✅ カスタムテストは作成・実行できる                               │
│  ✅ 環境分離の概念は学べる（実践は制限あり）                       │
│                                                                     │
│  💡 本番運用には Git連携版 をお勧めします                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📚 前提条件

```
✅ dbt-snowflake-handson【GUI版】（基礎編）を完了していること
✅ DBT_HANDSON_GUI.RAW にデータがロード済みであること
✅ DBT_HANDSON_GUI.ANALYTICS に handson_gui_project が作成済みであること
```

> ⚠️ 未完了の場合は先に [基礎編 GUI版](LEARNING_GUIDE_GUI.md) を完了してください

---

# Part 1: packages（15分）📦

## 🎯 今学ぶこと

**dbtコミュニティが作った便利な関数を使う方法**

## 💡 ポイント: なぜ packages が必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   packages がない場合 😫                                           │
│   ────────────────────                                              │
│   ・よく使う処理を毎回自分で書く                                   │
│   ・「カラムをピボットしたい」→ 複雑なSQL書く                      │
│   ・「NULL除外したい」→ COALESCE を毎回書く                        │
│                                                                     │
│   packages がある場合 😊                                           │
│   ────────────────────                                              │
│   ・dbt_utils.pivot() で一発                                       │
│   ・dbt_utils.safe_cast() で安全な型変換                           │
│   ・dbt_expectations で高度なテスト                                 │
│                                                                     │
│   💡 車輪の再発明をしない！                                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 1-1. packages.yml を作成

1. プロジェクトのルートで右クリック → **「新しいファイル」** → `packages.yml`
2. 以下をコピペ：

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
    
  - package: calogica/dbt_expectations
    version: 0.10.4
```

3. **Ctrl+S (Cmd+S)** で保存

### 主要パッケージの役割

| パッケージ | 何ができる？ | 使用例 |
|-----------|------------|--------|
| dbt_utils | 便利なマクロ集 | pivot, unpivot, generate_surrogate_key |
| dbt_expectations | 高度なテスト | expect_column_values_to_be_between |

## 1-2. packages をインストール（GUI版）

1. 画面右上の **「Run」** 横の **▼** をクリック
2. **「Install packages」** を選択（または `dbt deps` をターミナルで実行）

**期待される結果**:
```
Installing dbt-labs/dbt_utils
Installing calogica/dbt_expectations
  Installed from version 0.10.4
```

## 1-3. dbt_utils を使ってみる

### generate_surrogate_key: 複数カラムからユニークキーを生成

`marts/dim_customers.sql` を編集して、先頭のSELECT文に追加：

```sql
{{
    config(
        materialized='table'
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- ... 既存のコード ...

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_sk,  -- 👈 追加！
    c.customer_id,
    c.customer_name,
    -- ... 残りのカラム ...
```

**効果**: `customer_id` からハッシュ値のサロゲートキーを自動生成

### star: 全カラムを展開（除外指定可能）

```sql
-- 特定カラム以外を全部SELECT
SELECT
    {{ dbt_utils.star(ref('stg_orders'), except=['_loaded_at']) }}
FROM {{ ref('stg_orders') }}
```

## ✅ Part 1 理解チェック

```
□ packages.yml の役割が分かった
  → 「外部パッケージを定義するファイル」

□ dbt deps の役割が分かった
  → 「packages.yml に書いたパッケージをインストール」

□ dbt_utils の便利さが分かった
  → 「車輪の再発明をしなくて済む」
```

---

# Part 2: マクロ（20分）🔧

## 🎯 今学ぶこと

**再利用可能なSQLテンプレートを作る方法**

## 💡 ポイント: なぜマクロが必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   マクロがない場合 😫                                              │
│   ──────────────────                                                │
│                                                                     │
│   stg_orders.sql:     subtotal / 100.0 AS subtotal_dollars         │
│   stg_products.sql:   price / 100.0 AS price_dollars               │
│   stg_supplies.sql:   cost / 100.0 AS cost_dollars                 │
│                                                                     │
│   → 同じ「セント→ドル変換」を3回書いてる！                         │
│   → 変換ロジック変えたい時に3箇所修正...                           │
│                                                                     │
│   マクロがある場合 😊                                              │
│   ──────────────────                                                │
│                                                                     │
│   {{ cents_to_dollars('subtotal') }} AS subtotal_dollars           │
│   {{ cents_to_dollars('price') }} AS price_dollars                 │
│   {{ cents_to_dollars('cost') }} AS cost_dollars                   │
│                                                                     │
│   → マクロ1つ直せば全部直る！                                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 2-1. シンプルなマクロを作る

1. プロジェクトのルートで右クリック → **「新しいフォルダ」** → `macros`
2. `macros/` を右クリック → **「新しいファイル」** → `cents_to_dollars.sql`
3. 以下をコピペ：

```sql
{% macro cents_to_dollars(column_name) %}
    {{ column_name }} / 100.0
{% endmacro %}
```

**使い方**（stg_orders.sql などで）:

```sql
SELECT
    id AS order_id,
    {{ cents_to_dollars('subtotal') }} AS subtotal_dollars
FROM DBT_HANDSON_GUI.RAW.raw_orders
```

**展開後のSQL**:

```sql
SELECT
    id AS order_id,
    subtotal / 100.0 AS subtotal_dollars
FROM DBT_HANDSON_GUI.RAW.raw_orders
```

## 2-2. 引数付きマクロ

`macros/limit_rows.sql` を作成：

```sql
{% macro limit_rows(limit_count=100) %}
    {% if target.name == 'dev' %}
        LIMIT {{ limit_count }}
    {% endif %}
{% endmacro %}
```

**使い方**:

```sql
SELECT * FROM {{ ref('stg_orders') }}
{{ limit_rows(1000) }}
```

→ 開発環境では `LIMIT 1000` が付く、本番では付かない！

> ⚠️ **GUI版の制約**: GUI版ではtargetの切り替えが制限されるため、この機能はGit連携版で真価を発揮します。

## 2-3. Jinja の基本構文

| 構文 | 用途 | 例 |
|------|------|-----|
| `{{ }}` | 値を出力 | `{{ column_name }}` |
| `{% %}` | ロジック実行 | `{% if condition %}` |
| `{# #}` | コメント | `{# これはコメント #}` |

**条件分岐の例**:

```sql
{% macro get_date_column() %}
    {% if target.name == 'prod' %}
        CURRENT_DATE()
    {% else %}
        '2024-01-01'::DATE
    {% endif %}
{% endmacro %}
```

## ✅ Part 2 理解チェック

```
□ マクロの目的が分かった
  → 「再利用可能なSQLテンプレート」

□ Jinja構文が分かった
  → 「{{ }}で出力、{% %}でロジック」

□ マクロでDRY原則を実現できる
  → 「同じ処理を何度も書かない」
```

---

# Part 3: 高度なテスト（15分）🧪

## 🎯 今学ぶこと

**YAMLだけでは書けない複雑なテストの作り方**

## 💡 ポイント: なぜカスタムテストが必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   標準テスト（YAML）                カスタムテスト                  │
│   ─────────────────                ──────────────                   │
│                                                                     │
│   unique: 重複チェック             複数テーブル跨ぎチェック        │
│   not_null: NULL チェック          計算結果の整合性チェック        │
│   accepted_values: 値制限          ビジネスルール検証              │
│                                                                     │
│   → シンプルなチェック向き        → 複雑なチェック向き            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 3-1. カスタムテスト（SQLファイル）

1. プロジェクトのルートで右クリック → **「新しいフォルダ」** → `tests`
2. `tests/` を右クリック → **「新しいファイル」** → `assert_order_total_positive.sql`
3. 以下をコピペ：

```sql
-- 注文の売上がマイナスになっていないかチェック
-- 0件返れば成功、1件以上返ればテスト失敗

SELECT
    order_id,
    order_total
FROM {{ ref('fct_orders') }}
WHERE order_total < 0
```

**実行**: 画面右上の **▼** → **「Test」**

## 3-2. カスタムgenericテスト（再利用可能）

`macros/test_positive_value.sql` を作成：

```sql
{% test positive_value(model, column_name) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

**YAMLで使う**（`models/schema.yml` に追加）:

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_total
        tests:
          - positive_value  # 👈 カスタムテスト！
```

## 3-3. dbt_expectations を使う

**より表現力のあるテスト**:

`models/schema.yml` を更新：

```yaml
version: 2

models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: order_total
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100000
              
      - name: order_date
        tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: DATE
              
  - name: dim_customers
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 100
          max_value: 10000
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
```

### dbt_expectations の便利なテスト

| テスト | 用途 |
|--------|------|
| expect_column_values_to_be_between | 値の範囲チェック |
| expect_column_values_to_match_regex | 正規表現マッチ |
| expect_table_row_count_to_be_between | 行数の範囲チェック |
| expect_column_pair_values_to_be_equal | 2カラムの値が等しいか |

## ✅ Part 3 理解チェック

```
□ カスタムテスト（SQL）の書き方が分かった
  → 「0件返れば成功、1件以上で失敗」

□ カスタムgenericテストが分かった
  → 「マクロとして定義してYAMLから呼び出す」

□ dbt_expectations の便利さが分かった
  → 「標準より表現力のあるテストが書ける」
```

---

# Part 4: 環境分離（15分）🔀【概念理解編】

> ⚠️ **GUI版の制約**: GUI版dbtプロジェクトでは環境分離の実践が制限されます。
> このPartは概念を理解するための「読み物」として進めてください。
> 実践したい場合は **Git連携版** を使用してください。

## 🎯 今学ぶこと

**開発環境と本番環境を安全に分離する方法（概念）**

## 💡 ポイント: なぜ環境分離が必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   環境分離がない場合 😱                                            │
│   ────────────────────                                              │
│                                                                     │
│   開発中のミスが本番データを壊す！                                 │
│   「あ、間違えてPRODのテーブルDROPしちゃった...」                  │
│                                                                     │
│   環境分離がある場合 😊                                            │
│   ────────────────────                                              │
│                                                                     │
│   DEV_analytics.dim_customers  ← 開発者が自由に触れる              │
│   PROD_analytics.dim_customers ← 本番、厳重管理                    │
│                                                                     │
│   → 開発で何やっても本番は安全！                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 4-1. profiles.yml で環境を定義（Git連携版の場合）

```yaml
dbt_project:
  target: dev  # デフォルトは dev
  
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      role: DEV_ROLE
      database: DBT_HANDSON
      warehouse: COMPUTE_WH
      schema: DEV
      
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      role: PROD_ROLE
      database: DBT_HANDSON
      warehouse: COMPUTE_WH
      schema: PROD
```

## 4-2. target を使った条件分岐

```sql
{% macro limit_rows(limit_count=100) %}
    {% if target.name == 'dev' %}
        LIMIT {{ limit_count }}
    {% endif %}
{% endmacro %}
```

**効果**:
- `dbt run --target dev` → LIMIT 1000 がつく
- `dbt run --target prod` → LIMIT なし（全件）

## 4-3. GUI版での制約

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   ⚠️ GUI版の制約                                                   │
│   ────────────────                                                  │
│                                                                     │
│   GUI版では:                                                        │
│   ・profiles.yml は作成できるが、target切り替えが制限される        │
│   ・EXECUTE DBT PROJECT ... --target prod が使えない               │
│   ・環境ごとに別のGUIプロジェクトを作る必要がある                  │
│                                                                     │
│   Git連携版では:                                                    │
│   ・EXECUTE DBT PROJECT ... ARGS = 'run --target prod' が可能      │
│   ・同一プロジェクトで環境切り替え可能                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## ✅ Part 4 理解チェック

```
□ 環境分離の必要性が分かった
  → 「開発のミスから本番を守る」

□ target の概念が分かった
  → 「dev/prod で条件分岐できる」

□ GUI版の制約を理解した
  → 「本格的な環境分離にはGit連携版が必要」
```

---

# Part 5: CI/CD（20分）🔄【概念理解編】

> ⚠️ **GUI版では実践不可**: CI/CDはGitHubとの連携が前提のため、GUI版では実践できません。
> このPartは「なぜCI/CDが必要か」を理解するための読み物です。
> 実践したい場合は **Git連携版** を使用してください。

## 🎯 今学ぶこと

**Pull Request 時に自動でテストを実行する仕組み（概念）**

## 💡 ポイント: なぜ CI/CD が必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   CI/CD がない場合 😫                                              │
│   ──────────────────                                                │
│                                                                     │
│   「レビューお願いします」                                         │
│   「LGTM！マージ！」                                               │
│   → デプロイ後にエラー発覚 💥                                      │
│   「なんでテストしなかったの？」                                   │
│                                                                     │
│   CI/CD がある場合 😊                                              │
│   ──────────────────                                                │
│                                                                     │
│   PR作成 → 自動で dbt build                                        │
│   テスト失敗 → マージ不可 🚫                                       │
│   テスト成功 → 安心してマージ ✅                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 5-1. GitHub Actions ワークフロー（参考）

**.github/workflows/dbt_ci.yml**:

```yaml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'models/**'
      - 'macros/**'
      - 'tests/**'
      - 'dbt_project.yml'
      - 'packages.yml'

jobs:
  dbt-build:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          
      - name: Install dbt
        run: pip install dbt-snowflake
        
      - name: Install packages
        run: dbt deps
        
      - name: Run dbt build
        run: dbt build --target ci
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

## 5-2. CI/CD の流れ

```
1. feature ブランチを作成
2. models/ を修正
3. PR を作成
4. GitHub Actions が自動実行
5. テスト結果が PR に表示

   ✅ All checks have passed
   または
   ❌ dbt build failed
```

## 5-3. GUI版 vs Git連携版 の比較

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   GUI版                           Git連携版                         │
│   ─────                           ─────────                         │
│                                                                     │
│   ❌ CI/CD 不可                   ✅ GitHub Actions で自動テスト    │
│   ❌ バージョン管理なし           ✅ Git で履歴管理                 │
│   ❌ コードレビュー不可           ✅ PR でレビュー可能              │
│   ❌ ロールバック困難             ✅ 簡単にロールバック             │
│                                                                     │
│   👉 学習・PoC向け               👉 本番運用向け                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## ✅ Part 5 理解チェック

```
□ CI/CD の目的が分かった
  → 「マージ前にテストを自動実行」

□ GitHub Actions の基本構造が分かった
  → 「on: でトリガー、jobs: で処理」

□ GUI版の限界を理解した
  → 「本番運用にはGit連携が必要」
```

---

# ✅ 中級編（GUI版）完了チェックリスト

```
□ packages を導入できる
  → 「packages.yml に書いて dbt deps」

□ マクロを作成できる
  → 「{% macro %} で再利用可能なSQL」

□ カスタムテストを書ける
  → 「SQL or generic test」

□ 環境分離の概念を理解した
  → 「target で dev/prod 切り替え」（Git連携版で実践）

□ CI/CD の必要性を理解した
  → 「GitHub Actions で自動テスト」（Git連携版で実践）
```

---

# 📊 GUI版 vs Git連携版 総合比較

| 機能 | GUI版 | Git連携版 |
|------|-------|----------|
| packages | ✅ 使える | ✅ 使える |
| マクロ | ✅ 使える | ✅ 使える |
| カスタムテスト | ✅ 使える | ✅ 使える |
| 環境分離（target） | ⚠️ 制限あり | ✅ フル機能 |
| CI/CD | ❌ 不可 | ✅ GitHub Actions |
| TASKスケジュール | ❌ 不可 | ✅ 可能 |
| バージョン管理 | ❌ なし | ✅ Git |
| チーム開発 | ⚠️ 困難 | ✅ PR/レビュー可能 |

---

# 🎓 次のステップ

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  GUI版で学べたこと ✅                                              │
│  ────────────────────                                               │
│                                                                     │
│  ✅ packages / マクロ                                              │
│  ✅ カスタムテスト                                                 │
│  ✅ 環境分離の概念                                                 │
│  ✅ CI/CD の概念                                                   │
│                                                                     │
│  ─────────────────────────────────────────────────────────────     │
│                                                                     │
│  本番運用を目指すなら...                                           │
│  ─────────────────────                                              │
│                                                                     │
│  📖 Git連携版（LEARNING_GUIDE.md）に移行                           │
│     → 環境分離、CI/CD、スケジュール実行を実践                      │
│                                                                     │
│  さらに上を目指すなら...                                           │
│  ──────────────────────                                            │
│                                                                     │
│  📖 incremental（差分更新）                                        │
│  📖 snapshots（履歴管理）                                          │
│  📖 exposures（ダッシュボード連携）                                │
│  📖 dbt Mesh（複数プロジェクト連携）                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

**お疲れ様でした！** GUI版での中級編学習が完了しました。
本番運用を目指す場合は、ぜひGit連携版に挑戦してください！
