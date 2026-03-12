# dbt on Snowflake 中級編 🚀

**基礎編を完了した人が、実務レベルのdbtスキルを身につけるハンズオン**

---

## 📚 前提条件

```
✅ dbt-snowflake-handson（基礎編）を完了していること
✅ DBT_HANDSON.RAW にデータがロード済みであること
```

> ⚠️ 未完了の場合は先に [基礎編](https://github.com/sfc-gh-kenokizono/dbt-snowflake-handson) を完了してください

---

# Part 0: Git Repository 設定（5分）🔗

> 💡 基礎編と同じパターンです。中級編用のリポジトリを追加します。

### 公開リポジトリの場合（シンプル版）

```sql
-- 中級編リポジトリを追加
CREATE OR REPLACE GIT REPOSITORY DBT_HANDSON.INTEGRATIONS.dbt_handson_intermediate_repo
  API_INTEGRATION = github_api_integration
  ORIGIN = 'https://github.com/sfc-gh-kenokizono/dbt-snowflake-handson-intermediate.git';
```

### プライベートリポジトリの場合（PAT必要）

> 🔐 プライベートリポジトリを使う場合は、GitHub Personal Access Token (PAT) が必要です
> 基礎編でシークレットを作成済みの場合は、そのまま使えます。

**シークレット未作成の場合:**

```
1. GitHub.com → 右上アイコン → Settings
2. 左メニュー一番下 → Developer settings
3. Personal access tokens → Tokens (classic)
4. Generate new token → repo にチェック → Generate token
```

```sql
-- シークレット作成（未作成の場合）
CREATE OR REPLACE SECRET DBT_HANDSON.INTEGRATIONS.github_secret
  TYPE = password
  USERNAME = 'あなたのGitHubユーザー名'
  PASSWORD = 'ghp_xxxxx...';  -- ここにPATを貼る

-- API Integration 更新（未設定の場合）
CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/dbt-labs', 'https://github.com/sfc-gh-kenokizono')
  ALLOWED_AUTHENTICATION_SECRETS = (DBT_HANDSON.INTEGRATIONS.github_secret)
  ENABLED = TRUE;
```

```sql
-- プライベートリポジトリとして追加
CREATE OR REPLACE GIT REPOSITORY DBT_HANDSON.INTEGRATIONS.dbt_handson_intermediate_repo
  API_INTEGRATION = github_api_integration
  GIT_CREDENTIALS = DBT_HANDSON.INTEGRATIONS.github_secret
  ORIGIN = 'https://github.com/sfc-gh-kenokizono/dbt-snowflake-handson-intermediate.git';
```

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

プロジェクトのルートに `packages.yml` を作成します：

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
    
  - package: calogica/dbt_expectations
    version: 0.10.4
```

### 主要パッケージの役割

| パッケージ | 何ができる？ | 使用例 |
|-----------|------------|--------|
| dbt_utils | 便利なマクロ集 | pivot, unpivot, generate_surrogate_key |
| dbt_expectations | 高度なテスト | expect_column_values_to_be_between |

## 1-2. packages のインストール方法

> ⚠️ **重要**: Snowflake Native dbt では、`dbt deps` を**事前にローカルで実行**し、
> `dbt_packages/` フォルダをGitリポジトリにコミットしておく必要があります。

### なぜ事前インストールが必要？

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   CREATE DBT PROJECT 時にパッケージ検証が行われる                   │
│   → packages.yml があるのに dbt_packages/ がないとエラー！          │
│                                                                     │
│   解決策: ローカルで dbt deps → dbt_packages/ をコミット           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### ローカルでの準備（既に完了済み）

```bash
# ローカルで packages をインストール
cd dbt-snowflake-handson-intermediate
dbt deps

# dbt_packages/ を Git にコミット
# (.gitignore から dbt_packages/ を除外しておく)
git add dbt_packages/
git commit -m "Add dbt_packages for Snowflake Native dbt"
git push
```

### Snowflake でプロジェクト作成

```sql
-- Git リポジトリを最新に更新
ALTER GIT REPOSITORY DBT_HANDSON.INTEGRATIONS.dbt_handson_intermediate_repo FETCH;

-- dbt プロジェクトを作成（packages は既にリポジトリに含まれている）
CREATE OR REPLACE DBT PROJECT DBT_HANDSON.ANALYTICS.dbt_intermediate_project
  FROM '@DBT_HANDSON.INTEGRATIONS.dbt_handson_intermediate_repo/branches/main';
```

> 💡 **このハンズオンリポジトリは既に `dbt_packages/` がコミット済み**なので、
> 上記SQLを実行するだけでOKです！

## 1-3. dbt_utils を使ってみる

**generate_surrogate_key**: 複数カラムからユニークキーを生成

```sql
-- models/marts/dim_customers_v2.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    customer_id,
    customer_name,
    ...
FROM {{ ref('stg_customers') }}
```

**star**: 全カラムを展開（除外指定可能）

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

**macros/cents_to_dollars.sql**:

```sql
{% macro cents_to_dollars(column_name) %}
    {{ column_name }} / 100.0
{% endmacro %}
```

**使い方**:

```sql
-- models/staging/stg_orders.sql
SELECT
    id AS order_id,
    {{ cents_to_dollars('subtotal') }} AS subtotal_dollars
FROM {{ source('raw', 'raw_orders') }}
```

**展開後のSQL**:

```sql
SELECT
    id AS order_id,
    subtotal / 100.0 AS subtotal_dollars
FROM DBT_HANDSON.RAW.raw_orders
```

## 2-2. 引数付きマクロ

**macros/limit_rows.sql**:

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

## 2-4. 実践: カスタムスキーマ名マクロ

**macros/generate_schema_name.sql**:

```sql
{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name }}
    {%- endif -%}
{% endmacro %}
```

**効果**:

```
開発環境（target.schema = DEV）:
  staging → DEV_staging
  marts   → DEV_marts

本番環境（target.schema = PROD）:
  staging → PROD_staging
  marts   → PROD_marts
```

## ✅ Part 2 理解チェック

```
□ マクロの目的が分かった
  → 「再利用可能なSQLテンプレート」

□ Jinja構文が分かった
  → 「{{ }}で出力、{% %}でロジック」

□ generate_schema_name の役割が分かった
  → 「環境ごとにスキーマ名を変える」
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

**tests/assert_order_revenue_positive.sql**:

```sql
-- 注文の売上がマイナスになっていないかチェック
-- 0件返れば成功、1件以上返ればテスト失敗

SELECT
    order_id,
    order_total
FROM {{ ref('fct_orders') }}
WHERE order_total < 0
```

**実行**:

```sql
EXECUTE DBT PROJECT DBT_HANDSON.ANALYTICS.dbt_intermediate_project
  ARGS = 'test --select assert_order_revenue_positive';
```

## 3-2. カスタムgenericテスト（再利用可能）

**macros/test_positive_value.sql**:

```sql
{% test positive_value(model, column_name) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

**YAMLで使う**:

```yaml
# models/marts/_marts_models.yml
models:
  - name: fct_orders
    columns:
      - name: order_total
        data_tests:
          - positive_value  # 👈 カスタムテスト！
```

## 3-3. dbt_expectations を使う

**より表現力のあるテスト**:

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_total
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000
              
      - name: order_date
        data_tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: DATE
              
  - name: dim_customers
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 100
          max_value: 10000
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

# Part 4: 環境分離（15分）🔀

## 🎯 今学ぶこと

**開発環境と本番環境を安全に分離する方法**

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

## 4-1. profiles.yml で環境を定義

```yaml
dbt_intermediate_project:
  target: dev  # デフォルトは dev
  
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: DEV_ROLE
      database: DBT_HANDSON
      warehouse: COMPUTE_WH
      schema: DEV
      
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: PROD_ROLE
      database: DBT_HANDSON
      warehouse: COMPUTE_WH
      schema: PROD
```

## 4-2. target を使った条件分岐

**macros/limit_rows.sql**:

```sql
{% macro limit_rows(limit_count=100) %}
    {% if target.name == 'dev' %}
        LIMIT {{ limit_count }}
    {% endif %}
{% endmacro %}
```

**models/marts/fct_orders.sql**:

```sql
SELECT
    ...
FROM {{ ref('stg_orders') }}
{{ limit_rows(1000) }}
```

→ `dbt run --target dev` → LIMIT 1000 がつく
→ `dbt run --target prod` → LIMIT なし（全件）

## 4-3. Snowsight での環境切り替え

```sql
-- 開発環境で実行（デフォルト）
EXECUTE DBT PROJECT DBT_HANDSON.ANALYTICS.dbt_intermediate_project
  ARGS = 'run';

-- 本番環境で実行（--target prod）
EXECUTE DBT PROJECT DBT_HANDSON.ANALYTICS.dbt_intermediate_project
  ARGS = 'run --target prod';
```

## 4-4. --defer で開発を高速化

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   --defer がない場合 😫                                            │
│   ─────────────────────                                            │
│                                                                     │
│   dim_customers だけ直したいのに...                                │
│   → 依存する stg_*, int_* を全部ビルド                             │
│   → 10分かかる！                                                   │
│                                                                     │
│   --defer がある場合 😊                                            │
│   ─────────────────────                                            │
│                                                                     │
│   dim_customers だけ直したい！                                     │
│   → 依存モデルは本番のを参照                                       │
│   → 1分で完了！                                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

```sql
-- 本番の成果物を参照しながら、dim_customers だけビルド
EXECUTE DBT PROJECT DBT_HANDSON.ANALYTICS.dbt_intermediate_project
  ARGS = 'run --select dim_customers --defer --state prod-artifacts';
```

## ✅ Part 4 理解チェック

```
□ 環境分離の必要性が分かった
  → 「開発のミスから本番を守る」

□ target の使い方が分かった
  → 「dev/prod で条件分岐できる」

□ --defer の効果が分かった
  → 「開発時のビルド時間を短縮」
```

---

# Part 5: CI/CD（20分）🔄

## 🎯 今学ぶこと

**Pull Request 時に自動でテストを実行する仕組み**

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

## 5-1. GitHub Actions ワークフロー

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

## 5-2. CI用の target を追加

**profiles.yml に追加**:

```yaml
    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: CI_ROLE
      database: DBT_HANDSON
      warehouse: COMPUTE_WH
      schema: CI_{{ env_var('GITHUB_RUN_ID', 'local') }}
```

→ CIごとにユニークなスキーマを作成！

## 5-3. GitHub Secrets の設定

```
リポジトリ Settings → Secrets and variables → Actions

SNOWFLAKE_ACCOUNT: xxx.ap-northeast-1.aws
SNOWFLAKE_USER: CI_USER
SNOWFLAKE_PASSWORD: ********
```

## 5-4. PR時の動作確認

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

## ✅ Part 5 理解チェック

```
□ CI/CD の目的が分かった
  → 「マージ前にテストを自動実行」

□ GitHub Actions の基本構造が分かった
  → 「on: でトリガー、jobs: で処理」

□ CI用スキーマの必要性が分かった
  → 「PRごとに独立した環境でテスト」
```

---

# ✅ 中級編 完了チェックリスト

```
□ packages を導入できる
  → 「packages.yml に書いて dbt deps」

□ マクロを作成できる
  → 「{% macro %} で再利用可能なSQL」

□ カスタムテストを書ける
  → 「SQL or generic test」

□ 環境分離を設計できる
  → 「target で dev/prod 切り替え」

□ CI/CD を構築できる
  → 「GitHub Actions で自動テスト」
```

---

# 🎓 次のステップ

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  中級編で登ったレベル ✅                                           │
│  ──────────────────────                                            │
│                                                                     │
│  ✅ packages / マクロ                                              │
│  ✅ カスタムテスト                                                 │
│  ✅ 環境分離                                                       │
│  ✅ CI/CD                                                          │
│                                                                     │
│  ─────────────────────────────────────────────────────────────     │
│                                                                     │
│  さらに上を目指すなら...                                           │
│  ──────────────────────                                            │
│                                                                     │
│  📖 snapshots（履歴管理）                                          │
│  📖 exposures（ダッシュボード連携）                                │
│  📖 dbt Mesh（複数プロジェクト連携）                               │
│  📖 Semantic Layer（メトリクス定義）                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```
