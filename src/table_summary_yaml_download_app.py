import streamlit as st
import pandas as pd
import yaml
import datetime
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

st.title("テーブルサマリ YAML 生成アプリ")

# --- ユーザー入力 ---
db_name = st.text_input("対象DB名")
schema_name = st.text_input("対象スキーマ名")
table_name = st.text_input("対象テーブル名")
column_file = st.file_uploader("カラム定義ファイル（csv, table, column_typeのCSV形式）", type=["csv"])

if st.button("YAML生成") and db_name and schema_name and table_name and column_file:
    # カラム定義をDataFrameで読み込み
    column_df = pd.read_csv(column_file)
    column_df = column_df.dropna(subset=["csv", "table", "column_type"])

    # 対象カラムリストを作成
    columns = column_df[["csv", "table", "column_type"]].to_dict(orient="records")

    # テーブルの完全名
    full_table_name = f'"{db_name}"."{schema_name}"."{table_name}"'

    # 全体の行数
    count_query = f"SELECT COUNT(*) AS cnt FROM {full_table_name}"
    row_count = session.sql(count_query).collect()[0]["CNT"]

    summary = {
        f"{db_name}_{schema_name}_{table_name}": {
            "collection": {
                "count": row_count
            },
            "columns": {}
        }
    }

    for idx, col_info in enumerate(columns, start=1):
        csv_col = col_info["csv"]
        db_col = col_info["table"]
        col_type = col_info["column_type"].lower()
        column_key = f"column{idx}"
        stats = {}

        # nullを除くmin/max（型によって比較方法変更）
        if col_type == "number":
            min_max_query = f"""
                SELECT
                    MIN(TRY_TO_NUMBER({db_col})) AS min_value,
                    MAX(TRY_TO_NUMBER({db_col})) AS max_value
                FROM {full_table_name}
                WHERE {db_col} IS NOT NULL AND to_char({db_col}) <> ''
            """
        else:
            min_max_query = f"""
                SELECT
                    MIN({db_col}) AS min_value,
                    MAX({db_col}) AS max_value
                FROM {full_table_name}
                WHERE {db_col} IS NOT NULL AND to_char({db_col}) <> ''
            """
        min_max_result = session.sql(min_max_query).collect()[0]

        # null件数
        null_count_query = f"""
            SELECT COUNT(*) AS null_count
            FROM {full_table_name}
            WHERE {db_col} IS NULL OR to_char({db_col}) = ''
        """
        null_count = session.sql(null_count_query).collect()[0]["NULL_COUNT"]

        # 値の種類数と頻度（最大50件）
        freq_query = f"""
            SELECT {db_col} AS value, COUNT(*) AS count
            FROM {full_table_name}
            WHERE {db_col} IS NOT NULL AND to_char({db_col}) <> ''
            GROUP BY {db_col}
            ORDER BY count DESC
            LIMIT 51
        """
        freq_results = session.sql(freq_query).to_pandas()

        # 値種類の件数
        value_type_count_query = f"SELECT COUNT(DISTINCT {db_col}) FROM {full_table_name} WHERE {db_col} IS NOT NULL AND to_char({db_col}) <> ''"
        value_type_count = session.sql(value_type_count_query).collect()[0][0]

        stats["name"] = csv_col  # ✅ 修正ポイント：YAML出力にはcsv列を使う
        stats["min_value"] = str(min_max_result["MIN_VALUE"])
        stats["max_value"] = str(min_max_result["MAX_VALUE"])
        stats["null_count"] = null_count
        stats["value_type_count"] = value_type_count

        if value_type_count <= 50:
            for i, row in freq_results.iterrows():
                stats[f"value{i + 1}"] = {
                    "value": str(row["VALUE"]) if row["VALUE"] is not None else None,
                    "count": int(row["COUNT"])
                }

        summary[f"{db_name}_{schema_name}_{table_name}"]["columns"][column_key] = stats

    # --- YAML生成とダウンロード ---
    yaml_str = yaml.dump(summary, allow_unicode=True, sort_keys=False)
    now_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"table_summary_{db_name}_{schema_name}_{table_name}_{now_str}.yaml"

    st.download_button("YAMLをダウンロード", yaml_str, file_name=filename, mime="text/yaml")
