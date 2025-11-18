"""
データエクスプローラービュー: 取得したデータの詳細表示
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_data_explorer():
    """データエクスプローラーのメイン表示"""

    st.title("🔍 データエクスプローラー")

    st.markdown("""
    取得したデータを詳しく確認できます。
    実際のレースデータ、馬データ、血統データなどを閲覧・検索できます。
    """)

    st.markdown("---")

    # タブで各データ種類を分ける
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🏇 レースデータ",
        "🐴 馬データ",
        "🧬 血統データ",
        "📋 出馬表データ",
        "📊 統計情報"
    ])

    with tab1:
        render_races_explorer()

    with tab2:
        render_horses_explorer()

    with tab3:
        render_pedigrees_explorer()

    with tab4:
        render_shutuba_explorer()

    with tab5:
        render_statistics()


def render_races_explorer():
    """レースデータエクスプローラー"""

    st.markdown("### 🏇 レースデータ")

    races_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"

    if not races_parquet.exists():
        st.warning("⚠️ レースデータがまだありません。データパイプラインを実行してください。")
        return

    try:
        # データ読み込み
        df = pd.read_parquet(races_parquet)

        st.success(f"✅ {len(df):,}件のレースデータが見つかりました")

        # フィルタリングオプション
        col1, col2, col3 = st.columns(3)

        with col1:
            if "venue" in df.columns:
                venues = ["すべて"] + sorted(df["venue"].dropna().unique().tolist())
                selected_venue = st.selectbox("競馬場", venues, key="race_venue_filter")

        with col2:
            if "race_date" in df.columns:
                df["race_date"] = pd.to_datetime(df["race_date"], errors='coerce')
                min_date = df["race_date"].min()
                max_date = df["race_date"].max()

                if pd.notna(min_date) and pd.notna(max_date):
                    date_range = st.date_input(
                        "日付範囲",
                        value=(min_date, max_date),
                        key="race_date_filter"
                    )

        with col3:
            if "track_surface" in df.columns:
                surfaces = ["すべて"] + sorted(df["track_surface"].dropna().unique().tolist())
                selected_surface = st.selectbox("馬場", surfaces, key="race_surface_filter")

        # フィルタリング適用
        filtered_df = df.copy()

        if selected_venue != "すべて":
            filtered_df = filtered_df[filtered_df["venue"] == selected_venue]

        if selected_surface != "すべて":
            filtered_df = filtered_df[filtered_df["track_surface"] == selected_surface]

        if "race_date" in df.columns and len(date_range) == 2:
            filtered_df = filtered_df[
                (filtered_df["race_date"] >= pd.Timestamp(date_range[0])) &
                (filtered_df["race_date"] <= pd.Timestamp(date_range[1]))
            ]

        st.info(f"📊 フィルタ後: {len(filtered_df):,}件")

        # データテーブル表示（最初の100件）
        st.markdown("#### データプレビュー（最新100件）")

        display_cols = [
            "race_id", "race_date", "venue", "race_name",
            "track_surface", "distance_m", "weather", "track_condition",
            "head_count", "prize_1st"
        ]

        # 存在するカラムのみ選択
        available_cols = [col for col in display_cols if col in filtered_df.columns]

        st.dataframe(
            filtered_df[available_cols].head(100),
            use_container_width=True,
            hide_index=True
        )

        # データのダウンロード
        st.markdown("#### データをダウンロード")
        csv_data = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 CSVでダウンロード",
            data=csv_data,
            file_name="races_data.csv",
            mime="text/csv"
        )

        # カラム情報
        with st.expander("📋 カラム情報"):
            col_info = []
            for col in filtered_df.columns:
                col_info.append({
                    "カラム名": col,
                    "データ型": str(filtered_df[col].dtype),
                    "欠損数": filtered_df[col].isna().sum(),
                    "ユニーク数": filtered_df[col].nunique()
                })

            st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"❌ データ読み込みエラー: {str(e)}")


def render_horses_explorer():
    """馬データエクスプローラー"""

    st.markdown("### 🐴 馬データ")

    horses_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "horses" / "horses.parquet"

    if not horses_parquet.exists():
        st.warning("⚠️ 馬データがまだありません。データパイプラインを実行してください。")
        return

    try:
        df = pd.read_parquet(horses_parquet)

        st.success(f"✅ {len(df):,}頭の馬データが見つかりました")

        # 検索機能
        search_term = st.text_input("🔍 馬名で検索", key="horse_search")

        if search_term:
            if "horse_name" in df.columns:
                filtered_df = df[df["horse_name"].str.contains(search_term, case=False, na=False)]
                st.info(f"📊 検索結果: {len(filtered_df):,}頭")
            else:
                filtered_df = df
        else:
            filtered_df = df

        # データプレビュー
        st.markdown("#### データプレビュー（最新100件）")

        display_cols = [
            "horse_id", "horse_name", "sex", "birth_date",
            "breeder", "trainer_name", "owner_name"
        ]

        available_cols = [col for col in display_cols if col in filtered_df.columns]

        st.dataframe(
            filtered_df[available_cols].head(100),
            use_container_width=True,
            hide_index=True
        )

        # 統計情報
        if "sex" in df.columns:
            st.markdown("#### 性別分布")
            sex_counts = df["sex"].value_counts()
            col1, col2, col3 = st.columns(3)

            for i, (sex, count) in enumerate(sex_counts.items()):
                with [col1, col2, col3][i % 3]:
                    st.metric(sex, f"{count:,}頭")

    except Exception as e:
        st.error(f"❌ データ読み込みエラー: {str(e)}")


def render_pedigrees_explorer():
    """血統データエクスプローラー"""

    st.markdown("### 🧬 血統データ")

    pedigrees_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees" / "pedigrees.parquet"

    if not pedigrees_parquet.exists():
        st.warning("⚠️ 血統データがまだありません。データパイプラインを実行してください。")
        return

    try:
        df = pd.read_parquet(pedigrees_parquet)

        st.success(f"✅ {len(df):,}件の血統データ（5世代）が見つかりました")

        # 世代別フィルタ
        if "generation" in df.columns:
            generation = st.selectbox(
                "世代",
                options=sorted(df["generation"].unique()),
                key="pedigree_generation_filter"
            )

            filtered_df = df[df["generation"] == generation]

            st.info(f"📊 第{generation}世代: {len(filtered_df):,}件")

            # データプレビュー
            st.markdown("#### データプレビュー（最新100件）")

            display_cols = [
                "horse_id", "generation", "ancestor_id", "ancestor_name",
                "coat_color", "birth_year"
            ]

            available_cols = [col for col in display_cols if col in filtered_df.columns]

            st.dataframe(
                filtered_df[available_cols].head(100),
                use_container_width=True,
                hide_index=True
            )

    except Exception as e:
        st.error(f"❌ データ読み込みエラー: {str(e)}")


def render_shutuba_explorer():
    """出馬表データエクスプローラー"""

    st.markdown("### 📋 出馬表データ")

    shutuba_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "shutuba" / "shutuba.parquet"

    if not shutuba_parquet.exists():
        st.warning("⚠️ 出馬表データがまだありません。データパイプラインを実行してください。")
        return

    try:
        df = pd.read_parquet(shutuba_parquet)

        st.success(f"✅ {len(df):,}件の出馬表データが見つかりました")

        # データプレビュー
        st.markdown("#### データプレビュー（最新100件）")

        display_cols = [
            "race_id", "bracket_number", "horse_number", "horse_id", "horse_name",
            "sex_age", "jockey_id", "jockey_name", "basis_weight", "morning_odds"
        ]

        available_cols = [col for col in display_cols if col in df.columns]

        st.dataframe(
            df[available_cols].head(100),
            use_container_width=True,
            hide_index=True
        )

    except Exception as e:
        st.error(f"❌ データ読み込みエラー: {str(e)}")


def render_statistics():
    """統計情報"""

    st.markdown("### 📊 データ統計情報")

    stats = {}

    # レースデータ統計
    races_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    if races_parquet.exists():
        try:
            df = pd.read_parquet(races_parquet)
            stats["races"] = {
                "総レース数": len(df),
                "カラム数": len(df.columns),
                "データ期間": f"{df['race_date'].min()} ～ {df['race_date'].max()}" if "race_date" in df.columns else "N/A",
                "競馬場数": df["venue"].nunique() if "venue" in df.columns else "N/A",
            }
        except:
            pass

    # 馬データ統計
    horses_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "horses" / "horses.parquet"
    if horses_parquet.exists():
        try:
            df = pd.read_parquet(horses_parquet)
            stats["horses"] = {
                "総馬数": len(df),
                "カラム数": len(df.columns),
            }
        except:
            pass

    # 血統データ統計
    pedigrees_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees" / "pedigrees.parquet"
    if pedigrees_parquet.exists():
        try:
            df = pd.read_parquet(pedigrees_parquet)
            stats["pedigrees"] = {
                "総レコード数": len(df),
                "世代数": df["generation"].nunique() if "generation" in df.columns else "N/A",
            }
        except:
            pass

    # 表示
    if not stats:
        st.warning("⚠️ データがまだありません。")
        return

    for data_type, data_stats in stats.items():
        st.markdown(f"#### {data_type.capitalize()}データ")
        cols = st.columns(len(data_stats))
        for i, (key, value) in enumerate(data_stats.items()):
            with cols[i]:
                st.metric(key, value)

        st.markdown("---")
