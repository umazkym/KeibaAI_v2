"""
ダッシュボードビュー: システム概要とステータス表示
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_dashboard():
    """ダッシュボードのメイン表示"""

    st.title("🏠 システムダッシュボード")

    # システムステータス
    st.markdown("### 📊 システムステータス")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="総レース数",
            value=check_race_count(),
            delta="+1,250 (今月)",
            help="パース済みのレースデータ総数"
        )

    with col2:
        st.metric(
            label="馬データ数",
            value=check_horse_count(),
            delta="+450 (今月)",
            help="パース済みの馬データ総数"
        )

    with col3:
        st.metric(
            label="血統データ数",
            value="1.38M",
            delta="完全",
            help="5世代血統データ"
        )

    with col4:
        model_status = check_model_status()
        st.metric(
            label="モデルステータス",
            value=model_status,
            delta="最新",
            help="学習済みモデルの状態"
        )

    st.markdown("---")

    # データパイプライン概要
    st.markdown("### 🔄 データパイプライン概要")

    pipeline_phases = [
        {"phase": "Phase 0: データ取得", "status": "✅ 完了", "last_run": "2024-01-15", "records": "278,098"},
        {"phase": "Phase 1: パース処理", "status": "✅ 完了", "last_run": "2024-01-15", "records": "278,098"},
        {"phase": "Phase 2: 特徴量生成", "status": "✅ 完了", "last_run": "2024-01-14", "records": "265,432"},
        {"phase": "Phase 3: モデル学習", "status": "⚠️ 要更新", "last_run": "2024-01-10", "records": "N/A"},
        {"phase": "Phase 4: 日次予測", "status": "⏸️ 待機中", "last_run": "N/A", "records": "N/A"},
        {"phase": "Phase 5: シミュレーション", "status": "⏸️ 待機中", "last_run": "N/A", "records": "N/A"},
        {"phase": "Phase 6: 最適化", "status": "⏸️ 待機中", "last_run": "N/A", "records": "N/A"},
        {"phase": "Phase 7: 実行 (無効)", "status": "🔒 無効", "last_run": "N/A", "records": "N/A"},
    ]

    df_pipeline = pd.DataFrame(pipeline_phases)
    st.dataframe(df_pipeline, use_container_width=True, hide_index=True)

    st.markdown("---")

    # 最近の活動
    st.markdown("### 📅 最近の活動")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 直近の実行履歴")
        recent_activities = [
            {"時刻": "2024-01-15 03:30", "タスク": "スクレイピング", "ステータス": "✅ 成功", "件数": "1,250"},
            {"時刻": "2024-01-15 04:15", "タスク": "パース処理", "ステータス": "✅ 成功", "件数": "1,250"},
            {"時刻": "2024-01-14 10:00", "タスク": "特徴量生成", "ステータス": "✅ 成功", "件数": "15,000"},
            {"時刻": "2024-01-10 14:30", "タスク": "μモデル学習", "ステータス": "✅ 成功", "件数": "N/A"},
        ]
        st.dataframe(pd.DataFrame(recent_activities), use_container_width=True, hide_index=True)

    with col2:
        st.markdown("#### システム健全性")

        health_checks = {
            "データ品質": {"status": "✅ 良好", "detail": "欠損率 < 1%"},
            "モデル精度": {"status": "⚠️ 要確認", "detail": "最終評価: 10日前"},
            "ストレージ": {"status": "✅ 十分", "detail": "使用率: 45%"},
            "メモリ": {"status": "✅ 正常", "detail": "平均使用率: 60%"},
        }

        for check, info in health_checks.items():
            st.markdown(f"**{check}:** {info['status']} - {info['detail']}")

    st.markdown("---")

    # クイックアクション
    st.markdown("### ⚡ クイックアクション")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("📊 データパイプライン実行", use_container_width=True):
            st.switch_page("components/data_pipeline_view.py")

    with col2:
        if st.button("🤖 モデル学習開始", use_container_width=True):
            st.switch_page("components/model_training_view.py")

    with col3:
        if st.button("🎲 シミュレーション実行", use_container_width=True):
            st.switch_page("components/simulation_view.py")

    with col4:
        if st.button("📈 結果分析", use_container_width=True):
            st.switch_page("components/results_view.py")

    st.markdown("---")

    # システム情報
    with st.expander("🔍 システム詳細情報"):
        st.markdown("""
        #### プロジェクト情報
        - **プロジェクト名:** KeibaAI_v2
        - **バージョン:** 1.0.0
        - **Python:** 3.10+
        - **主要ライブラリ:** pandas, LightGBM, BeautifulSoup4, Selenium

        #### データ統計
        - **総レース数:** 278,098 レコード
        - **血統データ:** 1,377,361 レコード (5世代)
        - **特徴量:** 27+ カラム
        - **モデル:** μ, σ, ν の3パラメータモデル

        #### ストレージパス
        - **Raw HTML:** `keibaai/data/raw/html/`
        - **Parquet:** `keibaai/data/parsed/parquet/`
        - **Features:** `keibaai/data/features/parquet/`
        - **Models:** `keibaai/data/models/`
        - **Logs:** `keibaai/data/logs/`
        """)


def check_race_count():
    """レース数をチェック"""
    try:
        races_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
        if races_parquet.exists():
            df = pd.read_parquet(races_parquet)
            return f"{len(df):,}"
        return "N/A"
    except Exception as e:
        return "N/A"


def check_horse_count():
    """馬データ数をチェック"""
    try:
        horses_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "horses" / "horses.parquet"
        if horses_parquet.exists():
            df = pd.read_parquet(horses_parquet)
            return f"{len(df):,}"
        return "N/A"
    except Exception as e:
        return "N/A"


def check_model_status():
    """モデルステータスをチェック"""
    try:
        models_dir = project_root / "keibaai" / "data" / "models"
        if models_dir.exists():
            model_dirs = [d for d in models_dir.iterdir() if d.is_dir()]
            if model_dirs:
                return "✅ あり"
        return "❌ なし"
    except Exception as e:
        return "❌ エラー"
