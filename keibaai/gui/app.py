#!/usr/bin/env python3
"""
KeibaAI_v2 統合GUIダッシュボード
Streamlitベースのインターフェース

実行方法:
    streamlit run keibaai/gui/app.py

機能:
    - データパイプライン実行（スクレイピング、パース、特徴量生成）
    - モデル学習（μ, σ, ν）
    - シミュレーション実行
    - 最適化実行
    - 結果可視化
    - 設定管理
    - ログモニタリング
"""

import streamlit as st
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# ページ設定
st.set_page_config(
    page_title="KeibaAI_v2 Dashboard",
    page_icon="🐴",
    layout="wide",
    initial_sidebar_state="expanded"
)

# カスタムCSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        border-bottom: 3px solid #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .success-box {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border-left: 4px solid #17a2b8;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """メインアプリケーション"""

    # セッションステートの初期化
    if "navigation" not in st.session_state:
        st.session_state.navigation = "🏠 ダッシュボード"

    # サイドバー
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 1rem 0;'>
            <h1 style='font-size: 2rem; margin: 0;'>🐴 KeibaAI_v2</h1>
            <p style='font-size: 0.9rem; color: #666;'>競馬AI予測システム</p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        # ページ選択
        page = st.radio(
            "📋 ナビゲーション",
            [
                "🏠 ダッシュボード",
                "🚀 クイックスタート",
                "📊 データパイプライン",
                "🔍 データエクスプローラー",
                "🤖 モデル学習",
                "🎲 シミュレーション",
                "💰 ポートフォリオ最適化",
                "📈 結果分析",
                "⚙️ 設定管理",
                "📝 ログビューア"
            ],
            index=[
                "🏠 ダッシュボード",
                "🚀 クイックスタート",
                "📊 データパイプライン",
                "🔍 データエクスプローラー",
                "🤖 モデル学習",
                "🎲 シミュレーション",
                "💰 ポートフォリオ最適化",
                "📈 結果分析",
                "⚙️ 設定管理",
                "📝 ログビューア"
            ].index(st.session_state.navigation) if st.session_state.navigation in [
                "🏠 ダッシュボード",
                "🚀 クイックスタート",
                "📊 データパイプライン",
                "🔍 データエクスプローラー",
                "🤖 モデル学習",
                "🎲 シミュレーション",
                "💰 ポートフォリオ最適化",
                "📈 結果分析",
                "⚙️ 設定管理",
                "📝 ログビューア"
            ] else 0,
            key="navigation_radio"
        )

        # ページ状態を更新
        st.session_state.navigation = page

        st.markdown("---")
        st.markdown("### 💡 ヒント")
        st.info("""
        **初めての方は**
        🚀 クイックスタート
        から始めましょう！
        """)

        st.markdown("---")
        st.markdown("### システム情報")
        st.success("**Version:** 2.0.0\n**Status:** ✅ Running")

    # ページルーティング
    if page == "🏠 ダッシュボード":
        show_dashboard()
    elif page == "🚀 クイックスタート":
        show_quick_start()
    elif page == "📊 データパイプライン":
        show_data_pipeline()
    elif page == "🔍 データエクスプローラー":
        show_data_explorer()
    elif page == "🤖 モデル学習":
        show_model_training()
    elif page == "🎲 シミュレーション":
        show_simulation()
    elif page == "💰 ポートフォリオ最適化":
        show_optimization()
    elif page == "📈 結果分析":
        show_results_analysis()
    elif page == "⚙️ 設定管理":
        show_settings()
    elif page == "📝 ログビューア":
        show_logs()


def show_dashboard():
    """ダッシュボード: システム概要とステータス"""
    from components.dashboard_view import render_dashboard
    render_dashboard()


def show_quick_start():
    """クイックスタート: 初心者向けガイド"""
    from components.quick_start_view import render_quick_start
    render_quick_start()


def show_data_pipeline():
    """データパイプライン: スクレイピング・パース・特徴量生成"""
    from components.data_pipeline_view import render_data_pipeline
    render_data_pipeline()


def show_data_explorer():
    """データエクスプローラー: データの詳細表示"""
    from components.data_explorer_view import render_data_explorer
    render_data_explorer()


def show_model_training():
    """モデル学習: μ, σ, ν モデルの学習"""
    from components.model_training_view import render_model_training
    render_model_training()


def show_simulation():
    """シミュレーション: モンテカルロシミュレーション"""
    from components.simulation_view import render_simulation
    render_simulation()


def show_optimization():
    """ポートフォリオ最適化: ケリー基準による最適化"""
    from components.optimization_view import render_optimization
    render_optimization()


def show_results_analysis():
    """結果分析: パフォーマンスメトリクスと可視化"""
    from components.results_view import render_results
    render_results()


def show_settings():
    """設定管理: YAMLファイルのGUI編集"""
    from components.settings_view import render_settings
    render_settings()


def show_logs():
    """ログビューア: リアルタイムログ表示"""
    from components.logs_view import render_logs
    render_logs()


if __name__ == "__main__":
    main()
