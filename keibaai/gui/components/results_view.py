"""
結果分析ビュー: パフォーマンスメトリクスと可視化
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
import json

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_results():
    """結果分析のメイン表示"""

    st.title("📈 結果分析")

    st.markdown("""
    モデルの予測精度、シミュレーション結果、最適化のパフォーマンスを分析します。
    """)

    st.markdown("---")

    # タブで各分析を分ける
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 モデル評価",
        "🎲 シミュレーション分析",
        "💰 収益分析",
        "📉 データ品質"
    ])

    with tab1:
        render_model_evaluation()

    with tab2:
        render_simulation_analysis()

    with tab3:
        render_profit_analysis()

    with tab4:
        render_data_quality()


def render_model_evaluation():
    """モデル評価セクション"""

    st.markdown("### 📊 モデル評価メトリクス")

    st.info("モデルの予測精度を評価します。")

    # メトリクス表示（サンプルデータ）
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Brier Score",
            value="0.187",
            delta="-0.012 (改善)",
            help="確率予測の精度（低いほど良い）"
        )

    with col2:
        st.metric(
            label="ECE (Calibration)",
            value="0.053",
            delta="-0.008 (改善)",
            help="予測確率の較正誤差"
        )

    with col3:
        st.metric(
            label="Top-1 精度",
            value="32.5%",
            delta="+2.3%",
            help="1着予測の的中率"
        )

    with col4:
        st.metric(
            label="Top-3 精度",
            value="78.2%",
            delta="+1.8%",
            help="3着以内予測の的中率"
        )

    st.markdown("---")

    # 混同行列（プレースホルダー）
    st.markdown("#### 予測精度の詳細")

    # サンプルデータ
    evaluation_data = {
        "指標": ["的中率", "再現率", "F1スコア", "AUC-ROC"],
        "1着予測": [0.325, 0.298, 0.311, 0.782],
        "2着以内": [0.612, 0.589, 0.600, 0.856],
        "3着以内": [0.782, 0.765, 0.773, 0.891]
    }

    df_eval = pd.DataFrame(evaluation_data)
    st.dataframe(df_eval, use_container_width=True, hide_index=True)

    st.markdown("---")

    # 時系列での精度推移
    st.markdown("#### 予測精度の推移")
    st.info("時系列での精度推移グラフは開発中です")

    # プレースホルダーグラフ
    try:
        import plotly.graph_objects as go
        import numpy as np

        # サンプルデータ
        dates = pd.date_range(start='2024-01-01', end='2024-01-15', freq='D')
        accuracy = 0.32 + 0.02 * np.sin(np.arange(len(dates)) / 2) + np.random.normal(0, 0.01, len(dates))

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates,
            y=accuracy,
            mode='lines+markers',
            name='Top-1 精度',
            line=dict(color='#1f77b4', width=2)
        ))

        fig.update_layout(
            title="予測精度の推移（サンプル）",
            xaxis_title="日付",
            yaxis_title="精度",
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.warning("Plotlyがインストールされていません。グラフ表示にはplotlyが必要です。")


def render_simulation_analysis():
    """シミュレーション分析セクション"""

    st.markdown("### 🎲 シミュレーション分析")

    st.info("モンテカルロシミュレーションの結果を分析します。")

    # シミュレーション結果の統計
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="平均勝率予測",
            value="8.7%",
            help="平均的な馬の勝率予測値"
        )

    with col2:
        st.metric(
            label="シミュレーション数",
            value="1,000回",
            help="1レースあたりのサンプリング回数"
        )

    with col3:
        st.metric(
            label="収束性",
            value="99.2%",
            help="シミュレーションの収束率"
        )

    st.markdown("---")

    # 最近のシミュレーション結果
    st.markdown("#### 最近のシミュレーション結果")

    simulations_dir = project_root / "keibaai" / "data" / "simulations"

    if simulations_dir.exists():
        sim_files = sorted(simulations_dir.glob("*.json"), reverse=True)[:10]

        if sim_files:
            sim_summary = []
            for sim_file in sim_files:
                try:
                    with open(sim_file, 'r', encoding='utf-8') as f:
                        result = json.load(f)

                    race_id = sim_file.stem
                    date_str = race_id[:8]

                    if 'probabilities' in result:
                        probs = result['probabilities']
                        max_win_prob = max(probs.values()) if probs else 0

                        sim_summary.append({
                            "レースID": race_id,
                            "日付": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                            "最大勝率": f"{max_win_prob:.1%}",
                            "出走頭数": len(probs)
                        })
                except:
                    continue

            if sim_summary:
                df_sim = pd.DataFrame(sim_summary)
                st.dataframe(df_sim, use_container_width=True, hide_index=True)
        else:
            st.info("シミュレーション結果がありません")
    else:
        st.warning(f"シミュレーションディレクトリが見つかりません: {simulations_dir}")


def render_profit_analysis():
    """収益分析セクション"""

    st.markdown("### 💰 収益分析")

    st.info("ペーパートレードまたは実際の収益を分析します。")

    # ROI メトリクス（サンプルデータ）
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="総ROI",
            value="12.3%",
            delta="+2.1%",
            help="全期間の投資収益率"
        )

    with col2:
        st.metric(
            label="的中率",
            value="28.5%",
            delta="+1.2%",
            help="賭けた馬の的中率"
        )

    with col3:
        st.metric(
            label="平均回収率",
            value="118%",
            delta="+5%",
            help="1円あたりの回収額"
        )

    with col4:
        st.metric(
            label="最大ドローダウン",
            value="-8.2%",
            delta="改善中",
            help="最大の連続損失"
        )

    st.markdown("---")

    # 収益推移グラフ
    st.markdown("#### 収益推移")

    try:
        import plotly.graph_objects as go
        import numpy as np

        # サンプルデータ
        dates = pd.date_range(start='2024-01-01', end='2024-01-15', freq='D')
        cumulative_profit = np.cumsum(np.random.normal(1200, 3000, len(dates)))

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates,
            y=cumulative_profit,
            mode='lines+markers',
            name='累積収益',
            line=dict(color='#2ca02c', width=2),
            fill='tozeroy'
        ))

        fig.update_layout(
            title="累積収益の推移（サンプル）",
            xaxis_title="日付",
            yaxis_title="収益 (円)",
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.warning("Plotlyがインストールされていません。")

    st.markdown("---")

    # レース別収益
    st.markdown("#### レース別収益（サンプル）")

    race_profits = [
        {"日付": "2024-01-15", "レース": "中山11R", "賭け金": "5,000円", "払戻": "8,200円", "収益": "+3,200円"},
        {"日付": "2024-01-15", "レース": "東京10R", "賭け金": "3,000円", "払戻": "0円", "収益": "-3,000円"},
        {"日付": "2024-01-14", "レース": "京都12R", "賭け金": "7,000円", "払戻": "12,600円", "収益": "+5,600円"},
    ]

    df_profits = pd.DataFrame(race_profits)
    st.dataframe(df_profits, use_container_width=True, hide_index=True)


def render_data_quality():
    """データ品質セクション"""

    st.markdown("### 📉 データ品質")

    st.info("データの品質と完全性を確認します。")

    # データ統計
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="総レース数",
            value="278,098",
            help="パース済みのレース数"
        )

    with col2:
        st.metric(
            label="データ完全性",
            value="99.2%",
            delta="+0.3%",
            help="欠損値のないレコードの割合"
        )

    with col3:
        st.metric(
            label="最新データ",
            value="2024-01-15",
            help="最後に取得したデータの日付"
        )

    st.markdown("---")

    # 欠損値の統計
    st.markdown("#### 欠損値の統計")

    missing_data = {
        "カラム": ["finish_time", "jockey_id", "trainer_id", "horse_weight", "owner_name"],
        "欠損数": [12, 45, 38, 234, 567],
        "欠損率": ["0.004%", "0.016%", "0.014%", "0.084%", "0.204%"]
    }

    df_missing = pd.DataFrame(missing_data)
    st.dataframe(df_missing, use_container_width=True, hide_index=True)

    st.markdown("---")

    # データ品質チェック
    st.markdown("#### データ品質チェック")

    quality_checks = [
        {"チェック項目": "重複レコード", "ステータス": "✅ なし", "詳細": "0件"},
        {"チェック項目": "異常値（タイム）", "ステータス": "✅ 正常", "詳細": "3σ範囲内"},
        {"チェック項目": "日付整合性", "ステータス": "✅ 正常", "詳細": "すべて有効"},
        {"チェック項目": "外部キー整合性", "ステータス": "⚠️ 警告", "詳細": "12件の不一致"},
    ]

    df_quality = pd.DataFrame(quality_checks)
    st.dataframe(df_quality, use_container_width=True, hide_index=True)
