import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import numpy as np

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

st.set_page_config(page_title="予想の根拠", page_icon="🧬", layout="wide")

st.title("🧬 予想の根拠 (Feature Analysis)")

st.markdown("""
AIが「何を重視して予想したか」を解明します。
- **重要度ランキング**: どのデータが予想に一番影響したかランキング形式で見れます。
- **影響の分析**: そのデータがどうなると、AIの評価が上がるのか（例：体重は重い方がいい？軽い方がいい？）を確認できます。
""")

# --- サイドバー ---
st.sidebar.header("モデル選択")
model_version = st.sidebar.selectbox("モデルバージョン", ["latest", "v2.0", "v1.5"])

# --- データロード (ダミー) ---
@st.cache_data
def load_feature_importance():
    # TODO: LightGBMモデルからfeature_importanceを取得する
    features = [
        "avg_finish_last5 (近5走平均着順)", "win_odds (単勝オッズ)", "jockey_win_rate (騎手勝率)", "speed_index (スピード指数)", 
        "sire_avg_finish (種牡馬平均着順)", "distance_m (距離)", "horse_weight (馬体重)", "weight_trend (体重増減傾向)",
        "nige_ratio (逃げ馬比率)", "nicks_avg_finish (ニックス平均着順)", "course_bias_score (コースバイアス)"
    ]
    importance = np.random.exponential(scale=10, size=len(features))
    # 正規化
    importance = importance / importance.sum() * 100
    
    df = pd.DataFrame({
        '特徴量': features,
        '重要度 (%)': importance
    }).sort_values('重要度 (%)', ascending=True)
    return df

df_imp = load_feature_importance()

# --- 特徴量重要度 (Bar Chart) ---
st.subheader("🏆 どのデータが重要？ (重要度ランキング)")

fig_imp = px.bar(
    df_imp, x='重要度 (%)', y='特徴量', orientation='h',
    title="AIが重視しているデータ TOP10",
    color='重要度 (%)', color_continuous_scale='Blues'
)
st.plotly_chart(fig_imp, use_container_width=True)

# --- 特徴量詳細分析 ---
st.subheader("🔬 詳しく分析する")

selected_feature = st.selectbox("分析するデータを選択", df_imp['特徴量'].sort_values())

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"#### 📊 データの分布 ({selected_feature})")
    # ダミー分布データ
    dist_data = np.random.normal(loc=50, scale=15, size=1000)
    fig_dist = px.histogram(dist_data, nbins=30, title=f"データのばらつき")
    st.plotly_chart(fig_dist, use_container_width=True)

with col2:
    st.markdown(f"#### 📈 AIの評価はどう変わる？")
    # ダミーPDPデータ
    x_range = np.linspace(dist_data.min(), dist_data.max(), 50)
    y_effect = np.sin(x_range / 10) + (x_range / 50) # 適当な非線形関係
    
    fig_pdp = px.line(x=x_range, y=y_effect, title=f"値の変化とAI評価の関係")
    fig_pdp.update_layout(xaxis_title=selected_feature, yaxis_title="AIの評価スコア")
    st.plotly_chart(fig_pdp, use_container_width=True)
    
    st.caption("💡 **AI解説**: グラフが右上がりなら「値が大きいほど高評価」、右下がりなら「値が小さいほど高評価」です。")

# --- 改善提案 ---
st.divider()
st.subheader("💡 AIからの改善提案")

st.info("分析結果に基づき、AIの精度をさらに上げるためのヒントです：")

suggestions = [
    "**組み合わせの検討**: 「単勝オッズ」と「騎手勝率」に関係がありそうです。「オッズが高いのに騎手が上手い」といったパターンを学習させると良いかもしれません。",
    "**区分の見直し**: 「馬体重」は単純に重い/軽いだけでなく、「軽量」「中量」「重量」のようにグループ分けすると、AIが理解しやすくなるかもしれません。",
    "**不要なデータの削除**: 「体重増減傾向」はあまり予想の役に立っていないようです。削除して計算を軽くすることを検討してください。",
    "**データの穴埋め**: 「ニックス平均着順」にデータが欠けている部分があります。平均値で埋めるのではなく、似た馬のデータを使うなど工夫が必要です。"
]

for s in suggestions:
    st.markdown(f"- {s}")
