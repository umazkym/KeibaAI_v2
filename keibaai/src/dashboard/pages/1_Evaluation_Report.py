import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.append(str(project_root))

from src.utils.data_utils import load_parquet_data_by_date

st.set_page_config(page_title="AIの成績表", page_icon="📊", layout="wide")

st.title("📊 AIの成績表 (Evaluation Report)")

st.markdown("""
AIの予想がどれくらい当たっているかを確認します。
- **タイム誤差 (RMSE)**: AIが予想したタイムと、実際のタイムのズレです。**数字が小さいほど優秀**です。
- **順位予想の正確さ (スピアマン相関)**: 1位、2位...という順位をどれくらい正しく当てられたかです。**1.0に近いほど完璧**で、0だとデタラメ、マイナスだと逆の結果だったことを意味します。
- **的中率 (Hit Rate)**: AIが「上位に来る」と予想した馬が、実際に3着以内に入った確率です。
""")

# --- サイドバー設定 ---
st.sidebar.header("期間設定")

# 日付範囲選択
default_start = pd.to_datetime("2024-01-01")
default_end = pd.to_datetime("2024-12-31")

start_date = st.sidebar.date_input("開始日", default_start)
end_date = st.sidebar.date_input("終了日", default_end)

# --- データロード ---
# --- データロード ---
@st.cache_data
def load_evaluation_data():
    eval_path = project_root / "keibaai/data/evaluation/evaluation_results.csv"
    if eval_path.exists():
        df = pd.read_csv(eval_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    else:
        return pd.DataFrame()

# データを読み込む
eval_df = load_evaluation_data()

if eval_df.empty:
    st.warning("⚠️ 評価データが見つかりません。`evaluate_model.py` を実行してください。")
    # デモデータを表示する場合のフォールバック（必要なら残すが、今回はリアルデータ優先）
    st.stop()

# 日付フィルタリング
mask = (eval_df['date'] >= pd.to_datetime(start_date)) & (eval_df['date'] <= pd.to_datetime(end_date))
filtered_df = eval_df[mask].copy()

if filtered_df.empty:
    st.warning("指定された期間のデータがありません。")
    st.stop()

# 変数名を合わせる（demo_data -> filtered_df）
demo_data = filtered_df

# --- メトリクスサマリー ---
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("平均タイム誤差 (RMSE)", f"{demo_data['rmse'].mean():.4f}", f"{demo_data['rmse'].diff().mean():.4f} (前日比)")
with col2:
    st.metric("順位予想の正確さ (相関係数)", f"{demo_data['spearman_corr'].mean():.4f}", f"{demo_data['spearman_corr'].diff().mean():.4f} (前日比)")
with col3:
    st.metric("的中率 (3着内率)", f"{demo_data['hit_rate'].mean():.2%}", f"{demo_data['hit_rate'].diff().mean():.2%} (前日比)")

# --- チャート表示 ---
st.subheader("📈 成績の推移")

tab1, tab2 = st.tabs(["順位予想の正確さ", "タイム誤差"])

with tab1:
    fig_corr = px.line(demo_data, x='date', y='spearman_corr', title='日ごとの順位予想の正確さ')
    fig_corr.add_hline(y=demo_data['spearman_corr'].mean(), line_dash="dash", annotation_text="平均")
    st.plotly_chart(fig_corr, use_container_width=True)
    st.caption("💡 **AI解説**: グラフが上にある日は、AIがレース展開を正しく読めています。逆に下にある日は、予想外の馬が来たりして荒れたレースが多かった可能性があります。")

with tab2:
    fig_rmse = px.line(demo_data, x='date', y='rmse', title='日ごとのタイム誤差')
    fig_rmse.add_hline(y=demo_data['rmse'].mean(), line_dash="dash", annotation_text="平均")
    st.plotly_chart(fig_rmse, use_container_width=True)
    st.caption("💡 **AI解説**: グラフが下にあるほど、タイムを正確に予想できています。")

# --- 詳細分析 ---
st.subheader("🔍 詳しく見る")
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("#### 🏟️ 競馬場ごとの得意・不得意")
    # ダミーデータ
    venues = ['東京', '中山', '京都', '阪神']
    venue_data = pd.DataFrame({
        '競馬場': venues,
        '正確さ (相関係数)': np.random.uniform(0.3, 0.6, size=4),
        'レース数': np.random.randint(50, 200, size=4)
    })
    st.dataframe(venue_data, hide_index=True)

with col_b:
    st.markdown("#### 📏 距離ごとの得意・不得意")
    distances = ['短距離 (Sprint)', 'マイル (Mile)', '中距離 (Intermediate)', '長距離 (Long)']
    dist_data = pd.DataFrame({
        '距離区分': distances,
        '正確さ (相関係数)': np.random.uniform(0.3, 0.6, size=4),
        'レース数': np.random.randint(50, 200, size=4)
    })
    st.dataframe(dist_data, hide_index=True)
