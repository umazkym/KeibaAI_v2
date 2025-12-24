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

st.set_page_config(page_title="収支分析", page_icon="💰", layout="wide")

st.title("💰 収支分析 (ROI Analysis)")

st.markdown("""
AIの予想通りに馬券を買った場合の収益シミュレーションです。
- **総リターン**: 最終的に資金が何％増えたか（または減ったか）です。
- **シャープレシオ**: 「安定して稼げているか」の指標です。**1.0を超えると優秀**で、値が大きいほどリスクを抑えて利益を出せています。
- **最大ドローダウン**: 一時的に資金が最大でどれくらい減ったかです。この数字が大きいと、途中でハラハラする場面が多かったことを意味します。
""")

# --- サイドバー ---
st.sidebar.header("バックテスト設定")
strategy = st.sidebar.selectbox("買い方を選ぶ", ["ケリー基準 (自信度に応じて金額を変える)", "定額投資 (毎回同じ金額を買う)", "ボックス買い (手堅く買う)"])
initial_capital = st.sidebar.number_input("最初の資金 (円)", value=1000000)

# --- データロード (ダミー) ---
@st.cache_data
def load_backtest_results(strategy):
    # TODO: 実際のバックテスト結果を読み込む
    dates = pd.date_range(start="2024-01-01", end="2024-12-31")
    n_days = len(dates)
    
    # ランダムウォークで資産推移を生成
    daily_returns = np.random.normal(loc=0.001, scale=0.02, size=n_days)
    cumulative_returns = (1 + daily_returns).cumprod()
    capital = initial_capital * cumulative_returns
    
    df = pd.DataFrame({
        'Date': dates,
        'Capital': capital,
        'Daily Return': daily_returns,
        'Drawdown': (capital - np.maximum.accumulate(capital)) / np.maximum.accumulate(capital)
    })
    return df

df_res = load_backtest_results(strategy)

# --- KPI メトリクス ---
total_return = (df_res['Capital'].iloc[-1] / initial_capital) - 1
sharpe_ratio = df_res['Daily Return'].mean() / df_res['Daily Return'].std() * np.sqrt(252)
max_drawdown = df_res['Drawdown'].min()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("総リターン (増減率)", f"{total_return:.2%}", delta_color="normal")
with col2:
    st.metric("安定性 (シャープレシオ)", f"{sharpe_ratio:.2f}")
with col3:
    st.metric("最大損失率 (ドローダウン)", f"{max_drawdown:.2%}", delta_color="inverse")

# --- 資産推移チャート ---
st.subheader("📈 資金の増え方 (資産推移)")

fig_equity = px.line(df_res, x='Date', y='Capital', title=f"資金の推移 ({strategy})")
fig_equity.add_hline(y=initial_capital, line_dash="dash", line_color="gray", annotation_text="最初の資金")
st.plotly_chart(fig_equity, use_container_width=True)

# --- ドローダウンチャート ---
st.subheader("📉 資金の減り方 (ドローダウン)")
fig_dd = px.area(df_res, x='Date', y='Drawdown', title="ピークからの一時的な減少率", color_discrete_sequence=['red'])
st.plotly_chart(fig_dd, use_container_width=True)

# --- 月次リターン (ヒートマップ) ---
st.subheader("📅 月ごとの成績")

df_res['Year'] = df_res['Date'].dt.year
df_res['Month'] = df_res['Date'].dt.month

monthly_returns = df_res.groupby(['Year', 'Month'])['Daily Return'].sum().reset_index()
monthly_pivot = monthly_returns.pivot(index='Year', columns='Month', values='Daily Return')

fig_heat = px.imshow(
    monthly_pivot,
    labels=dict(x="月", y="年", color="リターン"),
    x=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'],
    color_continuous_scale='RdYlGn',
    text_auto='.1%'
)
st.plotly_chart(fig_heat, use_container_width=True)
