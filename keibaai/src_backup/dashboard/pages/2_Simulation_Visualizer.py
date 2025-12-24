import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import json
import numpy as np

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

st.set_page_config(page_title="レース予想とシミュレーション", page_icon="🎲", layout="wide")

st.title("🎲 レース予想とシミュレーション (Simulation Visualizer)")

st.markdown("""
レースごとのAI予想を詳しく見ることができます。
- **勝率**: AIが計算した「その馬が勝つ確率」です。
- **期待値**: 「100円買ったら平均して何円戻ってくるか」の目安です。**1.0（100円）を超えていると、買う価値が高い**と判断できます。
- **もしも分析 (What-If)**: オッズが変わったときに、期待値がどう変わるかを試せます。
""")

# --- サイドバー: レース選択 ---
st.sidebar.header("レース選択")

# 日付選択
# TODO: 実際のデータがある日付をスキャンして選択肢にする
selected_date = st.sidebar.date_input("日付", pd.to_datetime("2024-01-01"))

# 競馬場選択
venues = ["東京", "中山", "京都", "阪神", "新潟", "福島", "中京", "小倉", "札幌", "函館"]
selected_venue = st.sidebar.selectbox("競馬場", venues)

# レース番号選択
selected_race_num = st.sidebar.number_input("レース番号", min_value=1, max_value=12, value=11)

race_id_display = f"{selected_date.strftime('%Y%m%d')}_{selected_venue}_{selected_race_num}R"
st.sidebar.info(f"選択中のレースID: {race_id_display}")

# --- データロード ---
@st.cache_data
def load_simulation_data(race_id):
    # TODO: 実際のシミュレーション結果JSONを読み込む
    # path = project_root / f"keibaai/data/simulations/{race_id}/simulation.json"
    return None

sim_data = load_simulation_data(race_id_display)

if sim_data is None:
    st.warning("⚠️ このレースのシミュレーションデータが見つかりません。デモデータを表示します。")
    
    # デモデータ生成
    n_horses = 16
    horse_numbers = list(range(1, n_horses + 1))
    win_probs = np.random.dirichlet(np.ones(n_horses), size=1)[0]
    odds = 1 / win_probs * np.random.uniform(0.8, 1.2, size=n_horses) # オッズは確率の逆数付近
    
    df = pd.DataFrame({
        '馬番': horse_numbers,
        '予測勝率': win_probs,
        '単勝オッズ': odds,
        '馬名': [f"デモホース{i}" for i in horse_numbers]
    })
    df['期待値'] = df['予測勝率'] * df['単勝オッズ']
else:
    df = pd.DataFrame(sim_data) # 仮

# --- 可視化 ---

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("🏆 AIの勝率予想")
    fig_prob = px.bar(
        df, x='馬番', y='予測勝率',
        hover_data=['馬名', '単勝オッズ'],
        color='予測勝率',
        color_continuous_scale='Viridis',
        labels={'予測勝率': '勝率 (%)'}
    )
    fig_prob.update_layout(xaxis_type='category')
    st.plotly_chart(fig_prob, use_container_width=True)

with col2:
    st.subheader("💰 お買い得な馬は？ (期待値分析)")
    
    # 期待値1.0以上の馬をハイライト
    df['推奨馬'] = df['期待値'] > 1.0
    
    fig_ev = px.scatter(
        df, x='単勝オッズ', y='予測勝率',
        size='期待値', color='推奨馬',
        hover_name='馬名',
        color_discrete_map={True: 'green', False: 'gray'},
        title="勝率 vs オッズ (大きい丸ほどお買い得)"
    )
    
    # 損益分岐ライン (y = 1/x)
    x_range = np.linspace(df['単勝オッズ'].min(), df['単勝オッズ'].max(), 100)
    y_range = 1 / x_range
    fig_ev.add_trace(go.Scatter(x=x_range, y=y_range, mode='lines', name='損益分岐点', line=dict(dash='dash', color='red')))
    
    st.plotly_chart(fig_ev, use_container_width=True)
    
    st.caption("💡 **AIアドバイス**: 緑色の丸は「勝つ確率の割にオッズが高い（おいしい）」馬です。赤の点線より上にあればプラス収支が期待できます。")

# --- 詳細テーブル ---
st.subheader("📋 詳細データ")
st.dataframe(
    df.style.format({
        '予測勝率': '{:.2%}',
        '単勝オッズ': '{:.1f}',
        '期待値': '{:.2f}'
    }).background_gradient(subset=['期待値'], cmap='RdYlGn', vmin=0.5, vmax=1.5),
    use_container_width=True
)

# --- What-If 分析 ---
st.divider()
st.subheader("🤔 もしも分析 (オッズが変わったら？)")
st.markdown("「もしオッズが下がったら、まだ買う価値はある？」を確認できます。")

target_horse = st.selectbox("分析対象の馬を選択", df['馬番'])
current_odds = df[df['馬番'] == target_horse]['単勝オッズ'].values[0]
new_odds = st.slider(f"馬番 {target_horse} のオッズを変更してみる", min_value=1.0, max_value=100.0, value=float(current_odds))

current_prob = df[df['馬番'] == target_horse]['予測勝率'].values[0]
new_ev = current_prob * new_odds

col_w1, col_w2 = st.columns(2)
with col_w1:
    st.metric("現在の期待値", f"{current_prob * current_odds:.2f}")
with col_w2:
    st.metric("変更後の期待値", f"{new_ev:.2f}", delta=f"{new_ev - (current_prob * current_odds):.2f}")

if new_ev > 1.0:
    st.success(f"馬番 {target_horse} はオッズ {new_odds} でも「買い」です！ (期待値 > 1.0)")
else:
    st.error(f"馬番 {target_horse} はオッズ {new_odds} だと旨味がありません。 (期待値 < 1.0)")
