import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
# 現在のファイル: keibaai/src/ui/pages/X_Page.py
# 4つ上がプロジェクトルート
project_root = Path(__file__).resolve().parents[4]
sys.path.append(str(project_root))

def main():
    st.title("🐎 レース詳細 (Race Viewer)")
    
    if 'loader' not in st.session_state or 'selected_model' not in st.session_state:
        st.warning("Homeページからモデルを選択してください。")
        return

    loader = st.session_state['loader']
    selected_model = st.session_state['selected_model']
    
    # レース情報のロード（まずは2024年をデフォルトに）
    year = st.sidebar.selectbox("年", [2024, 2023, 2022, 2021, 2020], index=0)
    
    with st.spinner(f"{year}年のレース情報を読み込んでいます..."):
        df_races = loader.load_races(year=year)
        
    if df_races.empty:
        st.error("レース情報の読み込みに失敗しました。")
        return
        
    # 日付選択
    dates = sorted(df_races['race_date'].dt.strftime('%Y-%m-%d').unique(), reverse=True)
    selected_date = st.sidebar.selectbox("日付", dates)
    
    # 予測データのロード
    with st.spinner(f"{selected_date} の予測データを読み込んでいます..."):
        df_pred = loader.load_predictions(selected_date, selected_model)
        
    if df_pred.empty:
        st.warning(f"予測データが見つかりません: {selected_date}")
        return
        
    # その日のレース一覧
    daily_races = df_races[df_races['race_date'].dt.strftime('%Y-%m-%d') == selected_date]
    race_ids = sorted(daily_races['race_id'].unique())
    
    # レース選択
    selected_race_id = st.sidebar.selectbox(
        "レース選択",
        race_ids,
        format_func=lambda x: f"{x} - {daily_races[daily_races['race_id']==x]['race_name'].iloc[0]} ({daily_races[daily_races['race_id']==x]['venue'].iloc[0]})"
    )
    
    # レース詳細表示
    race_info = daily_races[daily_races['race_id'] == selected_race_id].iloc[0]
    
    # race_idからレース番号を抽出 (末尾2桁と仮定)
    try:
        race_num = int(str(selected_race_id)[-2:])
    except:
        race_num = "?"
        
    st.subheader(f"{race_info['race_name']} (R{race_num})")
    st.caption(f"{race_info['venue']} / {race_info['distance_m']}m / {race_info['track_surface']}")
    
    # 出走馬と予測のマージ
    race_pred = df_pred[df_pred['race_id'] == str(selected_race_id)].copy()
    
    if race_pred.empty:
        st.warning("このレースの予測データがありません。")
        return
        
    # 表示用データ作成
    # 期待値計算 (オッズがあれば)
    if 'win_odds' in race_pred.columns:
        race_pred['EV'] = race_pred['mu'] * race_pred['win_odds']
    else:
        race_pred['EV'] = 0.0
        
    # 表示カラム選択
    cols = ['horse_number', 'mu']
    if 'sigma' in race_pred.columns: cols.append('sigma')
    if 'nu' in race_pred.columns: cols.append('nu')
    if 'win_odds' in race_pred.columns: cols.append('win_odds')
    cols.append('EV')
    
    # カラム名変更（日本語化）
    rename_map = {
        'horse_number': '馬番',
        'mu': 'AI勝率スコア',
        'sigma': '不確実性(σ)',
        'nu': '荒れ度(ν)',
        'win_odds': '単勝オッズ',
        'EV': '期待値'
    }
    
    # ソート
    race_pred = race_pred.sort_values('mu', ascending=False)
    
    # データフレーム表示（ハイライト付き）
    st.write("予測一覧（AI勝率スコア順）")
    st.dataframe(
        race_pred[cols].rename(columns=rename_map).style.background_gradient(subset=['AI勝率スコア'], cmap='Blues')
                             .background_gradient(subset=['期待値'], cmap='Reds', vmin=0.8, vmax=1.5)
    )
    
    # シミュレーション結果があれば表示
    sim_result = loader.load_simulation_results(selected_race_id)
    if sim_result:
        st.subheader("シミュレーション結果 (Simulation Result)")
        st.write("100回レースを行った場合の勝率分布")
        win_probs = sim_result.get('win_probs', {})
        
        # グラフ用データ
        sim_df = pd.DataFrame({
            '馬番': list(win_probs.keys()),
            '勝率': list(win_probs.values())
        })
        sim_df['馬番'] = sim_df['馬番'].astype(int)
        sim_df = sim_df.sort_values('馬番')
        
        st.bar_chart(sim_df.set_index('馬番'))

if __name__ == "__main__":
    main()
