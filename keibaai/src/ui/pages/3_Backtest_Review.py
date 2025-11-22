import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path
import json

# プロジェクトルートをパスに追加
# 現在のファイル: keibaai/src/ui/pages/X_Page.py
# 4つ上がプロジェクトルート
project_root = Path(__file__).resolve().parents[4]
sys.path.append(str(project_root))

def main():
    st.title("📈 過去検証 (Backtest Review)")
    
    if 'loader' not in st.session_state:
        st.warning("Homeページからモデルを選択してください。")
        return

    loader = st.session_state['loader']
    
    # バックテスト結果のロード
    sim_dir = Path('data/simulations')
    sim_files = sorted(sim_dir.glob('*v2_backtest*.json'))
    
    if not sim_files:
        st.error("バックテスト結果（JSON）が見つかりません。")
        return
        
    st.write(f"シミュレーション結果ファイル数: {len(sim_files)}")
    
    if st.button("バックテスト結果を読み込んで分析する"):
        with st.spinner("読み込みと分析を実行中..."):
            all_results = []
            for file in sim_files[:500]: # パフォーマンスのため制限
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        race_id = data['race_id']
                        win_probs = data.get('win_probs', {})
                        
                        for horse_num, prob in win_probs.items():
                            all_results.append({
                                'race_id': race_id,
                                'horse_number': int(horse_num),
                                'win_prob': prob
                            })
                except:
                    continue
            
            df_sim = pd.DataFrame(all_results)
            
            # 今回はデモとして、シミュレーションの確率分布のみ表示
            st.subheader("予測勝率の分布 (Predicted Win Probabilities)")
            st.caption("AIが各馬に対して予測した勝率の分布です。")
            fig = px.histogram(df_sim, x='win_prob', nbins=50, title="勝率分布", labels={'win_prob': '予測勝率'})
            st.plotly_chart(fig)
            
            st.subheader("自信のある予測 (High Confidence Predictions)")
            st.caption("AIが「勝率20%以上」と予測した馬の一覧です。")
            high_conf = df_sim[df_sim['win_prob'] > 0.2].sort_values('win_prob', ascending=False)
            
            # カラム名変更
            high_conf = high_conf.rename(columns={
                'race_id': 'レースID',
                'horse_number': '馬番',
                'win_prob': '予測勝率'
            })
            
            st.dataframe(high_conf)

if __name__ == "__main__":
    main()
