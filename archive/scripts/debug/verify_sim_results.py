import json
import pandas as pd
import numpy as np
from pathlib import Path
import sys

def verify_simulation_results(sim_dir):
    sim_dir = Path(sim_dir)
    json_files = list(sim_dir.glob('*.json'))
    
    if not json_files:
        print(f"エラー: JSONファイルが見つかりません: {sim_dir}")
        return
    
    print(f"検証対象ファイル数: {len(json_files)}")
    
    results = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            race_id = data['race_id']
            K = data['K']
            win_probs = data['win_probs']
            place_probs = data['place_probs']
            exacta_probs = data['exacta_probs']
            trifecta_probs = data['trifecta_probs']
            
            # 検証1: 勝率の合計がほぼ1.0になるか
            total_win_prob = sum(win_probs.values())
            
            # 検証2: 複勝率の合計がほぼ3.0になるか（3着払いの場合）
            # 注: 出走頭数が少ない場合は3未満になることもある
            total_place_prob = sum(place_probs.values())
            
            # 検証3: 馬連の合計がほぼ1.0になるか
            total_exacta_prob = sum(exacta_probs.values())
            
            results.append({
                'race_id': race_id,
                'K': K,
                'total_win_prob': total_win_prob,
                'total_place_prob': total_place_prob,
                'total_exacta_prob': total_exacta_prob,
                'n_horses': len(win_probs)
            })
            
        except Exception as e:
            print(f"ファイル読み込みエラー {json_file}: {e}")

    df = pd.DataFrame(results)
    
    print("\n--- 検証結果サマリー ---")
    print(df.describe())
    
    # 異常値のチェック
    invalid_win = df[~np.isclose(df['total_win_prob'], 1.0, atol=0.01)]
    if not invalid_win.empty:
        print(f"\n⚠️ 勝率の合計が1.0でないレースがあります: {len(invalid_win)}件")
        print(invalid_win[['race_id', 'total_win_prob']])
    else:
        print("\n✅ 全レースで勝率の合計が約1.0です。")

    invalid_exacta = df[~np.isclose(df['total_exacta_prob'], 1.0, atol=0.01)]
    if not invalid_exacta.empty:
        print(f"\n⚠️ 馬連の合計が1.0でないレースがあります: {len(invalid_exacta)}件")
        # 馬連は組み合わせ数が多いので、Kが小さいと全組み合わせが出ない可能性があるが、
        # simulator.pyの実装では全シミュレーション結果から集計しているので1.0になるはず
        print(invalid_exacta[['race_id', 'total_exacta_prob']])
    else:
        print("\n✅ 全レースで馬連の合計が約1.0です。")
        
    print("\n検証完了")

if __name__ == '__main__':
    verify_simulation_results('c:/Users/zk-ht/Keiba/Keiba_AI_v2/keibaai/data/simulations')
