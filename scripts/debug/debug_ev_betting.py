
import json
import pandas as pd
from pathlib import Path
import sys
import io

def debug_ev_betting():
    """
    特定の1レースを対象に、期待値ベースの投資シミュレーションを行い、結果をファイルに出力するデバッグ用スクリプト。
    """
    output_filename = "debug_ev_betting_output.txt"
    with open(output_filename, 'w', encoding='utf-8') as f:
        # --- 設定項目 ---
        # 1. 分析対象のシミュレーションファイル
        sim_file_path = Path("data/simulations/20251122_141959_v2_backtest_202408070912.json")

        # 2. レース結果とオッズが含まれるParquetファイルのパス
        races_parquet_path = Path("keibaai/data/parsed/parquet/races/races.parquet")

        # 3. 投資判断の閾値 (期待値がこの値より大きい場合に賭ける)
        EV_THRESHOLD = 1.1 
        BET_AMOUNT = 100  # 1回の賭け金（円）

        # --- 処理開始 ---
        print("--- 期待値ベース投資 デバッグシミュレーション ---", file=f)

        if not sim_file_path.exists():
            print(f"エラー: シミュレーションファイルが見つかりません: {sim_file_path}", file=f)
            return

        if not races_parquet_path.exists():
            print(f"エラー: レース結果ファイルが見つかりません: {races_parquet_path}", file=f)
            return

        # 1. シミュレーション結果（勝率）の読み込み
        with open(sim_file_path, 'r', encoding='utf-8') as sim_f:
            sim_data = json.load(sim_f)
        
        race_id = sim_data['race_id']
        win_probs = sim_data['win_probs'] # {馬番: 勝率} の辞書
        
        print(f"\n分析対象レースID: {race_id}", file=f)
        print(f"モデルが算出した勝率: {win_probs}", file=f)

        # 2. レース結果（オッズ・着順）の読み込み
        races_df = pd.read_parquet(races_parquet_path)
        race_results_df = races_df[races_df['race_id'] == race_id].copy()

        if race_results_df.empty:
            print(f"エラー: レースID {race_id} の結果がraces.parquet内に見つかりません。", file=f)
            return
            
        # 3. 投資シミュレーション
        total_investment = 0
        total_return = 0

        print("\n--- 各馬の期待値計算と投資判断 ---", file=f)
        
        # "win_odds" が object 型の場合があるため、数値に変換
        race_results_df['win_odds'] = pd.to_numeric(race_results_df['win_odds'], errors='coerce')

        for index, horse in race_results_df.iterrows():
            horse_number = str(horse['horse_number'])
            horse_name = horse['horse_name']
            win_odds = horse['win_odds']
            finish_position = horse['finish_position']

            # 勝率を取得
            win_prob = win_probs.get(horse_number)

            if win_prob is None:
                print(f"  - 馬番 {horse_number} ({horse_name}): 勝率データなし、スキップ", file=f)
                continue

            # 期待値(EV)を計算
            expected_value = win_prob * win_odds

            print(f"  - 馬番 {horse_number} ({horse_name}):", file=f)
            print(f"    - 勝率: {win_prob:.4f} ({win_prob*100:.2f}%)", file=f)
            print(f"    - 単勝オッズ: {win_odds:.1f}倍", file=f)
            print(f"    - 期待値: {expected_value:.2f}", file=f)

            # 投資判断
            if expected_value > EV_THRESHOLD:
                print(f"    >> 投資実行 (期待値 {expected_value:.2f} > {EV_THRESHOLD})", file=f)
                total_investment += BET_AMOUNT
                
                # 結果判定
                if finish_position == 1:
                    payout = BET_AMOUNT * win_odds
                    total_return += payout
                    print(f"    >> 的中！ 払戻: {payout:.0f}円", file=f)
                else:
                    print(f"    >> 不的中... (着順: {finish_position}着)", file=f)

        # 4. 結果表示
        print("\n--- シミュレーション結果 ---", file=f)
        print(f"総投資額: {total_investment}円", file=f)
        print(f"総払戻額: {total_return:.0f}円", file=f)
        
        if total_investment > 0:
            roi = (total_return / total_investment) * 100
            print(f"回収率 (ROI): {roi:.1f}%", file=f)
        else:
            print("投資対象となる馬がいませんでした。", file=f)

if __name__ == '__main__':
    debug_ev_betting()
