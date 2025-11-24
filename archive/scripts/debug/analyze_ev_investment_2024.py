
import json
import pandas as pd
from pathlib import Path
import sys
import io
from tqdm import tqdm

def analyze_all_races_ev_betting():
    """
    2024年の全レースを対象に、期待値ベースの投資シミュレーションを行い、
    詳細なログと最終サマリーを出力する。
    """
    # stdoutのエンコーディングをUTF-8に設定
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    # --- 設定項目 ---
    SIMULATIONS_DIR = Path("data/simulations")
    RACES_PARQUET_PATH = Path("keibaai/data/parsed/parquet/races/races.parquet")
    
    # 出力ファイル
    SUMMARY_OUTPUT_FILE = "ev_investment_summary_2024.txt"
    DETAILS_OUTPUT_FILE = "ev_investment_details_2024.csv"

    # 投資判断の閾値
    EV_THRESHOLD = 1.1 
    BET_AMOUNT = 100  # 1回の賭け金（円）

    # --- 処理開始 ---
    print("--- 2024年 期待値ベース投資 全レースシミュレーション ---")

    # 0. 必要なファイルの存在チェック
    if not SIMULATIONS_DIR.exists():
        print(f"エラー: シミュレーションディレクトリが見つかりません: {SIMULATIONS_DIR}")
        return
    if not RACES_PARQUET_PATH.exists():
        print(f"エラー: レース結果ファイルが見つかりません: {RACES_PARQUET_PATH}")
        return

    # 1. 2024年の全シミュレーションファイルを取得
    print("2024年のシミュレーションファイルを検索中...")
    # model_id 'v2_backtest' と '2024' を含むファイルを対象とする
    sim_files = list(SIMULATIONS_DIR.glob("*_v2_backtest_2024*.json"))
    if not sim_files:
        print("エラー: 2024年のシミュレーションファイルが見つかりません。")
        return
    print(f"{len(sim_files)} 件のレースシミュレーションデータが見つかりました。")

    # 2. 全レース結果データをロード
    print("レース結果データをロード中...")
    races_df = pd.read_parquet(RACES_PARQUET_PATH)
    # 'race_id' をインデックスにして高速化
    races_df.set_index('race_id', inplace=True)
    # オッズと着順を数値型に変換
    races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
    races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
    print("レース結果データのロード完了。")

    # 3. シミュレーション実行
    total_investment = 0
    total_return = 0
    total_bets = 0
    total_wins = 0
    
    # 詳細ログを保存するためのリスト
    detailed_results = []

    # tqdmでプログレスバーを表示
    for sim_file_path in tqdm(sim_files, desc="レース処理中"):
        try:
            with open(sim_file_path, 'r', encoding='utf-8') as f:
                sim_data = json.load(f)
            
            race_id = sim_data['race_id']
            win_probs = sim_data['win_probs']

            # races_dfから該当レースの結果を取得
            if race_id not in races_df.index:
                continue # races.parquet に該当レースがなければスキップ
            
            race_results_df = races_df.loc[[race_id]].copy()

            for _, horse in race_results_df.iterrows():
                horse_number = str(horse['horse_number'])
                win_prob = win_probs.get(horse_number)
                win_odds = horse['win_odds']
                finish_position = horse['finish_position']

                if win_prob is None or pd.isna(win_odds) or pd.isna(finish_position):
                    continue

                expected_value = win_prob * win_odds

                # 期待値が閾値を超えた場合のみログに追加
                if expected_value > EV_THRESHOLD:
                    bet_placed = True
                    total_bets += 1
                    total_investment += BET_AMOUNT
                    payout = 0
                    
                    if finish_position == 1:
                        total_wins += 1
                        payout = BET_AMOUNT * win_odds
                        total_return += payout
                    
                    detailed_results.append({
                        "race_id": race_id,
                        "horse_number": horse['horse_number'],
                        "horse_name": horse['horse_name'],
                        "win_prob": win_prob,
                        "win_odds": win_odds,
                        "expected_value": expected_value,
                        "finish_position": int(finish_position),
                        "bet_amount": BET_AMOUNT,
                        "payout": payout
                    })
        except Exception as e:
            # tqdmを使っている場合、printの代わりにtqdm.writeを使用
            tqdm.write(f"ファイル処理中にエラー: {sim_file_path} - {e}")

    # 4. 詳細ログをCSVファイルに保存
    print(f"\n詳細な投資ログを {DETAILS_OUTPUT_FILE} に保存中...")
    details_df = pd.DataFrame(detailed_results)
    details_df.to_csv(DETAILS_OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print("保存完了。")

    # 5. 最終サマリーを作成してファイルとコンソールに出力
    print(f"最終サマリーを {SUMMARY_OUTPUT_FILE} に保存中...")
    
    roi = (total_return / total_investment) * 100 if total_investment > 0 else 0
    hit_rate = (total_wins / total_bets) * 100 if total_bets > 0 else 0

    summary_text = (
        f"--- 2024年 期待値ベース投資 全レースシミュレーション結果 ---\n"
        f"\n"
        f"【集計期間】\n"
        f"  - 2024年（シミュレーションデータが存在する全レース）\n"
        f"  - 分析レース数: {len(sim_files)}\n"
        f"\n"
        f"【投資戦略】\n"
        f"  - 期待値 > {EV_THRESHOLD} の単勝馬券に一律 {BET_AMOUNT}円を投資\n"
        f"\n"
        f"【パフォーマンスサマリー】\n"
        f"  - 総投資回数: {total_bets} 回\n"
        f"  - 総投資額: {total_investment} 円\n"
        f"  - 総払戻額: {total_return:,.0f} 円\n"
        f"  - 純利益: {total_return - total_investment:,.0f} 円\n"
        f"  - 回収率 (ROI): {roi:.2f} %\n"
        f"  - 的中率: {hit_rate:.2f} % ({total_wins} / {total_bets})\n"
        f"\n"
        f"【備考】\n"
        f"  - 詳細な投資内容は {DETAILS_OUTPUT_FILE} を参照してください。\n"
    )

    with open(SUMMARY_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(summary_text)
    
    print("保存完了。")
    print("\n" + summary_text)

if __name__ == '__main__':
    analyze_all_races_ev_betting()
