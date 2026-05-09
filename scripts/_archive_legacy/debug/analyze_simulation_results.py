# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import json
import logging
import sys
import numpy as np

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(message)s')

def main():
    project_root = Path(__file__).resolve().parent
    sim_dir = project_root / 'data/simulations' # プロジェクトルート直下のdata/simulations
    if not sim_dir.exists():
        # keibaai/data/simulations も確認
        sim_dir = project_root / 'keibaai/data/simulations'
    
    races_dir = project_root / 'keibaai/data/parsed/parquet/races'
    
    logging.info(f"シミュレーションディレクトリ: {sim_dir}")
    logging.info(f"レース結果ディレクトリ: {races_dir}")
    
    if not sim_dir.exists() or not races_dir.exists():
        logging.error("データディレクトリまたはレース結果ディレクトリが見つかりません。")
        return

    # 1. レース結果データのロード（正解データ）
    logging.info("レース結果データをロード中...")
    # ディレクトリ内のparquetファイルを検索
    parquet_files = list(races_dir.glob('*.parquet'))
    if not parquet_files:
        logging.error("レース結果データ(.parquet)が見つかりません。")
        return
    
    races_path = parquet_files[0]
    logging.info(f"  読み込みファイル: {races_path.name}")
    races_df = pd.read_parquet(races_path)
    
    # 必要なカラム: race_id, horse_number, finish_position, win_odds
    req_cols = ['race_id', 'horse_number', 'finish_position', 'win_odds']
    
    # カラム存在確認
    missing_cols = [col for col in req_cols if col not in races_df.columns]
    if missing_cols:
        logging.error(f"レース結果データに必要なカラムがありません: {missing_cols}")
        return
        
    results_df = races_df[req_cols].copy()
    # 型変換
    results_df['race_id'] = results_df['race_id'].astype(str)
    results_df['horse_number'] = pd.to_numeric(results_df['horse_number'], errors='coerce').fillna(0).astype(int)
    results_df['finish_position'] = pd.to_numeric(results_df['finish_position'], errors='coerce')
    results_df['win_odds'] = pd.to_numeric(results_df['win_odds'], errors='coerce')

    # 1着の馬を特定
    winners = results_df[results_df['finish_position'] == 1][['race_id', 'horse_number', 'win_odds']].set_index('race_id')
    
    # 2. シミュレーション結果のロード
    logging.info("シミュレーション結果をロード中...")
    json_files = list(sim_dir.glob('*.json'))
    logging.info(f"  ファイル数: {len(json_files)}")
    
    total_bet = 0
    total_return = 0
    n_races = 0
    n_hits = 0
    
    # 簡易的な戦略: 勝率が最も高い馬に単勝100円賭ける
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            race_id = str(data.get('race_id'))
            win_probs = data.get('win_probs', {})
            
            if not win_probs:
                continue
                
            # 勝率最大の馬番を探す
            # キーは文字列なので数値に変換してソートなどが必要だが、ここでは最大値を持つキーを探す
            best_horse_str = max(win_probs, key=win_probs.get)
            best_horse_num = int(best_horse_str)
            max_prob = win_probs[best_horse_str]
            
            # 閾値（例: 勝率20%以上なら賭ける）
            if max_prob < 0.2:
                continue
                
            n_races += 1
            bet_amount = 100
            total_bet += bet_amount
            
            # 結果確認
            if race_id in winners.index:
                winner_info = winners.loc[race_id]
                # 重複がある場合（同着など）はSeriesではなくDataFrameになる可能性がある
                if isinstance(winner_info, pd.DataFrame):
                    winner_info = winner_info.iloc[0] # 簡易的に最初の1頭
                
                winner_num = int(winner_info['horse_number'])
                win_odds = float(winner_info['win_odds'])
                
                if best_horse_num == winner_num:
                    return_amount = bet_amount * win_odds
                    total_return += return_amount
                    n_hits += 1
                    # logging.info(f"  的中! レース{race_id}: 馬番{best_horse_num} (odds {win_odds})")
            
        except Exception as e:
            logging.warning(f"ファイル読み込みエラー {json_file.name}: {e}")

    # 結果表示
    logging.info("-" * 40)
    logging.info("【簡易バックテスト結果】")
    logging.info("戦略: 勝率20%以上の馬の中で、最も勝率が高い馬に単勝100円")
    logging.info("-" * 40)
    logging.info(f"対象レース数: {n_races}")
    logging.info(f"的中レース数: {n_hits}")
    if n_races > 0:
        logging.info(f"的中率: {n_hits / n_races * 100:.2f}%")
        logging.info(f"総投資額: {total_bet}円")
        logging.info(f"総回収額: {total_return}円")
        logging.info(f"回収率 (ROI): {total_return / total_bet * 100:.2f}%")
    else:
        logging.info("対象レースがありませんでした。")
    logging.info("-" * 40)

if __name__ == '__main__':
    main()
