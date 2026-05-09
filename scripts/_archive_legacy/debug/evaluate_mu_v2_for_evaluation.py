"""
μv2.0モデル評価スクリプト
既存evaluation形式（日別集計）で結果をCSVに出力
"""
import pandas as pd
import pickle
from pathlib import Path
import numpy as np
from datetime import datetime

def evaluate_mu_v2_model():
    """
    μv2.0モデルを評価し、既存evaluation形式（日別集計）のCSVを生成
    
    出力形式:
    - date: レース日
    - rmse: N/A (μv2.0は分類モデル)
    - spearman_corr: N/A (μv2.0は分類モデル)
    - hit_rate: 的中率
    - race_count: レース数
    - recovery_rate: 回収率（オッズがあれば）
    - total_bet: 総賭け金
    - total_return: 総払戻金
    """
    print("=" * 80)
    print("μv2.0モデル評価（evaluation形式 - 日別集計）")
    print("=" * 80)
    
    # 1. データとモデル読み込み
    data_path = Path('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    model_path = Path('keibaai/models/mu_v2/mu_v2_model.pkl')
    
    print("\nデータとモデルを読み込んでいます...")
    df = pd.read_parquet(data_path)
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    # 2. テストデータ抽出（2024年以降）
    df['race_date'] = pd.to_datetime(df['race_date'])
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    
    print(f"テストデータ: {len(test_df):,}行 / {test_df['race_id'].nunique():,}レース")
    
    # 3. is_win作成
    if 'is_win' not in test_df.columns and 'finish_position' in test_df.columns:
        test_df['is_win'] = (test_df['finish_position'] == 1).astype(int)
    
    # 4. win_odds取得（races.parquetから）
    print("\nraces.parquetから確定オッズを取得中...")
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    
    if not races_path.exists():
        print(f"⚠️ {races_path} が見つかりません")
        has_odds = False
    else:
        races_df = pd.read_parquet(races_path)
        
        if 'win_odds' in races_df.columns:
            # race_id, horse_id, win_oddsのみ抽出
            odds_df = races_df[['race_id', 'horse_id', 'win_odds']].copy()
            
            # 型を統一
            test_df['race_id'] = test_df['race_id'].astype(str)
            test_df['horse_id'] = test_df['horse_id'].astype(str)
            odds_df['race_id'] = odds_df['race_id'].astype(str)
            odds_df['horse_id'] = odds_df['horse_id'].astype(str)
            
            # マージ
            before_count = len(test_df)
            test_df = test_df.merge(
                odds_df, 
                on=['race_id', 'horse_id'], 
                how='left', 
                suffixes=('', '_races')
            )
            
            # win_oddsがあるデータ
            odds_available = test_df['win_odds'].notna().sum()
            print(f"  マージ成功: {odds_available:,} / {len(test_df):,} 行 ({odds_available/len(test_df)*100:.1f}%) にオッズあり")
            
            has_odds = odds_available > 0
        else:
            print("⚠️ races.parquetにwin_oddsカラムがありません")
            has_odds = False
    
    # 5. 特徴量抽出と予測
    # モデルから特徴量名を取得（LightGBMの仕様に合わせて修正）
    feature_cols = model.feature_name()
    
    # データセットに存在しない特徴量がある場合、0で埋める（安全策）
    missing_cols = [c for c in feature_cols if c not in test_df.columns]
    if missing_cols:
        print(f"⚠️ {len(missing_cols)}個の特徴量がデータセットに見つかりません。0で埋めます。")
        # print(f"  例: {missing_cols[:5]}")
        for c in missing_cols:
            test_df[c] = 0.0
            
    print(f"特徴量数: {len(feature_cols)}")
    
    print("\n予測を実行中...")
    test_df['pred_prob'] = model.predict(test_df[feature_cols])
    
    # 6. レースごとに順位付け
    test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    # 7. 日別集計
    print("\n日別集計を実行中...")
    
    test_df['date'] = test_df['race_date'].dt.date
    daily_results = []
    
    for date, day_df in test_df.groupby('date'):
        # Top1戦略：各レースの予測1位に100円ベット
        top1_df = day_df[day_df['rank_pred'] == 1].copy()
        
        race_count = len(top1_df)
        hits = top1_df[top1_df['is_win'] == 1]
        hit_rate = len(hits) / race_count if race_count > 0 else 0
        
        # オッズがある場合のROI計算
        if has_odds:
            top1_valid = top1_df.dropna(subset=['win_odds'])
            hits_valid = top1_valid[top1_valid['is_win'] == 1]
            
            total_bet = len(top1_valid) * 100
            total_return = hits_valid['win_odds'].sum() * 100
            recovery_rate = (total_return / total_bet) if total_bet > 0 else 0
        else:
            total_bet = race_count * 100
            total_return = 0
            recovery_rate = 0
        
        daily_results.append({
            'date': date,
            'rmse': np.nan,  # 分類モデルのため該当なし
            'spearman_corr': np.nan,  # 分類モデルのため該当なし
            'hit_rate': hit_rate,
            'race_count': race_count,
            'recovery_rate': recovery_rate,
            'total_bet': total_bet,
            'total_return': total_return
        })
    
    # DataFrame化
    eval_df = pd.DataFrame(daily_results)
    eval_df = eval_df.sort_values('date')
    
    # 8. 集計統計表示
    print("\n" + "=" * 80)
    print("集計結果")
    print("=" * 80)
    
    total_races = eval_df['race_count'].sum()
    overall_hit_rate = eval_df['hit_rate'].mean()
    
    if has_odds and eval_df['total_bet'].sum() > 0:
        overall_recovery = eval_df['total_return'].sum() / eval_df['total_bet'].sum()
        print(f"\n総レース数: {total_races:,}レース")
        print(f"全体的中率: {overall_hit_rate:.2%}")
        print(f"全体回収率: {overall_recovery:.2%}")
        print(f"総収支: {eval_df['total_return'].sum() - eval_df['total_bet'].sum():+,.0f}円")
    else:
        print(f"\n総レース数: {total_races:,}レース")
        print(f"全体的中率: {overall_hit_rate:.2%}")
        print("⚠️ オッズデータ不足のため回収率は計算不可")
    
    # 9. CSV保存
    output_dir = Path('keibaai/data/evaluation')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = output_dir / f'evaluation_results_mu_v2_{timestamp}.csv'
    
    eval_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 評価結果を保存しました: {output_path}")
    print(f"   フォーマット: {list(eval_df.columns)}")
    print("=" * 80)
    
    return eval_df

if __name__ == "__main__":
    evaluate_mu_v2_model()
