import pandas as pd
import pickle
from pathlib import Path
import numpy as np

def calculate_roi():
    """μv2.0モデルのROI計算"""
    
    # 1. データ読み込み
    data_path = Path('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    model_path = Path('keibaai/models/mu_v2/mu_v2_model.pkl')
    
    print("データとモデルを読み込んでいます...")
    df = pd.read_parquet(data_path)
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    # 2. テストデータ抽出（2024年以降）
    df['race_date'] = pd.to_datetime(df['race_date'])
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    
    print(f"テストデータ: {len(test_df):,}行")
    
    # 3. ターゲット作成
    if 'is_win' not in test_df.columns and 'finish_position' in test_df.columns:
        test_df['is_win'] = (test_df['finish_position'] == 1).astype(int)
    
    # 4. 特徴量抽出
    feature_cols = model.feature_name()
    
    # 5. 予測
    print("予測を実行中...")
    test_df['pred_prob'] = model.predict(test_df[feature_cols])
    
    # 6. レースごとに順位付け
    test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    # 7. win_oddsの確認と取得
    if 'win_odds' not in test_df.columns:
        print("win_oddsがデータセットに含まれていません。")
        print("horses_performance_fixed.parquetから取得を試みます...")
        
        perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance_fixed.parquet')
        if perf_path.exists():
            perf_df = pd.read_parquet(perf_path)
            
            # race_id, horse_idでマージ
            if 'win_odds' in perf_df.columns:
                odds_df = perf_df[['race_id', 'horse_id', 'win_odds']].copy()
                # 型を統一
                test_df['race_id'] = test_df['race_id'].astype(str)
                test_df['horse_id'] = test_df['horse_id'].astype(str)
                odds_df['race_id'] = odds_df['race_id'].astype(str)
                odds_df['horse_id'] = odds_df['horse_id'].astype(str)
                
                test_df = test_df.merge(odds_df, on=['race_id', 'horse_id'], how='left', suffixes=('', '_perf'))
                print(f"win_oddsをマージしました。欠損数: {test_df['win_odds'].isna().sum():,}")
            else:
                print("horses_performance_fixed.parquetにもwin_oddsがありません。")
                return
        else:
            print("horses_performance_fixed.parquetが見つかりません。")
            return
    
    # 8. ROI計算
    print("\n" + "=" * 80)
    print("ROI計算結果")
    print("=" * 80)
    
    # Top1戦略
    top1_df = test_df[test_df['rank_pred'] == 1].copy()
    
    print(f"\n【Top1戦略】予測1位の馬に毎回100円ベット")
    print(f"  総レース数: {len(top1_df):,}レース")
    
    # 的中判定
    top1_df['is_hit'] = (top1_df['is_win'] == 1)
    hits = top1_df[top1_df['is_hit']]
    
    accuracy = len(hits) / len(top1_df) if len(top1_df) > 0 else 0
    print(f"  的中数: {len(hits):,}レース")
    print(f"  的中率: {accuracy:.2%}")
    
    # オッズがあるレースのみで計算
    top1_valid = top1_df.dropna(subset=['win_odds'])
    hits_valid = top1_valid[top1_valid['is_hit']]
    
    if len(top1_valid) == 0:
        print("\n  ⚠️ オッズデータが不足しているため、ROI計算ができません。")
        return
    
    bet_amount = len(top1_valid) * 100
    return_amount = hits_valid['win_odds'].sum() * 100
    roi = (return_amount / bet_amount) if bet_amount > 0 else 0
    
    print(f"\n  オッズ有効レース数: {len(top1_valid):,}レース")
    print(f"  賭け金総額: {bet_amount:,}円")
    print(f"  払戻総額: {return_amount:,.0f}円")
    print(f"  ★ ROI（回収率）: {roi:.2%}")
    print(f"  収支: {return_amount - bet_amount:+,.0f}円")
    
    # Top3戦略
    print(f"\n【Top3戦略】予測上位3頭に各100円ベット")
    top3_df = test_df[test_df['rank_pred'] <= 3].copy()
    
    print(f"  総ベット数: {len(top3_df):,}頭")
    
    top3_df['is_hit'] = (top3_df['is_win'] == 1)
    hits3 = top3_df[top3_df['is_hit']]
    
    top3_valid = top3_df.dropna(subset=['win_odds'])
    hits3_valid = top3_valid[top3_valid['is_hit']]
    
    if len(top3_valid) > 0:
        bet_amount3 = len(top3_valid) * 100
        return_amount3 = hits3_valid['win_odds'].sum() * 100
        roi3 = (return_amount3 / bet_amount3) if bet_amount3 > 0 else 0
        
        print(f"  オッズ有効ベット数: {len(top3_valid):,}頭")
        print(f"  賭け金総額: {bet_amount3:,}円")
        print(f"  払戻総額: {return_amount3:,.0f}円")
        print(f"  ★ ROI（回収率）: {roi3:.2%}")
        print(f"  収支: {return_amount3 - bet_amount3:+,.0f}円")
    
    # 月別ROI
    print(f"\n【月別ROI（Top1戦略）】")
    top1_valid['month'] = pd.to_datetime(top1_valid['race_date']).dt.to_period('M')
    
    monthly_stats = []
    for month, group in top1_valid.groupby('month'):
        hits_m = group[group['is_hit']]
        bet_m = len(group) * 100
        return_m = hits_m['win_odds'].sum() * 100
        roi_m = (return_m / bet_m) if bet_m > 0 else 0
        
        monthly_stats.append({
            'month': str(month),
            'races': len(group),
            'hits': len(hits_m),
            'accuracy': len(hits_m) / len(group) if len(group) > 0 else 0,
            'roi': roi_m
        })
    
    monthly_df = pd.DataFrame(monthly_stats)
    for _, row in monthly_df.iterrows():
        print(f"  {row['month']}: ROI {row['roi']:6.1%} | 的中率 {row['accuracy']:5.1%} | {row['hits']:3d}/{row['races']:3d}レース")
    
    print("\n" + "=" * 80)
    
    # 結果を保存
    result_path = Path('keibaai/models/mu_v2/roi_result.txt')
    with open(result_path, 'w', encoding='utf-8') as f:
        f.write(f"μv2.0モデル ROI計算結果\n")
        f.write(f"{'=' * 80}\n\n")
        f.write(f"テストデータ: 2024年以降\n")
        f.write(f"総レース数: {len(top1_df):,}レース\n\n")
        f.write(f"【Top1戦略】\n")
        f.write(f"  的中率: {accuracy:.2%}\n")
        f.write(f"  ROI: {roi:.2%}\n")
        f.write(f"  収支: {return_amount - bet_amount:+,.0f}円\n")
    
    print(f"\n結果を保存しました: {result_path}")

if __name__ == "__main__":
    calculate_roi()
