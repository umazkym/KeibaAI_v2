import pandas as pd
import pickle
from pathlib import Path

def calculate_accuracy():
    """μv2.0モデルの的中率計算（オッズなし版）"""
    
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
    
    # 7. 的中率計算
    print("\n" + "=" * 80)
    print("μv2.0モデル 的中率分析")
    print("=" * 80)
    
    # レース数のカウント
    total_races = test_df['race_id'].nunique()
    print(f"\n総レース数: {total_races:,}レース")
    print(f"総出走頭数: {len(test_df):,}頭")
    
    # Top1戦略
    print(f"\n【Top1戦略】予測1位の馬")
    top1_df = test_df[test_df['rank_pred'] == 1].copy()
    
    top1_df['is_hit'] = (top1_df['is_win'] == 1)
    hits = top1_df[top1_df['is_hit']]
    
    accuracy1 = len(hits) / len(top1_df) if len(top1_df) > 0 else 0
    
    print(f"  予測レース数: {len(top1_df):,}レース")
    print(f"  的中数: {len(hits):,}レース")
    print(f"  ★ 的中率: {accuracy1:.2%}")
    
    # Top3戦略
    print(f"\n【Top3戦略】予測上位3頭のいずれか")
    
    # レースごとにTop3のいずれかが的中したかチェック
    top3_df = test_df[test_df['rank_pred'] <= 3].copy()
    race_hit = top3_df.groupby('race_id')['is_win'].max()  # レースごとに1つでも的中すれば1
    
    accuracy3 = race_hit.sum() / len(race_hit)
    
    print(f"  予測レース数: {len(race_hit):,}レース")
    print(f"  的中数: {race_hit.sum():,}レース")
    print(f"  ★ 的中率: {accuracy3:.2%}")
    
    # 月別的中率
    print(f"\n【月別的中率（Top1戦略）】")
    top1_df['month'] = pd.to_datetime(top1_df['race_date']).dt.to_period('M')
    
    monthly_stats = []
    for month, group in top1_df.groupby('month'):
        hits_m = group[group['is_hit']]
        acc = len(hits_m) / len(group) if len(group) > 0 else 0
        
        monthly_stats.append({
            'month': str(month),
            'races': len(group),
            'hits': len(hits_m),
            'accuracy': acc
        })
    
    monthly_df = pd.DataFrame(monthly_stats)
    for _, row in monthly_df.iterrows():
        print(f"  {row['month']}: 的中率 {row['accuracy']:6.1%} | {row['hits']:3d}/{row['races']:3d}レース")
    
    print("\n" + "=" * 80)
    print("備考")
    print("=" * 80)
    print("⚠️ オッズデータが存在しないため、ROI（回収率）の計算はできません。")
    print("   ROI評価には以下のいずれかが必要です：")
    print("   - 確定オッズ（win_odds）")
    print("   - 前日オッズ（morning_odds）")
    print("\n   オッズデータを取得後、再度ROI計算を実行してください。")
    print("=" * 80)
    
    # 結果を保存
    result_path = Path('keibaai/models/mu_v2/accuracy_result.txt')
    with open(result_path, 'w', encoding='utf-8') as f:
        f.write(f"μv2.0モデル 的中率分析結果\n")
        f.write(f"{'=' * 80}\n\n")
        f.write(f"テストデータ: 2024年以降\n")
        f.write(f"総レース数: {total_races:,}レース\n\n")
        f.write(f"【Top1戦略】\n")
        f.write(f"  的中率: {accuracy1:.2%}\n\n")
        f.write(f"【Top3戦略】\n")
        f.write(f"  的中率: {accuracy3:.2%}\n\n")
        f.write(f"備考: オッズデータ不足のためROI計算不可\n")
    
    print(f"\n結果を保存しました: {result_path}")

if __name__ == "__main__":
    calculate_accuracy()
