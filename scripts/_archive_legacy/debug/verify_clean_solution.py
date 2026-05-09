#!/usr/bin/env python3
"""
作業完了後の検証スクリプト
クリーン特徴量の再生成とモデル再学習が正しく完了したか確認
"""
import pickle
import pandas as pd
from pathlib import Path

def check_clean_features():
    """生成された特徴量がクリーンか確認"""
    print("=" * 70)
    print("1. 特徴量のクリーン度チェック")
    print("=" * 70)
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 
                   'is_win', 'odds', 'popularity']
    
    # 学習期間の各年をチェック
    years = [2020, 2021, 2022, 2023]
    all_clean = True
    
    for year in years:
        path = Path(f'keibaai/data/features/parquet/year={year}/month=1')
        if path.exists():
            df = pd.read_parquet(path)
            leaked = [v for v in target_vars if v in df.columns]
            
            if leaked:
                print(f"  ✗ {year}年: ターゲット変数が残存 {leaked}")
                all_clean = False
            else:
                print(f"  ✓ {year}年: クリーン ({len(df.columns)}列)")
        else:
            print(f"  ⚠ {year}年: データなし")
            all_clean = False
    
    return all_clean

def check_clean_model():
    """モデルがクリーンか確認"""
    print("\n" + "=" * 70)
    print("2. モデルのクリーン度チェック")
    print("=" * 70)
    
    model_path = Path('keibaai/data/models/mu_model.pkl')
    if not model_path.exists():
        print("  ✗ モデルファイルが見つかりません")
        return False
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    if hasattr(model, 'expected_features'):
        expected = model.expected_features
    elif hasattr(model, 'feature_names_'):
        expected = model.feature_names_
    else:
        print("  ⚠ モデルに期待特徴量リストがありません")
        return False
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 
                   'is_win', 'odds', 'popularity']
    leaked = [v for v in target_vars if v in expected]
    
    if leaked:
        print(f"  ✗ モデルにターゲット変数: {leaked}")
        return False
    else:
        print(f"  ✓ モデルはクリーン")
        print(f"    期待特徴量数: {len(expected)}")
        return True

def check_evaluation_results():
    """評価結果が現実的か確認"""
    print("\n" + "=" * 70)
    print("3. 評価結果チェック")
    print("=" * 70)
    
    results_path = Path('keibaai/data/evaluation/evaluation_results.csv')
    if not results_path.exists():
        print("  ⚠ 評価結果ファイルが見つかりません")
        print("    evaluate_model.pyを実行してください")
        return None
    
    df = pd.read_csv(results_path)
    
    if 'rmse' in df.columns:
        avg_rmse = df['rmse'].mean()
        if avg_rmse < 0.01:
            print(f"  ✗ RMSE が異常に低い: {avg_rmse:.4f}")
            print(f"    → まだデータリークがある可能性")
            return False
        elif 0.1 <= avg_rmse <= 0.5:
            print(f"  ✓ RMSE が現実的: {avg_rmse:.4f}")
            return True
        else:
            print(f"  ⚠ RMSE: {avg_rmse:.4f}")
            print(f"    → 評価が必要")
            return None
    
    return None

def main():
    print("\n" + "=" * 70)
    print("クリーン化作業の検証")
    print("=" * 70 + "\n")
    
    features_clean = check_clean_features()
    model_clean = check_clean_model()
    eval_ok = check_evaluation_results()
    
    print("\n" + "=" * 70)
    print("総合結果")
    print("=" * 70)
    
    if features_clean and model_clean:
        print("✓ 特徴量とモデルはクリーンです")
        if eval_ok:
            print("✓ 評価結果も正常です")
            print("\n🎉 データリーク問題は完全に解決しました！")
        elif eval_ok is None:
            print("⚠ 評価を実行してください:")
            print("  python keibaai/src/models/evaluate_model.py --model_dir keibaai/data/models --start_date 2024-01-01 --end_date 2024-01-31")
        else:
            print("✗ 評価結果が異常です - さらに調査が必要")
    else:
        print("✗ まだ問題があります:")
        if not features_clean:
            print("  - 特徴量の再生成が必要")
        if not model_clean:
            print("  - モデルの再学習が必要")
    
    print("=" * 70)

if __name__ == '__main__':
    main()
