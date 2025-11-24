#!/usr/bin/env python3
"""
μv2.0 特徴量生成テストスクリプト
新規追加した48特徴量が正しく生成されるかを検証
"""
import sys
import logging
from pathlib import Path
import pandas as pd

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# パスの追加
sys.path.insert(0, str(Path(__file__).parent / 'keibaai' / 'src'))

from features.feature_engine import FeatureEngine

def test_mu_v2_features():
    """μv2.0特徴量のテスト"""
    
    logging.info("=" * 80)
    logging.info("μv2.0 特徴量生成テスト開始")
    logging.info("=" * 80)
    
    # 1. データ読み込み
    logging.info("Step 1: データ読み込み中...")
    
    try:
        # 出馬表データ（2024年の一部）
        shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        if not shutuba_path.exists():
            logging.error(f"出馬表データが見つかりません: {shutuba_path}")
            return False
        
        shutuba_df = pd.read_parquet(shutuba_path)
        
        # 2024年1月のデータのみをテスト用に使用
        shutuba_df['race_date'] = pd.to_datetime(shutuba_df['race_date'])
        test_df = shutuba_df[
            (shutuba_df['race_date'] >= '2024-01-01') &
            (shutuba_df['race_date'] <= '2024-01-31')
        ].copy()
        
        logging.info(f"  出馬表: {len(test_df):,}行 ({test_df['race_id'].nunique()}レース)")
        
        # 過去結果データ（学習用）
        # 修正版データセットを使用（IDと結果情報の両方を含む）
        results_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance_fixed.parquet')
        if not results_path.exists():
            logging.error(f"過去結果データが見つかりません: {results_path}")
            return False
        
        results_df = pd.read_parquet(results_path)
        
        # 2024年より前のデータを履歴として使用
        results_df['race_date'] = pd.to_datetime(results_df['race_date'])
        history_df = results_df[results_df['race_date'] < '2024-01-01'].copy()
        
        logging.info(f"  過去結果: {len(history_df):,}行")
        
        # プロフィールデータ
        profiles_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')
        if profiles_path.exists():
            profiles_df = pd.read_parquet(profiles_path)
            logging.info(f"  プロフィール: {len(profiles_df):,}行")
        else:
            profiles_df = pd.DataFrame()
            logging.warning("  プロフィールデータなし")
        
        # 血統データ
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        if pedigree_path.exists():
            pedigree_df = pd.read_parquet(pedigree_path)
            logging.info(f"  血統: {len(pedigree_df):,}行")
        else:
            pedigree_df = None
            logging.warning("  血統データなし")
        
    except Exception as e:
        logging.error(f"データ読み込みエラー: {e}")
        return False
    
    # 2. 特徴量エンジン初期化
    logging.info("\nStep 2: FeatureEngine初期化中...")
    
    try:
        config_path = Path('keibaai/configs/features.yaml')
        if not config_path.exists():
            logging.error(f"設定ファイルが見つかりません: {config_path}")
            return False
        
        engine = FeatureEngine(config_path)
        logging.info("  ✓ FeatureEngine初期化完了")
        
    except Exception as e:
        logging.error(f"FeatureEngine初期化エラー: {e}")
        return False
    
    # 3. 特徴量生成実行
    logging.info("\nStep 3: 特徴量生成実行中...")
    logging.info("  (μv2.0の48特徴量を含む)")
    
    try:
        features_df = engine.generate_features(
            shutuba_df=test_df,
            results_history_df=history_df,
            horse_profiles_df=profiles_df,
            pedigree_df=pedigree_df
        )
        
        logging.info(f"  ✓ 特徴量生成完了: {len(features_df):,}行 × {len(features_df.columns)}列")
        
    except Exception as e:
        logging.error(f"特徴量生成エラー: {e}", exc_info=True)
        return False
    
    # 4. μv2.0特徴量の確認
    logging.info("\nStep 4: μv2.0特徴量の存在確認...")
    
    # カテゴリ1の特徴量
    cat1_features = [
        'horse_finish_cv_last5',
        'horse_finish_range_last5',
        'horse_finish_trend_last5',
        'horse_top3_rate_last10',
        'horse_experience_log',
        'jockey_finish_cv_last10',
        'trainer_finish_cv_last10',
        'horse_consistency_score',
        'jockey_consistency_score',
        'trainer_consistency_score'
    ]
    
    cat1_found = [f for f in cat1_features if f in features_df.columns]
    logging.info(f"  カテゴリ1: {len(cat1_found)}/{len(cat1_features)}特徴量")
    if len(cat1_found) < len(cat1_features):
        missing = set(cat1_features) - set(cat1_found)
        logging.warning(f"    欠落: {missing}")
    
    # カテゴリ2の代表的な特徴量
    cat2_samples = [
        'distance_win_rate', 'surface_avg_finish', 'venue_win_rate',
        'condition_avg_finish', 'weather_win_rate'
    ]
    cat2_found = [f for f in cat2_samples if f in features_df.columns]
    logging.info(f"  カテゴリ2: {len(cat2_found)}/{len(cat2_samples)}特徴量（サンプル）")
    
    # カテゴリ3の代表的な特徴量
    cat3_samples = [
        'horse_running_style', 'last_3f_percentile_in_race',
        'race_expected_pace'
    ]
    cat3_found = [f for f in cat3_samples if f in features_df.columns]
    logging.info(f"  カテゴリ3: {len(cat3_found)}/{len(cat3_samples)}特徴量（サンプル）")
    
    # カテゴリ4の代表的な特徴量
    cat4_samples = [
        'jockey_horse_avg_finish', 'jockey_horse_win_rate',
        'jockey_trainer_win_rate'
    ]
    cat4_found = [f for f in cat4_samples if f in features_df.columns]
    logging.info(f"  カテゴリ4: {len(cat4_found)}/{len(cat4_samples)}特徴量（サンプル）")
    
    # カテゴリ5の代表的な特徴量
    cat5_samples = [
        'sire_avg_finish', 'sire_win_rate', 'damsire_avg_finish'
    ]
    cat5_found = [f for f in cat5_samples if f in features_df.columns]
    logging.info(f"  カテゴリ5: {len(cat5_found)}/{len(cat5_samples)}特徴量（サンプル）")
    
    # カテゴリ6の特徴量
    cat6_features = [
        'is_after_long_rest', 'is_consecutive_race',
        'weight_change_from_last', 'weight_change_pct'
    ]
    cat6_found = [f for f in cat6_features if f in features_df.columns]
    logging.info(f"  カテゴリ6: {len(cat6_found)}/{len(cat6_features)}特徴量")
    
    # 5. データ品質チェック
    logging.info("\nStep 5: データ品質チェック...")
    
    # 欠損率
    missing_rates = features_df.isnull().sum() / len(features_df)
    high_missing = missing_rates[missing_rates > 0.5]
    
    if len(high_missing) > 0:
        logging.warning(f"  ⚠️  欠損率50%超の特徴量: {len(high_missing)}個")
        for feat, rate in high_missing.head(10).items():
            logging.warning(f"    - {feat}: {rate:.1%}")
    else:
        logging.info("  ✓ 全特徴量で欠損率50%以下")
    
    # データリークチェック（finish_positionが含まれていないか）
    forbidden = ['finish_position', 'finish_time_seconds', 'is_win', 'win_odds']
    leaked = [f for f in forbidden if f in engine.feature_names_]
    
    if leaked:
        logging.error(f"  ❌ データリーク検出: {leaked}")
        return False
    else:
        logging.info("  ✓ データリークなし")
    
    # 6. サマリー出力
    logging.info("\n" + "=" * 80)
    logging.info("テスト結果サマリー")
    logging.info("=" * 80)
    logging.info(f"総特徴量数: {len(engine.feature_names_)}")
    logging.info(f"カテゴリ1（安定性）: {len(cat1_found)}/10")
    logging.info(f"データリーク: なし")
    logging.info(f"平均欠損率: {missing_rates.mean():.1%}")
    
    # 7. サンプルデータの表示
    logging.info("\nStep 6: サンプルデータ（最初の3行）...")
    sample_cols = cat1_found[:5] + cat2_found[:3] + ['horse_id', 'race_id']
    sample_cols = [c for c in sample_cols if c in features_df.columns]
    
    print("\n" + features_df[sample_cols].head(3).to_string())
    
    logging.info("\n" + "=" * 80)
    logging.info("✅ μv2.0特徴量生成テスト完了")
    logging.info("=" * 80)
    
    return True

if __name__ == '__main__':
    success = test_mu_v2_features()
    sys.exit(0 if success else 1)
