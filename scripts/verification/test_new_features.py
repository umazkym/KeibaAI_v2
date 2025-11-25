import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
import logging

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "keibaai" / "src"))

from keibaai.src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_new_features():
    logging.info("新特徴量のテストを開始します...")

    # 1. ダミーデータの作成
    # 今回のレース (出馬表)
    shutuba_data = {
        'race_id': ['202401010101', '202401010101'],
        'horse_id': ['H001', 'H002'],
        'race_date': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-01')],
        'head_count': [16, 16],
        'distance_m': [2000, 2000],
        'track_surface': ['芝', '芝'],
        'venue': ['東京', '東京'],
        'bracket_number': [1, 2],
        'jockey_id': ['J001', 'J002'],
        'trainer_id': ['T001', 'T002'],
        # 予測時には欠損する可能性があるが、学習時には存在するカラム
        'passing_order': ['1-1-1-1', '2-2-2-2'], 
        'last_3f_time': [34.5, 35.0]
    }
    shutuba_df = pd.DataFrame(shutuba_data)

    # 過去の履歴データ (parquet_contents_report.mdを参考に必要なカラムを追加)
    history_data = {
        'race_id': ['202312010101', '202311010101', '202310010101'],
        'horse_id': ['H001', 'H001', 'H002'],
        'race_date': [pd.Timestamp('2023-12-01'), pd.Timestamp('2023-11-01'), pd.Timestamp('2023-10-01')],
        'finish_position': [1, 3, 2],
        'distance_m': [1800, 1600, 2000],
        'track_surface': ['芝', 'ダート', '芝'],
        'venue': ['中山', '東京', '京都'],
        'jockey_id': ['J001', 'J001', 'J002'],
        'trainer_id': ['T001', 'T001', 'T002'],
        'morning_odds': [2.5, 5.0, 3.0],
        'bracket_number': [1, 2, 3],
        'popularity': [1, 3, 2],
        'head_count': [16, 14, 16],
        'passing_order': ['1-1-1-1', '3-3-3-3', '2-2-2-2'],
        'last_3f_time': [34.5, 35.0, 34.8]
    }
    results_history_df = pd.DataFrame(history_data)
    
    # 馬プロフィール (最小限)
    horse_profiles_df = pd.DataFrame({
        'horse_id': ['H001', 'H002'],
        'sire_id': ['S001', 'S002'],
        'damsire_id': ['DS001', 'DS002']
    })

    # 設定 (最小限)
    config = {
        'feature_engine': {'version': 'test'},
        'feature_recipes': {
            'basic_features': {'enabled': True},
            'career_stats': {'enabled': True, 'stats': ['career_starts']},
            'advanced_features': {'enabled': True} # コード構造により暗黙的に有効化
        }
    }

    # 2. FeatureEngineの初期化
    engine = FeatureEngine(config)

    # 3. 特徴量生成
    logging.info("特徴量を生成中...")
    features_df = engine.generate_features(
        shutuba_df,
        results_history_df,
        horse_profiles_df
    )

    logging.info("特徴量生成成功。")
    logging.info(f"生成されたカラム数: {len(features_df.columns)}")
    logging.info(f"データ行数: {len(features_df)}")
    
    # デバッグ: 全カラムをソートして表示
    all_cols = sorted(features_df.columns.tolist())
    logging.info(f"\n=== 生成された全カラム ({len(all_cols)}個) ===")
    for i, col in enumerate(all_cols, 1):
        logging.info(f"  {i:3d}. {col}")
    logging.info("=" * 50)
    
    # 新特徴量に関連するカラムをフィルタリング
    new_feature_keywords = ['finish_std', 'finish_cv', 'distance_change', 'surface_change', 
                           'venue_change', 'combo_win_rate', 'early_speed', 'late_speed',
                           'days_since_last', 'is_rest', 'typical_running']
    
    logging.info("\n=== 新特徴量関連のカラム ===")
    for keyword in new_feature_keywords:
        matching = [col for col in all_cols if keyword in col]
        if matching:
            logging.info(f"  {keyword}: {matching}")
        else:
            logging.info(f"  {keyword}: なし")
    logging.info("=" * 50)

    # 4. 新特徴量の検証
    try:
        # 安定性指標
        if 'finish_std_last3' not in features_df.columns:
            logging.error(f"finish_std_last3 missing. Columns: {features_df.columns.tolist()}")
        assert 'finish_std_last3' in features_df.columns, "finish_std_last3 が見つかりません"
        assert 'finish_cv_last3' in features_df.columns, "finish_cv_last3 が見つかりません"
        logging.info("安定性指標: OK")

        # 条件変化スコア
        if 'distance_change' not in features_df.columns:
             logging.error(f"distance_change missing. Columns: {features_df.columns.tolist()}")
             if 'prev_distance_m' in features_df.columns:
                 logging.info(f"prev_distance_m exists. Head: {features_df[['horse_id', 'distance_m', 'prev_distance_m']].head()}")
             else:
                 logging.error("prev_distance_m missing")
        
        assert 'distance_change' in features_df.columns, "distance_change が見つかりません"
        assert 'surface_change' in features_df.columns, "surface_change が見つかりません"
        assert 'venue_change' in features_df.columns, "venue_change が見つかりません"
        
        # H001の値チェック (前走: 2023-12-01, 距離1800, 芝, 中山)
        # 今回: 2000, 芝, 東京
        # distance_change: |2000 - 1800| = 200
        # surface_change: 0 (芝 == 芝)
        # venue_change: 1 (東京 != 中山)
        
        h001_row = features_df[features_df['horse_id'] == 'H001'].iloc[0]
        logging.info(f"H001 distance_change: {h001_row.get('distance_change', 'N/A')}")
        
        if 'distance_change' in features_df.columns:
            assert h001_row['distance_change'] == 200, f"期待値 200, 実際 {h001_row['distance_change']}"
            assert h001_row['surface_change'] == 0, f"期待値 0, 実際 {h001_row['surface_change']}"
            assert h001_row['venue_change'] == 1, f"期待値 1, 実際 {h001_row['venue_change']}"
        logging.info("条件変化スコア: OK")

        # 騎手・調教師シナジー
        assert 'combo_win_rate' in features_df.columns, "combo_win_rate が見つかりません"
        logging.info("騎手・調教師シナジー: OK")

        # ペース適性
        assert 'horse_early_speed_index' in features_df.columns, "horse_early_speed_index が見つかりません"
        assert 'horse_late_speed_index' in features_df.columns, "horse_late_speed_index が見つかりません"
        logging.info("ペース適性: OK")

        # 休養明けフラグ
        assert 'days_since_last_race' in features_df.columns, "days_since_last_race が見つかりません"
        assert 'is_rest_return' in features_df.columns, "is_rest_return が見つかりません"
        
        # H001: 2024-01-01 - 2023-12-01 = 31 days
        assert h001_row['days_since_last_race'] == 31, f"期待値 31, 実際 {h001_row['days_since_last_race']}"
        assert h001_row['is_rest_return'] == 0, "期待値 0 (休養明けではない)"
        logging.info("休養明けフラグ: OK")

        logging.info("全てのテストに合格しました！")
    except AssertionError as e:
        logging.error(f"テスト失敗: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"予期せぬエラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_new_features()
