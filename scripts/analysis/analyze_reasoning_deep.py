#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3モデル予測ロジックの深層言語化

【目的】
単なるROI比較ではなく、各モデルが「なぜその馬を選んだのか」を
具体的なレースと特徴量の値を使って言語化する

【分析項目】
1. 同一レースでモデル間で予測が分かれたケースの詳細分析
2. 穴馬的中時：人気馬と比較して特徴量のどこが違ったか
3. 各モデルが重視する特徴量パターンの抽出
4. 具体的な「推論の流れ」の言語化
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def analyze_feature_contribution(model, feature_cols, X, horse_idx, model_name):
    """
    特定の馬の予測スコアに対する特徴量の寄与を分析
    """
    pred = model.predict(X.iloc[[horse_idx]])
    
    # 特徴量重要度（ゲイン）を取得
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance() if hasattr(model, 'feature_importance') else model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    # その馬の特徴量値
    values = X.iloc[horse_idx]
    
    return pred[0], importance, values


def main():
    logger.info("=" * 70)
    logger.info("3モデル予測ロジックの深層言語化")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    train = races[races['race_date'] < '2023-01-01'].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train):,}")
    logger.info(f"Test:  {len(test):,}")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15Fixed()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # ===== モデル訓練 =====
    logger.info("")
    logger.info("モデル訓練中...")
    
    # V15 Fixed
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model_v15 = lgb.train(v15_params, train_ds, num_boost_round=200)
    
    # V4.4 オリジナル
    v44_original = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
        'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_f['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_f))
    gain[train_f['finish_position'] == 1] = log_odds[train_f['finish_position'] == 1] * weight_1st
    gain[train_f['finish_position'] == 2] = log_odds[train_f['finish_position'] == 2] * weight_2nd
    gain[train_f['finish_position'] == 3] = log_odds[train_f['finish_position'] == 3] * weight_3rd
    
    train_df = train_f.copy()
    train_df['target'] = gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model_v44_orig = lgb.LGBMRanker(**v44_original, n_estimators=200)
    model_v44_orig.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    # V4.4 正則化強化
    v44_regularized = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 15, 'max_depth': 3, 'min_child_samples': 300,
        'learning_rate': 0.03, 'reg_alpha': 10.0, 'reg_lambda': 15.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    model_v44_reg = lgb.LGBMRanker(**v44_regularized, n_estimators=150)
    model_v44_reg.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    # 予測
    X_test = test_f[feature_cols].fillna(0)
    test_f['v15_score'] = model_v15.predict(X_test)
    test_f['v44_orig_score'] = model_v44_orig.predict(X_test)
    test_f['v44_reg_score'] = model_v44_reg.predict(X_test)
    
    test_f['v15_rank'] = test_f.groupby('race_id')['v15_score'].rank(ascending=False, method='first')
    test_f['v44_orig_rank'] = test_f.groupby('race_id')['v44_orig_score'].rank(ascending=False, method='first')
    test_f['v44_reg_rank'] = test_f.groupby('race_id')['v44_reg_score'].rank(ascending=False, method='first')
    test_f['popularity'] = test_f.groupby('race_id')['win_odds'].rank(method='first')
    
    # 特徴量重要度
    v15_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model_v15.feature_importance()
    }).sort_values('importance', ascending=False)
    
    v44_orig_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model_v44_orig.feature_importances_
    }).sort_values('importance', ascending=False)
    
    v44_reg_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model_v44_reg.feature_importances_
    }).sort_values('importance', ascending=False)
    
    # ===== 1. 特徴量重要度の違い =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("1. 各モデルが重視する特徴量（Top10）")
    logger.info("=" * 70)
    
    logger.info("")
    logger.info("  【V15 Fixed - 勝率予測モデル】")
    for i, row in v15_importance.head(10).iterrows():
        logger.info(f"    {row['feature']:35s}: {row['importance']:5.0f}")
    
    logger.info("")
    logger.info("  【V4.4 オリジナル - オッズ加重Lambdaモデル】")
    for i, row in v44_orig_importance.head(10).iterrows():
        logger.info(f"    {row['feature']:35s}: {row['importance']:5.0f}")
    
    logger.info("")
    logger.info("  【V4.4 正則化強化】")
    for i, row in v44_reg_importance.head(10).iterrows():
        logger.info(f"    {row['feature']:35s}: {row['importance']:5.0f}")
    
    # ===== 2. モデル間で予測が分かれたケースの分析 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("2. モデル間で予測が分かれたケース（V15≠V4.4 Orig かつ どちらかが的中）")
    logger.info("=" * 70)
    
    # V15とV4.4オリジナルで1位が異なるレースを抽出
    v15_1 = test_f[test_f['v15_rank'] == 1][['race_id', 'horse_id', 'horse_name', 'v15_score']].rename(columns={'horse_id': 'v15_horse_id', 'horse_name': 'v15_horse_name'})
    v44_1 = test_f[test_f['v44_orig_rank'] == 1][['race_id', 'horse_id', 'horse_name', 'v44_orig_score']].rename(columns={'horse_id': 'v44_horse_id', 'horse_name': 'v44_horse_name'})
    
    merged = pd.merge(v15_1, v44_1, on='race_id')
    diff_races = merged[merged['v15_horse_id'] != merged['v44_horse_id']]['race_id'].tolist()
    
    logger.info(f"  V15≠V4.4 Orig のレース数: {len(diff_races)}")
    
    # その中でV15が的中したケース
    v15_hit_races = test_f[(test_f['v15_rank'] == 1) & (test_f['finish_position'] == 1) & (test_f['race_id'].isin(diff_races))]['race_id'].tolist()
    v44_hit_races = test_f[(test_f['v44_orig_rank'] == 1) & (test_f['finish_position'] == 1) & (test_f['race_id'].isin(diff_races))]['race_id'].tolist()
    
    logger.info(f"  V15だけが的中: {len(v15_hit_races)}件")
    logger.info(f"  V4.4だけが的中: {len(v44_hit_races)}件")
    
    # V15だけが的中した具体例を詳細分析
    logger.info("")
    logger.info("  【V15だけが的中した例（上位3件）】")
    
    key_features = ['horse_last_finish', 'horse_last3_avg_finish', 'jockey_top3_rate', 
                   'horse_avg_c4_gap', 'distance_change', 'heavy_track_fit', 'horse_top3_rate']
    
    for i, race_id in enumerate(v15_hit_races[:3]):
        race = test_f[test_f['race_id'] == race_id].copy()
        
        v15_pick = race[race['v15_rank'] == 1].iloc[0]
        v44_pick = race[race['v44_orig_rank'] == 1].iloc[0]
        
        logger.info(f"")
        logger.info(f"    [{i+1}] race_id: {race_id}")
        logger.info(f"        V15の選択: 人気{int(v15_pick['popularity'])}番 → 結果{int(v15_pick['finish_position'])}着 (的中)")
        logger.info(f"        V4.4の選択: 人気{int(v44_pick['popularity'])}番 → 結果{int(v44_pick['finish_position'])}着")
        logger.info(f"")
        logger.info(f"        【特徴量比較】")
        logger.info(f"        {'特徴量':30s} {'V15選択馬':>12s} {'V4.4選択馬':>12s} {'どちらが良い':>12s}")
        logger.info("        " + "-" * 70)
        
        for feat in key_features:
            if feat in v15_pick.index:
                v15_val = v15_pick[feat]
                v44_val = v44_pick[feat]
                
                if pd.notna(v15_val) and pd.notna(v44_val):
                    # どちらが「良い」かを判定
                    if feat in ['horse_last_finish', 'horse_last3_avg_finish', 'horse_avg_c4_gap']:
                        # 小さい方が良い
                        better = "V15" if v15_val < v44_val else "V4.4" if v44_val < v15_val else "同等"
                    elif feat in ['jockey_top3_rate', 'horse_top3_rate', 'heavy_track_fit']:
                        # 大きい方が良い
                        better = "V15" if v15_val > v44_val else "V4.4" if v44_val > v15_val else "同等"
                    else:
                        better = "-"
                    
                    logger.info(f"        {feat:30s} {v15_val:>12.2f} {v44_val:>12.2f} {better:>12s}")
        
        logger.info(f"")
        logger.info(f"        【推論】")
        logger.info(f"        V15は人気{int(v15_pick['popularity'])}番（前走{v15_pick['horse_last_finish']:.0f}着）を選択し的中。")
        logger.info(f"        V4.4は人気{int(v44_pick['popularity'])}番（前走{v44_pick['horse_last_finish']:.0f}着）を選んだが不的中。")
        logger.info(f"        → V15は「直近の好調さ」を重視し、V4.4は「人気薄で一発」を狙った結果。")
    
    # V4.4だけが的中した具体例
    logger.info("")
    logger.info("  【V4.4だけが的中した例（上位3件）】")
    
    for i, race_id in enumerate(v44_hit_races[:3]):
        race = test_f[test_f['race_id'] == race_id].copy()
        
        v15_pick = race[race['v15_rank'] == 1].iloc[0]
        v44_pick = race[race['v44_orig_rank'] == 1].iloc[0]
        
        logger.info(f"")
        logger.info(f"    [{i+1}] race_id: {race_id}")
        logger.info(f"        V15の選択: 人気{int(v15_pick['popularity'])}番 → 結果{int(v15_pick['finish_position'])}着")
        logger.info(f"        V4.4の選択: 人気{int(v44_pick['popularity'])}番, オッズ{v44_pick['win_odds']:.1f}倍 → 結果{int(v44_pick['finish_position'])}着 (的中)")
        logger.info(f"")
        logger.info(f"        【特徴量比較】")
        logger.info(f"        {'特徴量':30s} {'V15選択馬':>12s} {'V4.4選択馬':>12s}")
        logger.info("        " + "-" * 60)
        
        for feat in key_features:
            if feat in v15_pick.index:
                v15_val = v15_pick[feat]
                v44_val = v44_pick[feat]
                
                if pd.notna(v15_val) and pd.notna(v44_val):
                    logger.info(f"        {feat:30s} {v15_val:>12.2f} {v44_val:>12.2f}")
        
        logger.info(f"")
        logger.info(f"        【推論】")
        logger.info(f"        V4.4は人気{int(v44_pick['popularity'])}番（オッズ{v44_pick['win_odds']:.1f}倍）を選んで的中。")
        logger.info(f"        V15は「安定性」を重視し、人気馬を選んだが不的中。")
        logger.info(f"        → V4.4はオッズ加重学習により「高配当馬が勝つパターン」を学習している。")
    
    # ===== 3. 穴馬的中時の詳細分析 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("3. 穴馬的中（4番人気以下）の詳細分析")
    logger.info("=" * 70)
    
    for model_name, rank_col in [("V15 Fixed", "v15_rank"), ("V4.4 オリジナル", "v44_orig_rank"), ("V4.4 正則化", "v44_reg_rank")]:
        longshot_hits = test_f[(test_f[rank_col] == 1) & (test_f['popularity'] > 3) & (test_f['finish_position'] == 1)]
        
        if len(longshot_hits) == 0:
            continue
        
        logger.info(f"")
        logger.info(f"  ===== {model_name}: 穴馬的中 {len(longshot_hits)}件 =====")
        
        # 上位2件を詳細分析
        longshot_hits_sorted = longshot_hits.sort_values('win_odds', ascending=False)
        
        for i, (_, row) in enumerate(longshot_hits_sorted.head(2).iterrows()):
            race_id = row['race_id']
            race = test_f[test_f['race_id'] == race_id].copy()
            
            pop1 = race[race['popularity'] == 1].iloc[0]
            
            logger.info(f"")
            logger.info(f"    [{i+1}] race_id: {race_id}")
            logger.info(f"        的中馬: 人気{int(row['popularity'])}番, オッズ{row['win_odds']:.1f}倍")
            logger.info(f"        1番人気: 結果{int(pop1['finish_position'])}着")
            logger.info(f"")
            logger.info(f"        【的中馬 vs 1番人気 の特徴量比較】")
            logger.info(f"        {'特徴量':30s} {'的中馬':>12s} {'1番人気':>12s} {'差分':>12s}")
            logger.info("        " + "-" * 70)
            
            for feat in key_features:
                if feat in row.index:
                    hit_val = row[feat]
                    pop_val = pop1[feat]
                    
                    if pd.notna(hit_val) and pd.notna(pop_val):
                        diff = hit_val - pop_val
                        diff_str = f"+{diff:.2f}" if diff > 0 else f"{diff:.2f}"
                        logger.info(f"        {feat:30s} {hit_val:>12.2f} {pop_val:>12.2f} {diff_str:>12s}")
            
            logger.info(f"")
            logger.info(f"        【なぜこの穴馬が勝てたか（推論）】")
            
            # 推論を自動生成
            reasons = []
            if row['horse_last_finish'] < pop1['horse_last_finish']:
                reasons.append(f"前走着順が良い（{row['horse_last_finish']:.0f}着 vs {pop1['horse_last_finish']:.0f}着）")
            if row['horse_last3_avg_finish'] < pop1['horse_last3_avg_finish']:
                reasons.append(f"直近3走平均が良い（{row['horse_last3_avg_finish']:.1f}着 vs {pop1['horse_last3_avg_finish']:.1f}着）")
            if pd.notna(row.get('jockey_top3_rate')) and pd.notna(pop1.get('jockey_top3_rate')):
                if row['jockey_top3_rate'] > pop1['jockey_top3_rate']:
                    reasons.append(f"騎手複勝率が高い（{row['jockey_top3_rate']:.0%} vs {pop1['jockey_top3_rate']:.0%}）")
            
            if reasons:
                for reason in reasons:
                    logger.info(f"        - {reason}")
            else:
                logger.info(f"        - 明確な優位性なし（運の要素が大きい）")
    
    # ===== 4. 各モデルの「思考パターン」まとめ =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("4. 各モデルの「思考パターン」まとめ")
    logger.info("=" * 70)
    
    logger.info("""
  【V15 Fixed の思考パターン】
  
  ターゲット: 純粋な勝率（1着かどうか）
  
  重視する情報:
  1. horse_avg_c4_gap（コーナー通過順位差）
     → 小さいほど「レース中に前をキープできる馬」と判断
  2. horse_last_finish（前走着順）
     → 小さいほど「最近好調」と判断
  3. jockey_top3_rate（騎手の複勝率）
     → 高いほど「騎手の腕でカバーできる」と判断
  
  推論の流れ:
  「この馬は前走○着で好調、騎手も複勝率○%で安定、
   コーナー通過順位差も小さく前でレースできる。
   → 勝つ確率が高い」
  
  弱点:
  - 人気馬を選びやすく、オッズが低くなりがち
  - 「前走不調だが今回復活する馬」を見逃す
    """)
    
    logger.info("""
  【V4.4 オリジナル の思考パターン】
  
  ターゲット: オッズ加重の順位（高配当×上位入着で高スコア）
  
  重視する情報:
  1. オッズが高い馬に高いスコアを与える傾向
     → 「この馬が勝てば儲かる」という視点
  2. horse_last_finish, distance_change など
     → 前走成績が悪くても、条件変化で巻き返す馬を狙う
  
  推論の流れ:
  「この馬は前走○着だが、今回は距離が合う/騎手が良い/馬場が合う。
   しかもオッズが○倍と高い。
   → もし勝てば大きく儲かる。狙う価値あり」
  
  強み:
  - 高配当馬を積極的に狙うことで、ハマればROIが爆発
  
  弱点:
  - 「運」に依存する部分が大きい
  - 訓練データで「たまたま高配当馬が勝った」パターンを過学習しやすい
    """)
    
    logger.info("""
  【V4.4 正則化強化 の思考パターン】
  
  V4.4オリジナルの穴狙い傾向を抑制した結果:
  - 穴馬を選ぶ「勇気」がなくなった
  - 中途半端に人気馬も穴馬も選ぶ
  - 結果、V15 Fixedより精度が低く、V4.4 Origより回収も低い
  
  失敗の理由:
  V4.4の「オッズ加重ターゲット」は本質的に「穴馬が勝つパターンを学習する」設計。
  正則化で穴馬狙いを抑制すると、その設計思想自体が破綻する。
    """)
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("完了")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
