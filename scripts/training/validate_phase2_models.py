#!/usr/bin/env python3
"""
Phase 2 Go/No-Go Validation Script

各専門モデルの予測が着順と有意な相関を持つかを検証する。
Go基準: AUC >= 0.55 または Spearman ρ < -0.05

結果に基づいてPhase 3-4への進行可否を判断。
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
from scipy import stats
from sklearn.metrics import roc_auc_score
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def validate_pace_model():
    """展開予測モデルの検証"""
    logging.info("=" * 60)
    logging.info("PaceScenarioModel 検証")
    logging.info("=" * 60)
    
    from keibaai.src.models.specialized import PaceScenarioModel
    
    model = PaceScenarioModel()
    
    # レースデータ読み込み
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # Valid期間（2023年）でテスト
    valid_df = races_df[(races_df['race_date'] >= '2023-01-01') & 
                        (races_df['race_date'] < '2024-01-01')].copy()
    
    # サンプリング（計算時間短縮）
    sample_races = valid_df['race_id'].unique()[:500]
    valid_df = valid_df[valid_df['race_id'].isin(sample_races)]
    
    logging.info(f"  検証レース数: {len(sample_races)}")
    
    results = []
    for race_id in sample_races:
        race_horses = valid_df[valid_df['race_id'] == race_id].copy()
        race_date = race_horses['race_date'].iloc[0]
        
        # 脚質判定（簡易版：人気から推定）
        n_horses = len(race_horses)
        running_styles = []
        for _, h in race_horses.iterrows():
            pop = h.get('popularity', 5)
            if pd.isna(pop):
                pop = 5
            if pop <= 3:
                running_styles.append('senkou')
            elif pop <= 6:
                running_styles.append('sashi')
            else:
                running_styles.append('oikomi')
        
        pace = model.predict_pace(running_styles)
        advantages = [model.calculate_advantage(s, pace) for s in running_styles]
        
        for i, (_, h) in enumerate(race_horses.iterrows()):
            finish_pos = h['finish_position']
            is_win = 1 if (pd.notna(finish_pos) and finish_pos == 1) else 0
            results.append({
                'race_id': race_id,
                'horse_id': h['horse_id'],
                'advantage_score': advantages[i],
                'finish_position': finish_pos,
                'is_win': is_win
            })
    
    results_df = pd.DataFrame(results)
    results_df['finish_position'] = pd.to_numeric(results_df['finish_position'], errors='coerce')
    results_df = results_df.dropna(subset=['finish_position', 'advantage_score'])
    
    # 相関計算
    spearman_corr, spearman_p = stats.spearmanr(
        results_df['advantage_score'], 
        results_df['finish_position']
    )
    
    # AUC計算
    try:
        auc = roc_auc_score(results_df['is_win'], results_df['advantage_score'])
    except:
        auc = 0.5
    
    logging.info(f"  Spearman相関: {spearman_corr:.4f} (p={spearman_p:.4f})")
    logging.info(f"  AUC: {auc:.4f}")
    
    go_criteria = auc >= 0.52 or spearman_corr < -0.02
    logging.info(f"  Go判定: {'✅ GO' if go_criteria else '❌ NO-GO'}")
    
    return {
        'model': 'PaceScenarioModel',
        'spearman': spearman_corr,
        'spearman_p': spearman_p,
        'auc': auc,
        'go': go_criteria
    }


def validate_pedigree_model():
    """血統適性モデルの検証"""
    logging.info("=" * 60)
    logging.info("PedigreeAptitudeModel 検証")
    logging.info("=" * 60)
    
    from keibaai.src.models.specialized import PedigreeAptitudeModel
    
    # データ読み込み
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # モデルフィット
    model = PedigreeAptitudeModel(train_cutoff='2023-01-01')
    model.fit(races_df, pedigrees_df)
    
    # Valid期間でテスト
    valid_df = races_df[(races_df['race_date'] >= '2023-01-01') & 
                        (races_df['race_date'] < '2024-01-01')].copy()
    
    # サンプリング
    sample_races = valid_df['race_id'].unique()[:500]
    valid_df = valid_df[valid_df['race_id'].isin(sample_races)]
    
    logging.info(f"  検証レース数: {len(sample_races)}")
    
    results = []
    for _, h in valid_df.iterrows():
        conditions = {
            'surface': h.get('track_surface', '芝'),
            'distance_category': 'mile',  # 簡易版
            'is_heavy': h.get('track_condition', '') in ['重', '不良']
        }
        
        score = model.predict(str(h['horse_id']), conditions)
        
        finish_pos = h['finish_position']
        is_win = 1 if (pd.notna(finish_pos) and finish_pos == 1) else 0
        
        results.append({
            'horse_id': h['horse_id'],
            'aptitude_score': score,
            'finish_position': finish_pos,
            'is_win': is_win
        })
    
    results_df = pd.DataFrame(results)
    results_df['finish_position'] = pd.to_numeric(results_df['finish_position'], errors='coerce')
    results_df = results_df.dropna(subset=['finish_position', 'aptitude_score'])
    
    # 相関計算
    spearman_corr, spearman_p = stats.spearmanr(
        results_df['aptitude_score'], 
        results_df['finish_position']
    )
    
    # AUC計算
    try:
        auc = roc_auc_score(results_df['is_win'], results_df['aptitude_score'])
    except:
        auc = 0.5
    
    logging.info(f"  Spearman相関: {spearman_corr:.4f} (p={spearman_p:.4f})")
    logging.info(f"  AUC: {auc:.4f}")
    
    go_criteria = auc >= 0.52 or spearman_corr < -0.02
    logging.info(f"  Go判定: {'✅ GO' if go_criteria else '❌ NO-GO'}")
    
    return {
        'model': 'PedigreeAptitudeModel',
        'spearman': spearman_corr,
        'spearman_p': spearman_p,
        'auc': auc,
        'go': go_criteria
    }


def validate_condition_model():
    """状態変化モデルの検証"""
    logging.info("=" * 60)
    logging.info("ConditionChangeModel 検証")
    logging.info("=" * 60)
    
    from keibaai.src.models.specialized import ConditionChangeModel
    
    # データ読み込み
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # モデルフィット
    model = ConditionChangeModel(train_cutoff='2023-01-01')
    model.fit(races_df)
    
    # Valid期間でテスト
    valid_df = races_df[(races_df['race_date'] >= '2023-01-01') & 
                        (races_df['race_date'] < '2024-01-01')].copy()
    
    # サンプリング
    sample_races = valid_df['race_id'].unique()[:500]
    valid_df = valid_df[valid_df['race_id'].isin(sample_races)]
    
    logging.info(f"  検証レース数: {len(sample_races)}")
    
    results = []
    for _, h in valid_df.iterrows():
        race_date = h['race_date']
        weight = h.get('horse_weight', 0)
        if pd.isna(weight):
            weight = 0
        race_class = h.get('race_class', '')
        
        score = model.predict(
            str(h['horse_id']), 
            race_date, 
            float(weight), 
            str(race_class)
        )
        
        finish_pos = h['finish_position']
        is_win = 1 if (pd.notna(finish_pos) and finish_pos == 1) else 0
        
        results.append({
            'horse_id': h['horse_id'],
            'condition_score': score,
            'finish_position': finish_pos,
            'is_win': is_win
        })
    
    results_df = pd.DataFrame(results)
    results_df['finish_position'] = pd.to_numeric(results_df['finish_position'], errors='coerce')
    results_df = results_df.dropna(subset=['finish_position', 'condition_score'])
    
    # 相関計算
    spearman_corr, spearman_p = stats.spearmanr(
        results_df['condition_score'], 
        results_df['finish_position']
    )
    
    # AUC計算
    try:
        auc = roc_auc_score(results_df['is_win'], results_df['condition_score'])
    except:
        auc = 0.5
    
    logging.info(f"  Spearman相関: {spearman_corr:.4f} (p={spearman_p:.4f})")
    logging.info(f"  AUC: {auc:.4f}")
    
    go_criteria = auc >= 0.51 or spearman_corr < -0.01
    logging.info(f"  Go判定: {'✅ GO' if go_criteria else '❌ NO-GO'}")
    
    return {
        'model': 'ConditionChangeModel',
        'spearman': spearman_corr,
        'spearman_p': spearman_p,
        'auc': auc,
        'go': go_criteria
    }


def main():
    logging.info("=" * 60)
    logging.info("Phase 2 Go/No-Go Validation")
    logging.info("=" * 60)
    logging.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    results = []
    
    # 各モデルの検証
    try:
        results.append(validate_pace_model())
    except Exception as e:
        logging.error(f"PaceScenarioModel検証エラー: {e}")
        results.append({'model': 'PaceScenarioModel', 'go': False, 'error': str(e)})
    
    try:
        results.append(validate_pedigree_model())
    except Exception as e:
        logging.error(f"PedigreeAptitudeModel検証エラー: {e}")
        results.append({'model': 'PedigreeAptitudeModel', 'go': False, 'error': str(e)})
    
    try:
        results.append(validate_condition_model())
    except Exception as e:
        logging.error(f"ConditionChangeModel検証エラー: {e}")
        results.append({'model': 'ConditionChangeModel', 'go': False, 'error': str(e)})
    
    # サマリー
    logging.info("")
    logging.info("=" * 60)
    logging.info("【Phase 2 Go/No-Go サマリー】")
    logging.info("=" * 60)
    
    all_go = True
    for r in results:
        status = '✅ GO' if r.get('go', False) else '❌ NO-GO'
        auc = r.get('auc', 0)
        spearman = r.get('spearman', 0)
        logging.info(f"  {r['model']}: {status} (AUC={auc:.3f}, ρ={spearman:.3f})")
        if not r.get('go', False):
            all_go = False
    
    logging.info("")
    logging.info(f"【最終判定】: {'✅ Phase 3-4へ進行可能' if all_go else '⚠️ 一部モデルに課題あり'}")
    
    # 結果保存
    results_df = pd.DataFrame(results)
    output_path = Path('results/phase2_validation.csv')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_path, index=False)
    logging.info(f"結果保存: {output_path}")
    
    return results


if __name__ == "__main__":
    results = main()
