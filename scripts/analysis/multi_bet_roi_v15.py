#!/usr/bin/env python3
"""
券種別ROI分析（V15モデル使用）

【分析対象券種】
- 単勝 (tansho): 1着
- 複勝 (fukusho): 3着以内
- 馬連 (umaren): 1-2着（順不同）
- 馬単 (umatan): 1-2着（順序あり）
- ワイド (wide): 3着以内の2頭
- 三連複 (sanrenpuku): 1-3着（順不同）
- 三連単 (sanrentan): 1-3着（順序あり）

【戦略】
- V15特徴量エンジニア（55特徴量）でリークフリー特徴量生成
- LightGBM LambdaRankでレース内順位予測
- Top-K選択でROI計算（確率計算なし）

【リーク・過学習対策】
- Train: ~2024-12-31, Test: 2025-01-01~2025-10-31
- 全ての累積統計はshift(1)適用済み
- 既存検証スクリプトと同一のハイパーパラメータ
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MultiBetROIAnalyzerV15:
    """V15モデルを使用した全券種ROI分析"""
    
    def __init__(self):
        self.data_dir = Path('keibaai/data/parsed/parquet')
        self.model = None
        self.feature_cols = None
        
    def load_data(self):
        """全データ読み込み"""
        logger.info("=" * 70)
        logger.info("データ読み込み")
        logger.info("=" * 70)
        
        # races.parquet
        self.races_df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        self.races_df['race_id'] = self.races_df['race_id'].astype(str)
        self.races_df['horse_id'] = self.races_df['horse_id'].astype(str)
        self.races_df['race_date'] = pd.to_datetime(self.races_df['race_date'])
        logger.info(f"  races: {len(self.races_df):,} records")
        
        # returns.parquet
        self.returns_df = pd.read_parquet(self.data_dir / 'returns/returns.parquet')
        self.returns_df['race_id'] = self.returns_df['race_id'].astype(str)
        logger.info(f"  returns: {len(self.returns_df):,} records")
        
        # pedigrees.parquet
        self.pedigrees_df = pd.read_parquet(self.data_dir / 'pedigrees/pedigrees.parquet')
        self.pedigrees_df['horse_id'] = self.pedigrees_df['horse_id'].astype(str)
        logger.info(f"  pedigrees: {len(self.pedigrees_df):,} records")
        
        # corners.parquet
        self.corners_df = pd.read_parquet(self.data_dir / 'corners/corner_positions.parquet')
        self.corners_df['race_id'] = self.corners_df['race_id'].astype(str)
        logger.info(f"  corners: {len(self.corners_df):,} records")
        
        # race_details.parquet
        self.race_details_df = pd.read_parquet(self.data_dir / 'race_details/race_details.parquet')
        self.race_details_df['race_id'] = self.race_details_df['race_id'].astype(str)
        logger.info(f"  race_details: {len(self.race_details_df):,} records")
        
        # horses.parquet
        self.horses_df = pd.read_parquet(self.data_dir / 'horses/horses.parquet')
        self.horses_df['horse_id'] = self.horses_df['horse_id'].astype(str)
        logger.info(f"  horses: {len(self.horses_df):,} records")
        
    def prepare_features(self):
        """V15特徴量生成"""
        logger.info("\n" + "=" * 70)
        logger.info("V15特徴量エンジニアリング")
        logger.info("=" * 70)
        
        from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
        
        # 有効なfinish_positionのみ使用
        races = self.races_df[self.races_df['finish_position'].notna()].copy()
        races['finish_position'] = races['finish_position'].astype(int)
        
        # 期間分割（既存検証スクリプトと同一）
        train_mask = races['race_date'] <= '2024-12-31'
        test_mask = (races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')
        
        train_df = races[train_mask].copy()
        test_df = races[test_mask].copy()
        
        logger.info(f"  Train: {len(train_df):,} records ({train_df['race_date'].min()} ~ {train_df['race_date'].max()})")
        logger.info(f"  Test: {len(test_df):,} records ({test_df['race_date'].min()} ~ {test_df['race_date'].max()})")
        
        # V15特徴量エンジニア
        fe = LeakFreeFeatureEngineerV15()
        
        # fit (Train期間のみ)
        logger.info("\n  V15 fit開始...")
        fe.fit(
            races_df=train_df,
            pedigrees_df=self.pedigrees_df,
            corners_df=self.corners_df,
            race_details_df=self.race_details_df,
            returns_df=None,
            horses_df=self.horses_df
        )
        
        # transform
        logger.info("  V15 transform開始...")
        train_features = fe.transform(train_df)
        test_features = fe.transform(test_df)
        
        self.feature_cols = fe.get_feature_columns()
        logger.info(f"  特徴量数: {len(self.feature_cols)}")
        
        return train_features, test_features
        
    def train_model(self, train_df):
        """LightGBM Binary Classification学習（既存検証スクリプトと同一設定）"""
        logger.info("\n" + "=" * 70)
        logger.info("モデル学習 (LightGBM Binary Classification)")
        logger.info("=" * 70)
        
        # 特徴量準備
        X_train = train_df[self.feature_cols].fillna(0)
        y_train = (train_df['finish_position'] == 1).astype(int)
        
        # ハイパーパラメータ（既存検証スクリプトと同一）
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'learning_rate': 0.03,
            'num_leaves': 20,
            'max_depth': 3,
            'min_child_samples': 100,
            'reg_alpha': 3.0,
            'reg_lambda': 5.0,
            'bagging_fraction': 0.7,
            'bagging_freq': 3,
            'feature_fraction': 0.7,
        }
        
        logger.info(f"  ハイパーパラメータ: max_depth={params['max_depth']}, num_leaves={params['num_leaves']}")
        
        # 学習
        train_ds = lgb.Dataset(X_train, y_train)
        self.model = lgb.train(params, train_ds, num_boost_round=200)
        
        logger.info(f"  学習完了")
        
    def predict_and_evaluate(self, df, period_name):
        """予測とROI計算"""
        logger.info(f"\n" + "=" * 70)
        logger.info(f"【{period_name}期間】ROI分析")
        logger.info("=" * 70)
        
        # 予測
        X = df[self.feature_cols].fillna(0)
        df = df.copy()
        df['score'] = self.model.predict(X)
        df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # 予測精度（Top1/2/3）
        top1_hit = (df[df['rank_pred'] == 1]['finish_position'] == 1).mean()
        top2_in_top2 = df[df['rank_pred'] <= 2].groupby('race_id').apply(
            lambda g: (g['finish_position'] <= 2).sum() == 2
        ).mean() if len(df) > 0 else 0
        top3_in_top3 = df[df['rank_pred'] <= 3].groupby('race_id').apply(
            lambda g: (g['finish_position'] <= 3).sum() == 3
        ).mean() if len(df) > 0 else 0
        
        logger.info(f"  予測精度:")
        logger.info(f"    Top1→1着: {top1_hit*100:.1f}%")
        logger.info(f"    Top2→1-2着: {top2_in_top2*100:.1f}%")
        logger.info(f"    Top3→1-3着: {top3_in_top3*100:.1f}%")
        
        # 各券種のROI計算
        results = []
        race_ids = df['race_id'].unique()
        returns_period = self.returns_df[self.returns_df['race_id'].isin(race_ids)]
        
        results.append(self._calc_tansho_roi(df, returns_period))
        results.append(self._calc_fukusho_roi(df, returns_period))
        results.append(self._calc_umaren_roi(df, returns_period))
        results.append(self._calc_umatan_roi(df, returns_period))
        results.append(self._calc_wide_roi(df, returns_period))
        results.append(self._calc_sanrenpuku_roi(df, returns_period))
        results.append(self._calc_sanrentan_roi(df, returns_period))
        
        # サマリー
        logger.info(f"\n  券種別ROI:")
        logger.info(f"    {'券種':<12} | {'投資数':>8} | {'的中数':>8} | {'的中率':>8} | {'ROI':>10}")
        logger.info("    " + "-" * 55)
        
        for r in sorted(results, key=lambda x: x['roi'], reverse=True):
            hit_rate = r['hits'] / r['bets'] * 100 if r['bets'] > 0 else 0
            status = "✅" if r['roi'] >= 100 else "⚠️"
            logger.info(f"    {status} {r['bet_type']:<10} | {r['bets']:>8,} | {r['hits']:>8,} | {hit_rate:>7.1f}% | {r['roi']:>9.1f}%")
        
        return results
        
    def _calc_tansho_roi(self, df, returns):
        """単勝ROI"""
        tansho = returns[returns['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
        tansho.columns = ['race_id', 'winner_hn', 'payout']
        
        top1 = df[df['rank_pred'] == 1][['race_id', 'horse_number']].copy()
        merged = top1.merge(tansho, left_on=['race_id', 'horse_number'], right_on=['race_id', 'winner_hn'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(top1)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'tansho', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_fukusho_roi(self, df, returns):
        """複勝ROI"""
        fukusho = returns[returns['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
        fukusho.columns = ['race_id', 'place_hn', 'payout']
        
        top1 = df[df['rank_pred'] == 1][['race_id', 'horse_number']].copy()
        merged = top1.merge(fukusho, left_on=['race_id', 'horse_number'], right_on=['race_id', 'place_hn'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(top1)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'fukusho', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_umaren_roi(self, df, returns):
        """馬連ROI"""
        umaren = returns[returns['bet_type'] == 'umaren'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        top2 = df[df['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred']].copy()
        pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        if len(pivot.columns) < 3:
            return {'bet_type': 'umaren', 'bets': 0, 'hits': 0, 'payout': 0, 'roi': 0}
        pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # 順不同のためソート
        pivot['p_min'] = pivot[['pred_1', 'pred_2']].min(axis=1)
        pivot['p_max'] = pivot[['pred_1', 'pred_2']].max(axis=1)
        umaren['h_min'] = umaren[['horse_1', 'horse_2']].min(axis=1)
        umaren['h_max'] = umaren[['horse_1', 'horse_2']].max(axis=1)
        
        merged = pivot.merge(umaren, left_on=['race_id', 'p_min', 'p_max'], right_on=['race_id', 'h_min', 'h_max'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(pivot)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'umaren', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_umatan_roi(self, df, returns):
        """馬単ROI"""
        umatan = returns[returns['bet_type'] == 'umatan'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        top2 = df[df['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred']].copy()
        pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        if len(pivot.columns) < 3:
            return {'bet_type': 'umatan', 'bets': 0, 'hits': 0, 'payout': 0, 'roi': 0}
        pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # 順序あり
        merged = pivot.merge(umatan, left_on=['race_id', 'pred_1', 'pred_2'], right_on=['race_id', 'horse_1', 'horse_2'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(pivot)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'umatan', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_wide_roi(self, df, returns):
        """ワイドROI"""
        wide = returns[returns['bet_type'] == 'wide'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
        
        top2 = df[df['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred']].copy()
        pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        if len(pivot.columns) < 3:
            return {'bet_type': 'wide', 'bets': 0, 'hits': 0, 'payout': 0, 'roi': 0}
        pivot.columns = ['race_id', 'pred_1', 'pred_2']
        
        # 順不同
        pivot['p_min'] = pivot[['pred_1', 'pred_2']].min(axis=1)
        pivot['p_max'] = pivot[['pred_1', 'pred_2']].max(axis=1)
        wide['h_min'] = wide[['horse_1', 'horse_2']].min(axis=1)
        wide['h_max'] = wide[['horse_1', 'horse_2']].max(axis=1)
        
        merged = pivot.merge(wide, left_on=['race_id', 'p_min', 'p_max'], right_on=['race_id', 'h_min', 'h_max'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(pivot)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'wide', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_sanrenpuku_roi(self, df, returns):
        """三連複ROI"""
        sanrenpuku = returns[returns['bet_type'] == 'sanrenpuku'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
        
        top3 = df[df['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred']].copy()
        pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        if len(pivot.columns) < 4:
            return {'bet_type': 'sanrenpuku', 'bets': 0, 'hits': 0, 'payout': 0, 'roi': 0}
        pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
        
        # 順不同のためソート
        def sort_triple(row):
            return tuple(sorted([row['pred_1'], row['pred_2'], row['pred_3']]))
        pivot['key'] = pivot.apply(sort_triple, axis=1)
        
        def sort_triple_ret(row):
            return tuple(sorted([row['horse_1'], row['horse_2'], row['horse_3']]))
        sanrenpuku['key'] = sanrenpuku.apply(sort_triple_ret, axis=1)
        
        merged = pivot.merge(sanrenpuku[['race_id', 'key', 'payout']], on=['race_id', 'key'], how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(pivot)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'sanrenpuku', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def _calc_sanrentan_roi(self, df, returns):
        """三連単ROI"""
        sanrentan = returns[returns['bet_type'] == 'sanrentan'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
        
        top3 = df[df['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred']].copy()
        pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        if len(pivot.columns) < 4:
            return {'bet_type': 'sanrentan', 'bets': 0, 'hits': 0, 'payout': 0, 'roi': 0}
        pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
        
        # 順序あり
        merged = pivot.merge(sanrentan, 
                            left_on=['race_id', 'pred_1', 'pred_2', 'pred_3'], 
                            right_on=['race_id', 'horse_1', 'horse_2', 'horse_3'], 
                            how='left')
        
        hits = merged['payout'].notna().sum()
        payout = merged['payout'].fillna(0).sum()
        bets = len(pivot)
        roi = payout / (bets * 100) * 100 if bets > 0 else 0
        
        return {'bet_type': 'sanrentan', 'bets': bets, 'hits': hits, 'payout': payout, 'roi': roi}
        
    def analyze_model_suitability(self, valid_results, test_results):
        """モデル適合性分析"""
        logger.info("\n" + "=" * 70)
        logger.info("モデル適合性評価")
        logger.info("=" * 70)
        
        # Valid vs Test比較
        logger.info("\n【Valid vs Test ROI比較】")
        logger.info(f"  {'券種':<12} | {'Valid ROI':>10} | {'Test ROI':>10} | {'差分':>8}")
        logger.info("  " + "-" * 50)
        
        for v, t in zip(sorted(valid_results, key=lambda x: x['bet_type']),
                       sorted(test_results, key=lambda x: x['bet_type'])):
            diff = t['roi'] - v['roi']
            status = "📈" if diff > 0 else "📉" if diff < -10 else "➡️"
            logger.info(f"  {status} {v['bet_type']:<10} | {v['roi']:>9.1f}% | {t['roi']:>9.1f}% | {diff:>+7.1f}%")
        
        # 各券種の適合性判定
        logger.info("\n【券種別モデル適合性判定】")
        
        for r in sorted(test_results, key=lambda x: x['roi'], reverse=True):
            bet_type = r['bet_type']
            roi = r['roi']
            
            if bet_type == 'tansho':
                suitability = "最適" if roi > 85 else "適合"
                reason = "V15は1着予測に最適化"
            elif bet_type == 'fukusho':
                suitability = "適合" if roi > 90 else "やや低"
                reason = "複勝は3着以内なので的中率高"
            elif bet_type in ['umaren', 'wide']:
                suitability = "要検討" if roi < 80 else "適合"
                reason = "2頭の組み合わせ精度に依存"
            elif bet_type == 'umatan':
                suitability = "難" if roi < 60 else "要検討"
                reason = "1-2着の順序精度が必要"
            elif bet_type == 'sanrenpuku':
                suitability = "難" if roi < 50 else "要検討"
                reason = "3頭の組み合わせ精度が必要"
            elif bet_type == 'sanrentan':
                suitability = "非常に難" if roi < 30 else "難"
                reason = "1-2-3着の順序精度が必要"
            else:
                suitability = "不明"
                reason = ""
            
            logger.info(f"  {bet_type:<12}: 【{suitability}】 - {reason} (ROI: {roi:.1f}%)")
        
        # 過学習チェック
        logger.info("\n【過学習チェック】")
        for v, t in zip(sorted(valid_results, key=lambda x: x['bet_type']),
                       sorted(test_results, key=lambda x: x['bet_type'])):
            gap = abs(v['roi'] - t['roi'])
            if gap > 30:
                logger.info(f"  ⚠️ {v['bet_type']}: Gap {gap:.1f}% (過学習リスク)")
            else:
                logger.info(f"  ✅ {v['bet_type']}: Gap {gap:.1f}% (許容範囲)")
        
    def run(self):
        """メイン実行"""
        logger.info("=" * 70)
        logger.info("券種別ROI分析（V15モデル）")
        logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
        
        # データ読み込み
        self.load_data()
        
        # 特徴量生成
        train_df, test_df = self.prepare_features()
        
        # モデル学習
        self.train_model(train_df)
        
        # Train期間評価（参考）
        train_results = self.predict_and_evaluate(train_df, "Train")
        
        # Test期間評価
        test_results = self.predict_and_evaluate(test_df, "Test")
        
        # 結果サマリー
        logger.info("\n" + "=" * 70)
        logger.info("過学習チェック (Train vs Test)")
        logger.info("=" * 70)
        
        for tr, te in zip(sorted(train_results, key=lambda x: x['bet_type']),
                         sorted(test_results, key=lambda x: x['bet_type'])):
            gap = tr['roi'] - te['roi']
            status = "⚠️" if gap > 50 else "✅"
            logger.info(f"  {status} {tr['bet_type']:<12}: Train={tr['roi']:.1f}%, Test={te['roi']:.1f}%, Gap={gap:.1f}%")
        
        logger.info("\n" + "=" * 70)
        logger.info("分析完了")
        logger.info("=" * 70)
        
        return {'train': train_results, 'test': test_results}


if __name__ == "__main__":
    analyzer = MultiBetROIAnalyzerV15()
    results = analyzer.run()
