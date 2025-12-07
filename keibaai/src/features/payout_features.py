"""
配当傾向特徴量生成モジュール（μモデル v11用）

【目的】
returns.parquetを活用し、騎手/調教師/血統の「穴馬的中傾向」を特徴量化する。
高配当を出しやすい組み合わせを発見することがゴール。

【生成する特徴量】
1. jockey_avg_payout_lf: 騎手の過去勝利時の平均配当（累積）
2. jockey_upset_rate_lf: 騎手の穴馬（5番人気以下）での勝率（累積）
3. trainer_avg_payout_lf: 調教師の過去勝利時の平均配当（累積）
4. trainer_upset_rate_lf: 調教師の穴馬での勝率（累積）
5. sire_avg_payout_fixed: 父産駒の平均配当（Train期間固定）

【リーク防止】
- 累積計算は全て expanding().mean().shift(1)
- 血統統計は Train期間（2022年末まで）で固定
- returns.parquetの当該レース情報は使用しない
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path


class PayoutFeatureEngine:
    """配当傾向特徴量生成エンジン"""
    
    def __init__(self, train_cutoff: str = '2023-01-01'):
        """
        Args:
            train_cutoff: Train期間の終端日
        """
        self.logger = logging.getLogger(__name__)
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.sire_payout_stats = None  # Train期間で固定
        
    def load_returns(self, returns_path: str = 'keibaai/data/parsed/parquet/returns/returns.parquet') -> pd.DataFrame:
        """returns.parquetを読み込み"""
        ret = pd.read_parquet(returns_path)
        ret['race_id'] = ret['race_id'].astype(str)
        return ret
    
    def generate_features(
        self,
        df: pd.DataFrame,
        races_df: pd.DataFrame,
        returns_df: pd.DataFrame = None,
        pedigree_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        配当関連特徴量を生成
        
        Args:
            df: 特徴量を追加する対象DataFrame（出馬表ベース）
            races_df: レース結果DataFrame（finish_position, jockey_id, trainer_id, popularity含む）
            returns_df: returns.parquet（単勝配当含む）
            pedigree_df: 血統データ（sire_id取得用）
        
        Returns:
            特徴量を追加したDataFrame
        """
        self.logger.info("=" * 60)
        self.logger.info("配当傾向特徴量の生成開始（v11 リークフリー）")
        self.logger.info("=" * 60)
        
        if returns_df is None:
            returns_df = self.load_returns()
        
        # 型変換
        df = df.copy()
        df['race_id'] = df['race_id'].astype(str)
        races_df = races_df.copy()
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        for col in ['horse_id', 'jockey_id', 'trainer_id']:
            if col in df.columns:
                df[col] = df[col].astype(str)
            if col in races_df.columns:
                races_df[col] = races_df[col].astype(str)
        
        # 単勝配当をraces_dfに結合
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'payout', 'popularity']].copy()
        tansho = tansho.rename(columns={'payout': 'win_payout', 'popularity': 'winner_popularity'})
        
        # 結果データを準備
        perf = races_df[['race_id', 'race_date', 'horse_id', 'jockey_id', 'trainer_id', 'finish_position', 'popularity']].copy()
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce')
        perf['is_win'] = (perf['finish_position'] == 1).fillna(False).astype(int)
        perf['is_longshot'] = (perf['popularity'] >= 5).fillna(False)
        perf['is_longshot_win'] = (perf['is_win'] == 1) & perf['is_longshot']
        
        # 勝者のみに単勝配当を結合
        perf = perf.merge(tansho, on='race_id', how='left')
        perf.loc[perf['is_win'] != 1, 'win_payout'] = np.nan
        
        # ソート
        perf = perf.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 1. 騎手の配当傾向
        df = self._add_jockey_payout_features(df, perf)
        
        # 2. 調教師の配当傾向
        df = self._add_trainer_payout_features(df, perf)
        
        # 3. 血統の配当傾向（Train期間固定）
        if pedigree_df is not None:
            df = self._add_sire_payout_features(df, perf, pedigree_df)
        
        self.logger.info("=" * 60)
        self.logger.info("配当傾向特徴量の生成完了")
        self.logger.info("=" * 60)
        
        return df
    
    def _add_jockey_payout_features(
        self,
        df: pd.DataFrame,
        perf: pd.DataFrame
    ) -> pd.DataFrame:
        """
        【特徴量1,2】騎手の配当傾向（累積、リークフリー）
        """
        self.logger.info("  [1/3] 騎手の配当傾向（累積・shift済み）...")
        
        # 勝利時の配当平均（累積）
        perf['jockey_avg_payout_lf'] = perf.groupby('jockey_id')['win_payout'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 穴馬勝率: is_longshot のレースでの is_win の累積平均
        # 穴馬騎乗時のみ抽出してから計算
        longshot_perf = perf[perf['is_longshot']].copy()
        longshot_perf['jockey_upset_rate_temp'] = longshot_perf.groupby('jockey_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 元のperfにマージ
        perf = perf.merge(
            longshot_perf[['race_id', 'jockey_id', 'jockey_upset_rate_temp']].drop_duplicates(),
            on=['race_id', 'jockey_id'],
            how='left'
        )
        
        # 穴馬でない行にも値を伝播（最新の値を保持）
        perf = perf.sort_values(['jockey_id', 'race_date'])
        perf['jockey_upset_rate_lf'] = perf.groupby('jockey_id')['jockey_upset_rate_temp'].ffill()
        
        # dfにマージ
        merge_cols = ['race_id', 'jockey_id', 'horse_id', 'jockey_avg_payout_lf', 'jockey_upset_rate_lf']
        actual_cols = [c for c in merge_cols if c in perf.columns]
        df = df.merge(
            perf[actual_cols].drop_duplicates(subset=['race_id', 'jockey_id', 'horse_id']),
            on=['race_id', 'jockey_id', 'horse_id'],
            how='left'
        )
        
        self.logger.info("    生成完了: jockey_avg_payout_lf, jockey_upset_rate_lf")
        
        return df
    
    def _add_trainer_payout_features(
        self,
        df: pd.DataFrame,
        perf: pd.DataFrame
    ) -> pd.DataFrame:
        """
        【特徴量3,4】調教師の配当傾向（累積、リークフリー）
        """
        self.logger.info("  [2/3] 調教師の配当傾向（累積・shift済み）...")
        
        # 元のperfをソート（既にソートされているはずだが念のため）
        perf = perf.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 勝利時の配当平均（累積）
        perf['trainer_avg_payout_lf'] = perf.groupby('trainer_id')['win_payout'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 穴馬勝率
        longshot_perf = perf[perf['is_longshot']].copy()
        longshot_perf['trainer_upset_rate_temp'] = longshot_perf.groupby('trainer_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # マージして伝播
        perf = perf.merge(
            longshot_perf[['race_id', 'trainer_id', 'trainer_upset_rate_temp']].drop_duplicates(),
            on=['race_id', 'trainer_id'],
            how='left'
        )
        perf = perf.sort_values(['trainer_id', 'race_date'])
        perf['trainer_upset_rate_lf'] = perf.groupby('trainer_id')['trainer_upset_rate_temp'].ffill()
        
        # dfにマージ
        merge_cols = ['race_id', 'trainer_id', 'horse_id', 'trainer_avg_payout_lf', 'trainer_upset_rate_lf']
        actual_cols = [c for c in merge_cols if c in perf.columns]
        df = df.merge(
            perf[actual_cols].drop_duplicates(subset=['race_id', 'trainer_id', 'horse_id']),
            on=['race_id', 'trainer_id', 'horse_id'],
            how='left'
        )
        
        self.logger.info("    生成完了: trainer_avg_payout_lf, trainer_upset_rate_lf")
        
        return df
    
    def _add_sire_payout_features(
        self,
        df: pd.DataFrame,
        perf: pd.DataFrame,
        pedigree_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        【特徴量5】父産駒の配当傾向（Train期間固定）
        
        リーク防止: Train期間のデータのみで計算し、全期間に適用
        """
        self.logger.info("  [3/3] 父産駒の配当傾向（Train期間固定）...")
        
        # sire_idを取得
        sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates(subset=['horse_id'])
        sire_df.columns = ['horse_id', 'sire_id']
        sire_df['horse_id'] = sire_df['horse_id'].astype(str)
        sire_df['sire_id'] = sire_df['sire_id'].astype(str)
        
        # perfにsire_idを結合
        perf = perf.merge(sire_df, on='horse_id', how='left')
        
        # Train期間のデータのみ
        train_perf = perf[perf['race_date'] < self.train_cutoff].copy()
        
        if len(train_perf) == 0:
            self.logger.warning("  Train期間のデータがありません。血統配当傾向をスキップします。")
            df['sire_avg_payout_fixed'] = np.nan
            return df
        
        # 父別の平均配当と穴馬勝率
        sire_stats = train_perf.groupby('sire_id').agg({
            'win_payout': 'mean',
            'is_longshot_win': 'sum',
            'is_longshot': 'sum'
        }).reset_index()
        sire_stats.columns = ['sire_id', 'sire_avg_payout_fixed', 'sire_longshot_wins', 'sire_longshot_races']
        sire_stats['sire_upset_rate_fixed'] = sire_stats['sire_longshot_wins'] / sire_stats['sire_longshot_races'].replace(0, np.nan)
        
        # サンプル数が少ない場合はNaN
        sire_stats.loc[sire_stats['sire_longshot_races'] < 10, 'sire_upset_rate_fixed'] = np.nan
        
        # 保存
        self.sire_payout_stats = sire_stats
        
        # dfにsire_idを結合（まだなければ）
        if 'sire_id' not in df.columns:
            df = df.merge(sire_df, on='horse_id', how='left')
        
        # 統計をマージ
        df = df.merge(
            sire_stats[['sire_id', 'sire_avg_payout_fixed', 'sire_upset_rate_fixed']],
            on='sire_id',
            how='left'
        )
        
        self.logger.info(f"    生成完了: sire_avg_payout_fixed, sire_upset_rate_fixed ({len(sire_stats)}父)")
        
        return df


if __name__ == "__main__":
    """テスト実行"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # データ読み込み
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    returns = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    
    # サンプルデータ作成（2024年1月の出馬）
    races['race_date'] = pd.to_datetime(races['race_date'])
    sample = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2024-02-01')].copy()
    
    print(f"サンプルデータ: {len(sample):,}行")
    
    # 特徴量生成
    engine = PayoutFeatureEngine(train_cutoff='2023-01-01')
    result = engine.generate_features(sample, races, returns, pedigrees)
    
    # 結果確認
    payout_cols = [
        'jockey_avg_payout_lf', 'jockey_upset_rate_lf',
        'trainer_avg_payout_lf', 'trainer_upset_rate_lf',
        'sire_avg_payout_fixed', 'sire_upset_rate_fixed'
    ]
    print("\n生成された特徴量:")
    for col in payout_cols:
        if col in result.columns:
            non_null = result[col].notna().sum()
            if non_null > 0:
                print(f"  {col}: mean={result[col].mean():.3f}, std={result[col].std():.3f}, non-null={non_null}")
            else:
                print(f"  {col}: all null")
