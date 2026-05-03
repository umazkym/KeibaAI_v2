#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Race Context Analyzer v3 — 過去レースの展開文脈を分析するモジュール

全レースデータ(parquet)を活用した高精度な展開分析:

1. ペース判定: 先頭3頭のRPCIで判定 + 距離帯別Z-score正規化
2. 脚質4分類: 逃げ/先行/中団/差追込（頭数比率ベース）
3. ポジションバイアス: 同日同コース午前/午後分割 + オッズ乖離ベース
4. 馬場速度: クラス補正 + 距離正規化 + 同日集約
5. 相手強度: 同馬場面+距離帯フィルタの過去実績ベース
6. 展開補正: 4分類脚質 × ペースZ × 距離正規化着差の傾斜関数
7. 上がり3F順位: 同レース内での末脚ランキング
8. 構造化注釈: テキストパース不要の数値/ラベル出力

Usage:
    analyzer = RaceContextAnalyzer()
    context = analyzer.analyze_race('202509050411')
    annotations = analyzer.annotate_horse_in_race('202509050411', horse_position=2, horse_corner4=3)
"""

import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
PKL_DIR = PROJECT_ROOT / "keibaai" / "data" / "processed"

# ---------------------------------------------------------------------------
# 距離帯定義
# ---------------------------------------------------------------------------
DIST_BANDS = {
    'sprint': (0, 1400),      # スプリント
    'mile': (1401, 2000),     # マイル〜中距離
    'long': (2001, 9999),     # 長距離
}

# クラスのordinal値（馬場速度のクラス補正用）
CLASS_ORDINAL = {
    '新馬': 1, '未勝利': 2,
    '500': 3, '1勝': 3, '1勝クラス': 3,
    '1000': 4, '2勝': 4, '2勝クラス': 4,
    '1600': 5, '3勝': 5, '3勝クラス': 5,
    'OP': 6, 'オープン': 6, 'L': 6, 'リステッド': 6,
    'G3': 7, 'G2': 8, 'G1': 9,
}


def _dist_band(distance_m: int) -> str:
    """距離帯ラベルを返す"""
    for band, (lo, hi) in DIST_BANDS.items():
        if lo <= distance_m <= hi:
            return band
    return 'mile'


def _class_ordinal(race_class: str) -> int:
    """クラス文字列からordinal値を返す"""
    if not race_class or pd.isna(race_class):
        return 3  # デフォルト: 1勝クラス相当
    for key, val in CLASS_ORDINAL.items():
        if key in str(race_class):
            return val
    return 3


def _race_number_from_id(race_id: str) -> int:
    """race_idからレース番号を抽出 (末尾2桁)"""
    try:
        return int(race_id[-2:])
    except (ValueError, IndexError):
        return 6  # デフォルト


class RaceContextAnalyzer:
    """過去レースの展開文脈を分析するクラス (v3)"""

    def __init__(self, parquet_path: str = None):
        self.parquet_path = parquet_path or str(PARQUET_PATH)
        self._df = None
        self._race_cache = {}          # race_id -> DataFrame
        self._global_stats = None
        self._venue_adj = None
        self._class_table = None       # (venue, dist, surf, cond) -> sec_per_level
        self._class_fallback = None    # (dist, surf) -> sec_per_level
        self._rpci_stats = None        # (surface, dist_band) -> (mean, std)
        self._daily_bias_cache = {}    # (date, venue, surface, half) -> dict
        self._daily_speed_cache = {}   # cache key -> dict
        self._horse_history_cache = {} # horse_id -> avg_t_index

    # ------------------------------------------------------------------
    # データ読み込み
    # ------------------------------------------------------------------

    def _load_parquet(self):
        """parquet読み込み (遅延ロード)"""
        if self._df is not None:
            return
        logger.info(f"Loading parquet: {self.parquet_path}")
        self._df = pd.read_parquet(self.parquet_path, columns=[
            'race_id', 'race_date', 'distance_m', 'track_surface',
            'track_condition', 'venue', 'finish_position',
            'finish_time_seconds', 'last_3f_time',
            'passing_order_1', 'passing_order_2',
            'passing_order_3', 'passing_order_4',
            'pace_index', 'bracket_number', 'horse_number',
            'horse_id', 'horse_name', 'final_corner_to_finish',
            'horse_weight', 'popularity', 'win_odds',
            'race_name', 'race_class'
        ])
        # 型変換
        self._df['race_id'] = self._df['race_id'].astype(str)
        self._df['horse_id'] = self._df['horse_id'].astype(str)
        for col in ['finish_position', 'finish_time_seconds', 'last_3f_time',
                     'passing_order_4', 'passing_order_1', 'pace_index',
                     'bracket_number', 'final_corner_to_finish',
                     'popularity', 'win_odds']:
            self._df[col] = pd.to_numeric(self._df[col], errors='coerce')

        # 距離帯カラム
        self._df['dist_band'] = self._df['distance_m'].apply(
            lambda d: _dist_band(int(d)) if pd.notna(d) else 'mile')

        # 頭数カラム
        race_counts = self._df.dropna(subset=['finish_position']).groupby('race_id').size()
        self._df['num_runners'] = self._df['race_id'].map(race_counts)

        # 相対位置カラム (4角通過順/頭数)
        self._df['relative_pos_4'] = (
            self._df['passing_order_4'] / self._df['num_runners']
        )

        # レース番号（午前/午後判定用）
        self._df['race_number'] = self._df['race_id'].apply(_race_number_from_id)

        # クラスordinal
        self._df['class_ordinal'] = self._df['race_class'].apply(_class_ordinal)

        logger.info(f"Loaded {len(self._df)} rows from parquet")

        # RPCI統計を事前計算（先頭3頭ベース）
        self._precompute_rpci_stats()

    def _load_pkl_stats(self):
        """PKL統計を読み込み（T指数計算用 + クラス補正テーブル）"""
        if self._global_stats is None:
            gpath = PKL_DIR / "global_stats_lookup.pkl"
            if gpath.exists():
                with open(gpath, 'rb') as f:
                    self._global_stats = pickle.load(f)
            else:
                self._global_stats = {}

        if self._venue_adj is None:
            vpath = PKL_DIR / "venue_adjustment.pkl"
            if vpath.exists():
                with open(vpath, 'rb') as f:
                    self._venue_adj = pickle.load(f)
            else:
                self._venue_adj = {}

        if self._class_table is None:
            cpath = PKL_DIR / "class_time_table.pkl"
            if cpath.exists():
                with open(cpath, 'rb') as f:
                    ct_data = pickle.load(f)
                self._class_table = {}
                for k, v in ct_data.get('class_table', {}).items():
                    self._class_table[k] = v.get('sec_per_level', 0.0)
                self._class_fallback = ct_data.get('global_fallback', {})
                logger.info(f"Loaded class time table: {len(self._class_table)} entries")
            else:
                self._class_table = {}
                self._class_fallback = {}
                logger.info("No class_time_table.pkl found, using default")

    def _precompute_rpci_stats(self):
        """距離帯×馬場別のRPCI統計を先頭3頭ベースで事前計算"""
        if self._rpci_stats is not None:
            return
        self._rpci_stats = {}
        valid = self._df.dropna(subset=['pace_index', 'finish_position', 'passing_order_4']).copy()

        # 先頭3頭（4角通過順上位3）のRPCIから中央値を取る
        valid['rank_in_race'] = valid.groupby('race_id')['passing_order_4'].rank(method='first')
        front3 = valid[valid['rank_in_race'] <= 3]
        race_pace = front3.groupby('race_id')['pace_index'].median().reset_index()
        race_pace.columns = ['race_id', 'front3_rpci']

        # 各レースの情報を結合
        race_info = valid.drop_duplicates('race_id')[['race_id', 'track_surface', 'dist_band']]
        race_pace = race_pace.merge(race_info, on='race_id', how='left')

        for (surf, band), grp in race_pace.groupby(['track_surface', 'dist_band']):
            vals = grp['front3_rpci'].dropna()
            if len(vals) < 10:
                continue
            self._rpci_stats[(surf, band)] = {
                'mean': vals.mean(),
                'std': max(vals.std(), 1.0),
                'median': vals.median(),
                'q25': vals.quantile(0.25),
                'q75': vals.quantile(0.75),
            }
        logger.info(f"RPCI stats (front-3 based) computed for {len(self._rpci_stats)} groups")

    # ------------------------------------------------------------------
    # 基本アクセサ
    # ------------------------------------------------------------------

    def get_race_runners(self, race_id: str) -> pd.DataFrame:
        """指定race_idの全出走馬データを取得"""
        self._load_parquet()
        if race_id in self._race_cache:
            return self._race_cache[race_id]
        runners = self._df[self._df['race_id'] == race_id].copy()
        self._race_cache[race_id] = runners
        return runners

    # ------------------------------------------------------------------
    # 1. ペース判定 (先頭3頭RPCI + 距離帯別Z-score)
    # ------------------------------------------------------------------

    def _analyze_pace(self, runners: pd.DataFrame) -> Dict:
        """先頭3頭のRPCIでレースペースを判定"""
        valid = runners.dropna(subset=['pace_index', 'passing_order_4'])
        if len(valid) == 0:
            return {'pace_type': '', 'pace_rpci_front3': 50.0,
                    'pace_z_score': 0.0, 'pace_deviation': ''}

        # ★ 先頭3頭（4角通過順上位3）のRPCIの中央値 = レースペース
        valid_sorted = valid.nsmallest(3, 'passing_order_4')
        race_rpci = valid_sorted['pace_index'].median()

        result = {'pace_rpci_front3': round(float(race_rpci), 1)}

        # 距離帯の統計で正規化
        row0 = runners.iloc[0]
        surf = row0.get('track_surface', '芝')
        band = row0.get('dist_band', 'mile')
        stats = self._rpci_stats.get((surf, band), {})
        rpci_mean = stats.get('mean', 50.0)
        rpci_std = stats.get('std', 3.0)

        z = (race_rpci - rpci_mean) / rpci_std
        result['pace_z_score'] = round(float(z), 2)

        # Z-scoreで判定（0.7σ以上で有意）
        if z > 0.7:
            result['pace_type'] = 'スロー'
            result['pace_deviation'] = '前半遅い'
        elif z < -0.7:
            result['pace_type'] = 'ハイ'
            result['pace_deviation'] = '前半速い'
        elif z > 0.3:
            result['pace_type'] = 'ややスロー'
            result['pace_deviation'] = ''
        elif z < -0.3:
            result['pace_type'] = 'ややハイ'
            result['pace_deviation'] = ''
        else:
            result['pace_type'] = 'ミドル'
            result['pace_deviation'] = ''

        return result

    # ------------------------------------------------------------------
    # 2. 脚質4分類 (頭数比率ベース)
    # ------------------------------------------------------------------

    @staticmethod
    def classify_running_style(corner4: float, num_runners: int) -> str:
        """頭数比率ベースの脚質4分類"""
        if pd.isna(corner4) or pd.isna(num_runners) or num_runners < 3:
            return '不明'
        ratio = corner4 / num_runners
        if ratio <= 0.15:
            return '逃げ'
        elif ratio <= 0.40:
            return '先行'
        elif ratio <= 0.67:
            return '中団'
        else:
            return '差追込'

    @staticmethod
    def is_front_runner(corner4: float, num_runners: int) -> Optional[bool]:
        """先行馬かどうかを頭数比率で判定"""
        if pd.isna(corner4) or pd.isna(num_runners) or num_runners < 3:
            return None
        return (corner4 / num_runners) <= 0.40

    @staticmethod
    def _style_sensitivity(style: str) -> float:
        """脚質別のペース補正感度（バックテスト検証済み）
        
        検証結果(N=3000):
        - 中団: ハイ/スロー差分+50% → 展開影響度が最大
        - 先行: 推定感度0.06 → 影響は実は小さい
        - 逃げ: N不足 → 理論値1.5を維持
        - 差追込: 方向は正しい → -1.0維持
        """
        return {
            '逃げ': 1.5,     # 理論値維持（ペースメーカーとして最も影響大）
            '先行': 0.7,     # 1.0→0.7 バックテストで推定感度が低かった
            '中団': 1.0,     # 0.5→1.0 バックテストで最大の展開影響度(差分+50%)
            '差追込': -1.0,  # 維持（方向確認済み）
            '不明': 0.0,
        }.get(style, 0.0)

    # ------------------------------------------------------------------
    # 3. ポジションバイアス (午前/午後分割 + オッズ乖離)
    # ------------------------------------------------------------------

    def _get_daily_position_bias(self, race_date, venue, surface,
                                  race_number: int = 6) -> Dict:
        """同日・同会場・同馬場面のバイアス（午前/午後分割）"""
        half = 'PM' if race_number >= 7 else 'AM'
        cache_key = (str(race_date)[:10], venue, surface, half)
        if cache_key in self._daily_bias_cache:
            return self._daily_bias_cache[cache_key]

        self._load_parquet()

        # 同日・同会場・同馬場面のデータを午前/午後で分割
        mask = (
            (self._df['race_date'] == race_date) &
            (self._df['venue'] == venue) &
            (self._df['track_surface'] == surface) &
            (self._df['finish_position'].notna()) &
            (self._df['passing_order_4'].notna()) &
            (self._df['num_runners'] >= 5)
        )
        if half == 'PM':
            mask = mask & (self._df['race_number'] >= 7)
        else:
            mask = mask & (self._df['race_number'] < 7)

        day_data = self._df[mask].copy()

        if len(day_data) < 20:
            # 午前/午後のデータが少なければ終日データにフォールバック
            mask_full = (
                (self._df['race_date'] == race_date) &
                (self._df['venue'] == venue) &
                (self._df['track_surface'] == surface) &
                (self._df['finish_position'].notna()) &
                (self._df['passing_order_4'].notna()) &
                (self._df['num_runners'] >= 5)
            )
            day_data = self._df[mask_full].copy()

        if len(day_data) < 20:
            result = {'bias_corr': 0.0, 'bias_label': '', 'bias_confidence': 'low',
                      'front_top3_rate': 0.5, 'sample_races': 0,
                      'odds_bias': 0.0}
            self._daily_bias_cache[cache_key] = result
            return result

        # --- 方法1: 相関ベース ---
        day_data['rel_finish'] = day_data['finish_position'] / day_data['num_runners']
        corr = day_data['relative_pos_4'].corr(day_data['rel_finish'])
        corr = float(corr) if not np.isnan(corr) else 0.0

        # --- 方法2: オッズ乖離ベース (能力差を除去) ---
        odds_bias = 0.0
        odds_data = day_data.dropna(subset=['win_odds', 'popularity'])
        if len(odds_data) >= 15:
            # オッズ期待着順 = 人気順をそのまま使用（簡易近似）
            odds_data = odds_data.copy()
            odds_data['expected_pos'] = odds_data.groupby('race_id')['popularity'].rank()
            odds_data['surprise'] = odds_data['expected_pos'] - odds_data['finish_position']
            # surprise > 0 = 人気より良い成績（上振れ）

            front = odds_data[odds_data['relative_pos_4'] <= 0.40]
            back = odds_data[odds_data['relative_pos_4'] > 0.40]
            if len(front) >= 5 and len(back) >= 5:
                # 先行馬の上振れ平均 - 差し馬の上振れ平均 → 正=前有利
                odds_bias = float(front['surprise'].mean() - back['surprise'].mean())

        # 先行馬Top3入り率
        front_mask = day_data['relative_pos_4'] <= 0.33
        front_group = day_data[front_mask]
        front_top3_rate = float((front_group['finish_position'] <= 3).mean()) if len(front_group) > 0 else 0.0

        n_races = day_data['race_id'].nunique()
        confidence = 'high' if n_races >= 5 else ('mid' if n_races >= 3 else 'low')

        # 総合判定（相関 + オッズ乖離のハイブリッド）
        combined_score = corr * 0.5 + odds_bias * 0.1  # スケール調整
        if combined_score > 0.15 and confidence != 'low':
            label = '前有利'
        elif combined_score < -0.10 and confidence != 'low':
            label = '差有利'
        else:
            label = ''

        result = {
            'bias_corr': round(corr, 3),
            'bias_label': label,
            'bias_confidence': confidence,
            'front_top3_rate': round(front_top3_rate, 3),
            'sample_races': n_races,
            'odds_bias': round(odds_bias, 3),
        }
        self._daily_bias_cache[cache_key] = result
        return result

    # ------------------------------------------------------------------
    # 4. 馬場速度 (クラス補正 + 距離正規化 + 同日集約)
    # ------------------------------------------------------------------

    def _get_daily_track_speed(self, race_date, venue, distance, surface,
                                condition, race_class=None) -> Dict:
        """クラス補正を入れた馬場速度の算出"""
        self._load_parquet()
        self._load_pkl_stats()

        target_ordinal = _class_ordinal(race_class) if race_class else 3

        # 同日・同会場・同馬場面・同距離のTop3データを集約
        mask = (
            (self._df['race_date'] == race_date) &
            (self._df['venue'] == venue) &
            (self._df['distance_m'] == distance) &
            (self._df['track_surface'] == surface) &
            (self._df['finish_position'].notna()) &
            (self._df['finish_time_seconds'].notna()) &
            (self._df['finish_position'] <= 3)
        )
        day_top3 = self._df[mask].copy()

        if len(day_top3) < 3:
            return {'track_speed_per1000': 0.0, 'track_speed_label': '',
                    'track_speed_raw': 0.0, 'confidence': 'low'}

        # ★ クラス補正: 実データベースのテーブルを使用
        cond_map = {"良": "良", "稍": "稍重", "重": "重", "不": "不良"}
        full_cond = cond_map.get(str(condition)[:1], condition) if condition else '良'

        # テーブルから秒/レベルを取得（フォールバック階層あり）
        ct_key = (venue, int(distance), surface, full_cond)
        sec_per_level = self._class_table.get(ct_key)
        if sec_per_level is None:
            # フォールバック: 距離×馬場面
            fb_key = (int(distance), surface)
            sec_per_level = self._class_fallback.get(fb_key)
        if sec_per_level is None:
            # 最終フォールバック: 固定値
            sec_per_level = 0.6 * (distance / 1000.0)

        day_top3['class_adj'] = (day_top3['class_ordinal'] - target_ordinal) * sec_per_level
        day_top3['adj_time'] = day_top3['finish_time_seconds'] + day_top3['class_adj']

        avg_time = day_top3['adj_time'].median()

        # グローバル基準タイム
        cond_map = {"良": "良", "稍": "稍重", "重": "重", "不": "不良"}
        full_cond = cond_map.get(str(condition)[:1], condition) if condition else '良'

        global_key = (int(distance), surface, full_cond)
        g_stats = self._global_stats.get(global_key)
        if not g_stats:
            global_key_nocond = (int(distance), surface, '良')
            g_stats = self._global_stats.get(global_key_nocond)

        if not g_stats:
            return {'track_speed_per1000': 0.0, 'track_speed_label': '',
                    'track_speed_raw': 0.0, 'confidence': 'low'}

        raw_diff = avg_time - g_stats['time_mean']
        per1000 = raw_diff / (distance / 1000.0)

        if per1000 < -0.5:
            label = '高速馬場'
        elif per1000 > 0.5:
            label = '低速馬場'
        elif per1000 < -0.2:
            label = 'やや高速'
        elif per1000 > 0.2:
            label = 'やや低速'
        else:
            label = '標準馬場'

        n_races = day_top3['race_id'].nunique()
        confidence = 'high' if n_races >= 3 else ('mid' if n_races >= 2 else 'low')

        return {
            'track_speed_per1000': round(float(per1000), 3),
            'track_speed_label': label,
            'track_speed_raw': round(float(raw_diff), 2),
            'confidence': confidence,
        }

    # ------------------------------------------------------------------
    # 5. 相手強度 (同馬場面+距離帯フィルタの過去実績ベース)
    # ------------------------------------------------------------------

    def _compute_horse_historical_t(self, horse_id: str, before_date=None,
                                     target_surface: str = None,
                                     target_dist_band: str = None) -> float:
        """指定馬の過去走T指数平均（同馬場面・近距離帯を優先）"""
        cache_key = (horse_id, str(before_date)[:10] if before_date else '',
                     target_surface or '', target_dist_band or '')
        if cache_key in self._horse_history_cache:
            return self._horse_history_cache[cache_key]

        self._load_parquet()
        self._load_pkl_stats()

        mask = (
            (self._df['horse_id'] == horse_id) &
            (self._df['finish_position'].notna()) &
            (self._df['finish_time_seconds'].notna())
        )
        horse_races = self._df[mask].copy()

        if before_date is not None:
            horse_races = horse_races[horse_races['race_date'] < before_date]

        # ★ 同馬場面でフィルタ（芝↔ダートの混在を避ける）
        if target_surface and len(horse_races) > 0:
            same_surface = horse_races[horse_races['track_surface'] == target_surface]
            if len(same_surface) >= 2:
                horse_races = same_surface

        # ★ 同距離帯でフィルタ（距離適性の考慮）
        if target_dist_band and len(horse_races) > 0:
            same_band = horse_races[horse_races['dist_band'] == target_dist_band]
            if len(same_band) >= 2:
                horse_races = same_band

        # 直近5走（3走→5走に拡張。外れ値除外のため）
        horse_races = horse_races.sort_values('race_date', ascending=False).head(5)

        if len(horse_races) == 0:
            self._horse_history_cache[cache_key] = 50.0
            return 50.0

        t_indices = []
        for _, r in horse_races.iterrows():
            t_idx = self._single_t_index(r)
            if t_idx is not None:
                t_indices.append(t_idx)

        if not t_indices:
            self._horse_history_cache[cache_key] = 50.0
            return 50.0

        # 外れ値除外（最高・最低を除く中間値。3走以上の場合）
        if len(t_indices) >= 3:
            t_indices_sorted = sorted(t_indices)
            t_indices = t_indices_sorted[1:-1]  # 最高・最低を除外

        avg_t = float(np.mean(t_indices))
        self._horse_history_cache[cache_key] = avg_t
        return avg_t

    def _single_t_index(self, row) -> Optional[float]:
        """1レコードからT指数を算出"""
        time_sec = row.get('finish_time_seconds')
        dist = row.get('distance_m')
        surf = row.get('track_surface')
        cond = row.get('track_condition')
        venue = row.get('venue')

        if pd.isna(time_sec) or pd.isna(dist) or not surf:
            return None

        global_key = (int(dist), surf, cond)
        g_stats = self._global_stats.get(global_key)
        if not g_stats:
            return None

        venue_key = (venue, int(dist), surf, cond)
        v_adj = self._venue_adj.get(venue_key, {})
        time_adj = v_adj.get('time_adjustment', 0.0)

        adj_time = time_sec - time_adj
        g_mean = g_stats['time_mean']
        g_std = g_stats['time_std']

        if g_std <= 0:
            return None

        t_idx = 50 + 10 * (g_mean - adj_time) / g_std
        return max(20.0, min(80.0, t_idx))

    def _compute_field_strength(self, runners: pd.DataFrame) -> Dict:
        """出走馬の過去実績ベース（同馬場面+距離帯フィルタ）でメンバーレベルを評価"""
        if runners.empty:
            return {'field_t_avg': 50.0, 'field_label': '', 'field_top5_avg': 50.0}

        row0 = runners.iloc[0]
        race_date = row0.get('race_date')
        target_surface = row0.get('track_surface', '芝')
        target_band = row0.get('dist_band', 'mile')
        horse_ids = runners['horse_id'].dropna().unique()

        t_values = []
        for hid in horse_ids:
            t = self._compute_horse_historical_t(
                hid, before_date=race_date,
                target_surface=target_surface,
                target_dist_band=target_band)
            t_values.append(t)

        if not t_values:
            return {'field_t_avg': 50.0, 'field_label': '', 'field_top5_avg': 50.0}

        t_values_sorted = sorted(t_values, reverse=True)
        top5 = t_values_sorted[:5]
        top5_avg = np.mean(top5)
        all_avg = np.mean(t_values)

        if top5_avg >= 56.0:
            label = '強メンバー'
        elif top5_avg <= 44.0:
            label = '弱メンバー'
        elif top5_avg >= 53.0:
            label = 'やや強メンバー'
        elif top5_avg <= 47.0:
            label = 'やや弱メンバー'
        else:
            label = '平均メンバー'

        return {
            'field_t_avg': round(float(all_avg), 1),
            'field_label': label,
            'field_top5_avg': round(float(top5_avg), 1),
        }

    # ------------------------------------------------------------------
    # 6. 展開補正 (4分類脚質 × 傾斜関数 + 距離正規化着差)
    # ------------------------------------------------------------------

    def _compute_adjustment(self, pace_z: float, style: str,
                            bias_label: str, horse_position: int,
                            num_runners: int, margin_sec: float = 0.0,
                            distance_m: int = 2000) -> Dict:
        """
        展開補正値を4分類脚質×傾斜関数で計算。
        着差は距離正規化して凡走判定に使用。
        """
        if style == '不明':
            return {'pace_adj': 0.0, 'pos_adj': 0.0, 'total_adj': 0.0,
                    'adj_label': '', 'adj_reason': '脚質不明'}

        is_front = style in ('逃げ', '先行')

        # --- ペース補正 (4分類脚質×傾斜関数) ---
        # 感度は脚質で変わる（逃げ1.5, 先行1.0, 中団0.5, 差追込-1.0）
        sensitivity = self._style_sensitivity(style)
        # スロー(+z)かつ逃げ(sensitivity=1.5) → z*(-1.5) = マイナス(展開利)
        # ハイ(-z)かつ逃げ(sensitivity=1.5) → (-z)*(-1.5) = プラス(展開不利)
        # スロー(+z)かつ差追込(sensitivity=-1.0) → z*(1.0) = プラス(展開不利)
        pace_raw = pace_z * (-sensitivity)
        pace_adj = max(-4.0, min(4.0, pace_raw))

        # --- ポジションバイアス補正 ---
        pos_adj = 0.0
        if bias_label == '前有利':
            if is_front:
                pos_adj = -1.0
            else:
                pos_adj = 1.5
        elif bias_label == '差有利':
            if not is_front:
                pos_adj = -1.0
            else:
                pos_adj = 1.5

        # --- 好走/凡走の修正 (★着差を距離正規化) ---
        relative_pos = horse_position / num_runners if num_runners > 0 else 0.5
        is_good = relative_pos <= 0.33

        # 距離正規化: 1000mあたりの着差に変換
        dist_km = max(distance_m / 1000.0, 1.0)
        margin_per1000 = margin_sec / dist_km

        if not is_good:
            # ★ 凡走時の減衰を緩和 (0.3→0.7)
            # 旧: 0.3倍で度外視判定が実質無効化されていた
            # 新: 0.7倍で不利方向の補正を残す→凡走でも度外視判定が発動
            if margin_per1000 > 1.2:   # 1000mあたり1.2秒 = 大敗
                if pace_adj > 0:
                    pace_adj *= 0.7   # 不利補正を減衰するが残す
                else:
                    pace_adj *= 1.1   # 利の場合はわずかに増幅
                if pos_adj > 0:
                    pos_adj *= 0.7
                else:
                    pos_adj *= 1.1
            elif margin_per1000 > 0.5:  # 1000mあたり0.5秒 = 中敗
                if pace_adj > 0:
                    pace_adj *= 0.8
                if pos_adj > 0:
                    pos_adj *= 0.8

        total = round(pace_adj + pos_adj, 1)

        # ラベル
        if total >= 2.5:
            label = '展開不利'
        elif total >= 1.0:
            label = 'やや展開不利'
        elif total <= -2.5:
            label = '展開利あり'
        elif total <= -1.0:
            label = 'やや展開利'
        else:
            label = '展開中立'

        # 理由テキスト生成
        reasons = []
        if abs(pace_adj) >= 0.5:
            pace_type = 'ハイ' if pace_z < -0.3 else ('スロー' if pace_z > 0.3 else 'ミドル')
            if pace_adj > 0:
                reasons.append(f'{pace_type}中{style}で不利')
            else:
                reasons.append(f'{pace_type}中{style}で展開利')
        if abs(pos_adj) >= 0.5:
            if pos_adj > 0:
                reasons.append(f'{bias_label}の中{style}で逆バイアス')
            else:
                reasons.append(f'{bias_label}方向で順バイアス')

        return {
            'pace_adj': round(pace_adj, 1),
            'pos_adj': round(pos_adj, 1),
            'total_adj': total,
            'adj_label': label,
            'adj_reason': '。'.join(reasons) if reasons else '展開影響なし',
        }

    # ------------------------------------------------------------------
    # 7. 上がり3F順位
    # ------------------------------------------------------------------

    def _compute_last3f_rank(self, runners: pd.DataFrame,
                              horse_position: int) -> Dict:
        """同レース内での上がり3F順位を算出"""
        valid = runners.dropna(subset=['last_3f_time', 'finish_position'])
        if len(valid) == 0:
            return {'last3f_rank': None, 'last3f_time': None,
                    'last3f_total': 0, 'last3f_label': ''}

        valid = valid.sort_values('last_3f_time')
        total = len(valid)

        # この馬の着順に該当する行を探す
        horse_row = valid[valid['finish_position'] == horse_position]
        if len(horse_row) == 0:
            return {'last3f_rank': None, 'last3f_time': None,
                    'last3f_total': total, 'last3f_label': ''}

        h_time = horse_row.iloc[0]['last_3f_time']
        rank = int((valid['last_3f_time'] <= h_time).sum())

        # ラベル
        if rank == 1:
            label = '上がり最速'
        elif rank <= 3:
            label = '上がり上位'
        elif rank >= total - 2:
            label = '上がり最遅'
        else:
            label = ''

        return {
            'last3f_rank': rank,
            'last3f_time': round(float(h_time), 1),
            'last3f_total': total,
            'last3f_label': label,
        }

    # ------------------------------------------------------------------
    # 8. 隊列分析 (passing_order_1のばらつき)
    # ------------------------------------------------------------------

    def _analyze_formation(self, runners: pd.DataFrame) -> Dict:
        """隊列の凝縮度を passing_order_1 の標準偏差で推定
        
        均一分布のstd = N/sqrt(12) を基準に、実際のstdとの比率で判定。
        ratio < 0.7: 凝縮（馬群密集）
        ratio > 1.3: 縦長（隊列拡散）
        """
        valid = runners.dropna(subset=['passing_order_1', 'finish_position'])
        if len(valid) < 5:
            return {'formation_std': 0.0, 'formation_label': '',
                    'formation_confidence': 'low'}

        actual_std = float(valid['passing_order_1'].std())
        num = len(valid)

        # 均一分布(1,2,...,N)のstd = sqrt((N²-1)/12)
        import math
        expected_std = math.sqrt((num ** 2 - 1) / 12.0)
        if expected_std < 0.1:
            return {'formation_std': 0.0, 'formation_label': '',
                    'formation_confidence': 'low'}

        ratio = actual_std / expected_std

        if ratio < 0.7:
            label = '凝縮'    # 密集→位置取りの差が出にくい
        elif ratio > 1.3:
            label = '縦長'    # 拡散→先行馬の不利が大きい
        else:
            label = '標準'

        return {
            'formation_std': round(ratio, 2),
            'formation_label': label,
            'formation_confidence': 'high' if num >= 10 else 'mid',
        }

    # ------------------------------------------------------------------
    # 9. 度外視判定
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_verdict(total_adj: float, horse_position: int,
                          num_runners: int, last3f_rank: int = None,
                          last3f_total: int = 0,
                          margin_per1000: float = 0.0,
                          formation_label: str = '') -> Dict:
        """
        展開補正+着差+上がり順位の複合判定で「度外視」候補を判定。
        """
        relative_pos = horse_position / num_runners if num_runners > 0 else 0.5
        is_good = relative_pos <= 0.33
        is_bad = relative_pos > 0.67

        verdict = ''
        verdict_detail = ''
        verdict_level = 0  # 0=なし, 1=参考, 2=要注目, 3=度外視

        # --- 展開不利パターン ---
        if total_adj >= 3.0:
            if is_bad and margin_per1000 > 0.8:
                verdict = '度外視候補'
                verdict_detail = '展開大不利で大敗。実力を発揮できなかった可能性大'
                verdict_level = 3
            elif is_bad and last3f_rank and last3f_rank <= 3:
                verdict = '展開不利の中末脚使用'
                verdict_detail = '大敗も上がり上位。展開向けば巻き返し有力'
                verdict_level = 3
            elif is_good:
                verdict = '展開不利で好走'
                verdict_detail = '不利条件で好走。実力は額面以上'
                verdict_level = 2
            else:
                verdict = '展開不利で凡走'
                verdict_detail = '不利もあったが着差大きく能力疑問も'
                verdict_level = 1
        elif total_adj >= 2.0:
            if is_good and last3f_rank and last3f_rank <= 3:
                verdict = '不利の中好走+末脚'
                verdict_detail = '展開不利ながら好走し上がりも優秀。次走注目'
                verdict_level = 2
            elif is_bad and last3f_rank and last3f_rank <= 3:
                verdict = '展開不利+末脚あり'
                verdict_detail = '大敗も上がりは使えていた。条件替わりで注目'
                verdict_level = 2
            elif is_good:
                verdict = 'やや不利で好走'
                verdict_detail = '展開不利の中でも好走。安定した実力'
                verdict_level = 1

        # --- 展開利パターン ---
        elif total_adj <= -3.0:
            if is_good:
                verdict = '展開利で好走'
                verdict_detail = '好走も展開恵まれ。額面通りの実力か割引'
                verdict_level = -2
            elif is_bad:
                verdict = '展開利でも凡走'
                verdict_detail = '恵まれた展開で凡走。能力不足の可能性'
                verdict_level = -3
        elif total_adj <= -2.0:
            if is_good:
                verdict = 'やや展開利で好走'
                verdict_detail = '展開の助けもあった好走。過信注意'
                verdict_level = -1

        # --- 上がり最速なのに着順が悪い → 何か不利があった可能性 ---
        if not verdict and last3f_rank == 1 and is_bad:
            verdict = '上がり最速も大敗'
            verdict_detail = '末脚は最速だが届かず。不利や位置取り失敗の可能性'
            verdict_level = 2

        # --- 隊列×ペース補正の信頼度 ---
        if formation_label == '凝縮' and abs(total_adj) >= 1.0:
            verdict_detail += '（※凝縮隊列のため展開差は出にくかった可能性）'

        return {
            'verdict': verdict,
            'verdict_detail': verdict_detail,
            'verdict_level': verdict_level,
        }

    # ------------------------------------------------------------------
    # メイン分析関数
    # ------------------------------------------------------------------

    def analyze_race(self, race_id: str) -> Dict:
        """レース全体の展開分析を実行"""
        runners = self.get_race_runners(race_id)
        if runners.empty or len(runners) < 3:
            return self._empty_race_context()

        valid = runners.dropna(subset=['finish_position'])
        num_runners = len(valid)

        row0 = runners.iloc[0]
        race_date = row0.get('race_date')
        venue = row0.get('venue', '')
        surface = row0.get('track_surface', '芝')
        condition = row0.get('track_condition', '良')
        distance = row0.get('distance_m', 2000)
        race_class = row0.get('race_class', '')
        race_num = _race_number_from_id(race_id)

        # 1. ペース判定（先頭3頭ベース）
        pace_info = self._analyze_pace(runners)

        # 2. ポジションバイアス（午前/午後分割 + オッズ乖離）
        bias_info = self._get_daily_position_bias(race_date, venue, surface, race_num)

        # 3. 馬場速度（クラス補正＋距離正規化）
        speed_info = self._get_daily_track_speed(
            race_date, venue, distance, surface, condition, race_class)

        # 4. 相手強度（同馬場面＋距離帯フィルタ）
        field_info = self._compute_field_strength(runners)

        # 5. 隊列分析
        formation_info = self._analyze_formation(runners)

        return {
            'num_runners': num_runners,
            'distance_m': int(distance) if pd.notna(distance) else 2000,
            # ペース
            'pace_type': pace_info.get('pace_type', ''),
            'pace_rpci_front3': pace_info.get('pace_rpci_front3', 50.0),
            'pace_z_score': pace_info.get('pace_z_score', 0.0),
            # バイアス
            'bias_label': bias_info.get('bias_label', ''),
            'bias_corr': bias_info.get('bias_corr', 0.0),
            'bias_confidence': bias_info.get('bias_confidence', 'low'),
            'bias_front_top3_rate': bias_info.get('front_top3_rate', 0.5),
            'bias_sample_races': bias_info.get('sample_races', 0),
            'bias_odds': bias_info.get('odds_bias', 0.0),
            # 馬場速度
            'track_speed_per1000': speed_info.get('track_speed_per1000', 0.0),
            'track_speed_label': speed_info.get('track_speed_label', ''),
            'track_speed_raw': speed_info.get('track_speed_raw', 0.0),
            'track_speed_confidence': speed_info.get('confidence', 'low'),
            # 相手強度
            'field_t_avg': field_info.get('field_t_avg', 50.0),
            'field_label': field_info.get('field_label', ''),
            'field_top5_avg': field_info.get('field_top5_avg', 50.0),
            # 隊列
            'formation_std': formation_info.get('formation_std', 0.0),
            'formation_label': formation_info.get('formation_label', ''),
        }

    def annotate_horse_in_race(
        self, race_id: str, horse_corner4: float, horse_position: int
    ) -> Dict:
        """
        特定の馬のレースでの展開利/不利を判定し構造化注釈を生成。
        """
        ctx = self.analyze_race(race_id)
        runners = self.get_race_runners(race_id)
        num_runners = ctx.get('num_runners', 18)
        distance_m = ctx.get('distance_m', 2000)

        # 脚質4分類
        style = self.classify_running_style(horse_corner4, num_runners)
        is_front = self.is_front_runner(horse_corner4, num_runners)

        # 着差計算
        margin_sec = 0.0
        if horse_position and runners is not None and not runners.empty:
            winner = runners[runners['finish_position'] == 1]
            horse_row = runners[runners['finish_position'] == horse_position]
            if len(winner) > 0 and len(horse_row) > 0:
                w_time = winner.iloc[0].get('finish_time_seconds')
                h_time = horse_row.iloc[0].get('finish_time_seconds')
                if pd.notna(w_time) and pd.notna(h_time):
                    margin_sec = max(0.0, h_time - w_time)

        # 展開補正計算（★4分類脚質 + 距離正規化着差）
        adj_info = self._compute_adjustment(
            pace_z=ctx.get('pace_z_score', 0.0),
            style=style,
            bias_label=ctx.get('bias_label', ''),
            horse_position=horse_position or 0,
            num_runners=num_runners,
            margin_sec=margin_sec,
            distance_m=distance_m,
        )

        # ★ 上がり3F順位
        last3f_info = self._compute_last3f_rank(runners, horse_position)

        # ★ 度外視判定
        total_adj = adj_info.get('total_adj', 0.0)
        dist_km = max(distance_m / 1000.0, 1.0)
        margin_per1000 = margin_sec / dist_km
        formation_label = ctx.get('formation_label', '')

        verdict_info = self._compute_verdict(
            total_adj=total_adj,
            horse_position=horse_position or 0,
            num_runners=num_runners,
            last3f_rank=last3f_info.get('last3f_rank'),
            last3f_total=last3f_info.get('last3f_total', 0),
            margin_per1000=margin_per1000,
            formation_label=formation_label,
        )

        # 構造化コメント生成
        parts = []
        pace_type = ctx.get('pace_type', '')
        if pace_type and pace_type not in ('ミドル', ''):
            parts.append(pace_type)
        bias_label = ctx.get('bias_label', '')
        if bias_label:
            conf = ctx.get('bias_confidence', 'low')
            parts.append(f'{bias_label}({conf})')
        ts_label = ctx.get('track_speed_label', '')
        if ts_label and ts_label != '標準馬場':
            parts.append(ts_label)
        fl_label = ctx.get('field_label', '')
        if fl_label and fl_label != '平均メンバー':
            parts.append(fl_label)
        if formation_label and formation_label != '標準':
            parts.append(f'隊列:{formation_label}')

        overall_comment = ' / '.join(parts) if parts else '特記なし'

        if total_adj >= 3.0:
            overall_comment += ' 💎'
        elif total_adj <= -3.0:
            overall_comment += ' ⚠️'

        return {
            'race_context': ctx,
            # 構造化フィールド
            'horse_style': style,
            'is_front': is_front,
            'pace_adj': adj_info['pace_adj'],
            'position_adj': adj_info['pos_adj'],
            'total_adjustment': adj_info['total_adj'],
            'adjustment_label': adj_info['adj_label'],
            'adjustment_reason': adj_info['adj_reason'],
            'margin_sec': round(margin_sec, 2),
            # 展開要素
            'pace_type': pace_type,
            'pace_z': ctx.get('pace_z_score', 0.0),
            'bias_label': bias_label,
            'bias_confidence': ctx.get('bias_confidence', 'low'),
            'track_speed_label': ts_label,
            'track_speed_per1000': ctx.get('track_speed_per1000', 0.0),
            'field_label': fl_label,
            'field_top5_avg': ctx.get('field_top5_avg', 50.0),
            # 隊列
            'formation_label': formation_label,
            'formation_std': ctx.get('formation_std', 0.0),
            # 上がり3F
            'last3f_rank': last3f_info.get('last3f_rank'),
            'last3f_time': last3f_info.get('last3f_time'),
            'last3f_total': last3f_info.get('last3f_total', 0),
            'last3f_label': last3f_info.get('last3f_label', ''),
            # 度外視判定
            'verdict': verdict_info.get('verdict', ''),
            'verdict_detail': verdict_info.get('verdict_detail', ''),
            'verdict_level': verdict_info.get('verdict_level', 0),
            # 後方互換
            'overall_comment': overall_comment,
            'pace_comment': f"{pace_type}(Z={ctx.get('pace_z_score', 0):.1f})" if pace_type else '',
            'position_comment': adj_info['adj_reason'],
            'track_comment': f"{ts_label}({ctx.get('track_speed_per1000', 0):+.2f}秒/1000m)" if ts_label else '',
            'field_comment': f"{fl_label}(Top5平均T{ctx.get('field_top5_avg', 50):.0f})" if fl_label else '',
        }

    # ------------------------------------------------------------------
    # 枠順バイアス（変更なし）
    # ------------------------------------------------------------------

    def analyze_waku_bias(self, venue: str, distance: int, surface: str,
                          years: int = 3) -> Dict:
        """枠順バイアスを分析"""
        self._load_parquet()
        cutoff = pd.Timestamp.now() - pd.DateOffset(years=years)
        mask = (
            (self._df['venue'] == venue) &
            (self._df['distance_m'] == distance) &
            (self._df['track_surface'] == surface) &
            (self._df['race_date'] >= cutoff) &
            (self._df['finish_position'].notna()) &
            (self._df['bracket_number'].notna())
        )
        subset = self._df[mask].copy()
        if len(subset) < 50:
            return {}
        result = {}
        for waku in range(1, 9):
            w_data = subset[subset['bracket_number'] == waku]
            if len(w_data) < 5:
                continue
            fukusho = len(w_data[w_data['finish_position'] <= 3]) / len(w_data)
            result[waku] = {
                'fukusho_rate': round(fukusho, 3),
                'count': len(w_data),
                'avg_position': round(w_data['finish_position'].mean(), 1)
            }
        return result

    # ------------------------------------------------------------------
    # ペース予測（変更なし）
    # ------------------------------------------------------------------

    def predict_pace(self, horse_histories: List[Dict]) -> Dict:
        """出走馬の過去データから今日のレースのペースを予測"""
        self._load_parquet()
        running_styles = []
        pace_benefit = []

        for horse_data in horse_histories:
            name = horse_data.get('馬名', '?')
            umaban = horse_data.get('馬番', '?')
            records = horse_data.get('records', [])
            if not records:
                continue
            c4_positions = []
            rpcis = []
            hi_wins, hi_total, slow_wins, slow_total = 0, 0, 0, 0
            for rec in records[:5]:
                c4 = rec.get('4C')
                try:
                    c4 = float(c4) if c4 else None
                except (ValueError, TypeError):
                    c4 = None
                if c4:
                    c4_positions.append(c4)
                rpci = rec.get('RPCI')
                try:
                    rpci = float(rpci) if rpci else None
                except (ValueError, TypeError):
                    rpci = None
                if rpci:
                    rpcis.append(rpci)
                rank = rec.get('着順')
                try:
                    rank = int(rank) if rank else None
                except (ValueError, TypeError):
                    rank = None
                if rpci and rank:
                    if rpci < 48:
                        hi_total += 1
                        if rank <= 3:
                            hi_wins += 1
                    elif rpci > 52:
                        slow_total += 1
                        if rank <= 3:
                            slow_wins += 1

            avg_c4 = np.mean(c4_positions) if c4_positions else 9.0
            avg_rpci = np.mean(rpcis) if rpcis else 50.0
            running_styles.append({
                'umaban': umaban, 'name': name,
                'avg_c4': round(avg_c4, 1), 'avg_rpci': round(avg_rpci, 1),
            })
            hi_rate = round(hi_wins / hi_total * 100, 0) if hi_total >= 2 else None
            slow_rate = round(slow_wins / slow_total * 100, 0) if slow_total >= 2 else None
            pace_benefit.append({
                'umaban': umaban, 'name': name,
                'high_pace_rate': hi_rate, 'slow_pace_rate': slow_rate,
                'hi_sample': hi_total, 'slow_sample': slow_total,
            })

        if not running_styles:
            return {'predicted_rpci': 50.0, 'predicted_pace': 'ミドル'}

        avg_rpci_all = np.mean([s['avg_rpci'] for s in running_styles])
        front_count = sum(1 for s in running_styles if s['avg_c4'] <= 4)
        front_ratio = front_count / len(running_styles)
        rpci_correction = -3.0 * (front_ratio - 0.35)
        predicted_rpci = round(avg_rpci_all + rpci_correction, 1)

        if predicted_rpci > 52:
            predicted_pace = 'スロー'
        elif predicted_rpci < 48:
            predicted_pace = 'ハイ'
        else:
            predicted_pace = 'ミドル'

        sorted_styles = sorted(running_styles, key=lambda x: x['avg_c4'])
        return {
            'predicted_rpci': predicted_rpci,
            'predicted_pace': predicted_pace,
            'escaping_horses': [s for s in sorted_styles if s['avg_c4'] <= 2],
            'front_runners': [s for s in sorted_styles if 2 < s['avg_c4'] <= 5],
            'mid_runners': [s for s in sorted_styles if 5 < s['avg_c4'] <= 10],
            'closers': [s for s in sorted_styles if s['avg_c4'] > 10],
            'pace_benefit_table': pace_benefit,
            'front_ratio': round(front_ratio, 2),
        }

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------

    def _empty_race_context(self) -> Dict:
        return {
            'pace_type': '', 'pace_rpci_front3': 50.0, 'pace_z_score': 0.0,
            'bias_label': '', 'bias_corr': 0.0, 'bias_confidence': 'low',
            'bias_front_top3_rate': 0.5, 'bias_sample_races': 0,
            'bias_odds': 0.0,
            'track_speed_per1000': 0.0, 'track_speed_label': '',
            'track_speed_raw': 0.0, 'track_speed_confidence': 'low',
            'field_t_avg': 50.0, 'field_label': '',
            'field_top5_avg': 50.0, 'num_runners': 0,
            'distance_m': 2000,
        }


# =====================================================================
# batch_annotate_history (外部インターフェース維持)
# =====================================================================

def batch_annotate_history(
    analyzer: RaceContextAnalyzer,
    history: List[Dict]
) -> List[Dict]:
    """
    馬の過去走一覧に展開注釈を一括追加。
    """
    for race in history:
        race_id = race.get('race_id', '')
        if not race_id or len(race_id) < 10:
            _set_empty_annotations(race)
            continue

        try:
            c4_str = race.get('4C', '')
            c4 = float(c4_str) if c4_str else None
        except (ValueError, TypeError):
            c4 = None

        try:
            pos_str = race.get('着順', '')
            pos = int(pos_str) if pos_str else None
        except (ValueError, TypeError):
            pos = None

        if c4 is None and pos is None:
            _set_empty_annotations(race)
            continue

        try:
            ann = analyzer.annotate_horse_in_race(race_id, c4, pos)

            # 後方互換カラム
            race['展開注釈'] = ann['overall_comment']
            race['展開補正'] = ann['total_adjustment']
            race['ペース'] = ann.get('pace_comment', '')
            race['位置バイアス'] = ann.get('position_comment', '')
            race['馬場速度'] = ann.get('track_comment', '')
            race['相手強度'] = ann.get('field_comment', '')
            race['展開ラベル'] = ann['adjustment_label']

            # 構造化フィールド
            race['展開_脚質'] = ann.get('horse_style', '')
            race['展開_ペース種別'] = ann.get('pace_type', '')
            race['展開_ペースZ'] = ann.get('pace_z', 0.0)
            race['展開_バイアス'] = ann.get('bias_label', '')
            race['展開_バイアス信頼度'] = ann.get('bias_confidence', 'low')
            race['展開_馬場速度ラベル'] = ann.get('track_speed_label', '')
            race['展開_馬場速度値'] = ann.get('track_speed_per1000', 0.0)
            race['展開_相手強度ラベル'] = ann.get('field_label', '')
            race['展開_相手強度T'] = ann.get('field_top5_avg', 50.0)
            race['展開_ペース補正'] = ann.get('pace_adj', 0.0)
            race['展開_位置補正'] = ann.get('position_adj', 0.0)
            race['展開_補正理由'] = ann.get('adjustment_reason', '')
            race['展開_着差秒'] = ann.get('margin_sec', 0.0)

            # ★ 上がり3F
            race['展開_上がり順位'] = ann.get('last3f_rank')
            race['展開_上がりタイム'] = ann.get('last3f_time')
            race['展開_上がり頭数'] = ann.get('last3f_total', 0)
            race['展開_上がりラベル'] = ann.get('last3f_label', '')

            # ★ 隊列
            race['展開_隊列'] = ann.get('formation_label', '')
            race['展開_隊列std'] = ann.get('formation_std', 0.0)

            # ★ 度外視判定
            race['展開_判定'] = ann.get('verdict', '')
            race['展開_判定詳細'] = ann.get('verdict_detail', '')
            race['展開_判定レベル'] = ann.get('verdict_level', 0)

            # 展開補正T指数
            raw_t = race.get('タイム指数') or race.get('T指数')
            if raw_t:
                try:
                    raw_t_val = float(raw_t)
                    adj_t = max(20.0, min(80.0, raw_t_val + ann['total_adjustment']))
                    race['展開補正T指数'] = round(adj_t, 1)
                except (ValueError, TypeError):
                    pass

        except Exception as e:
            logger.warning(f"Failed to annotate race {race_id}: {e}")
            _set_empty_annotations(race)

    return history


def _set_empty_annotations(race: Dict):
    """空の注釈カラムをセット"""
    race['展開注釈'] = ''
    race['展開補正'] = 0.0
    race['ペース'] = ''
    race['位置バイアス'] = ''
    race['馬場速度'] = ''
    race['相手強度'] = ''
    race['展開ラベル'] = ''
    race['展開_脚質'] = ''
    race['展開_ペース種別'] = ''
    race['展開_ペースZ'] = 0.0
    race['展開_バイアス'] = ''
    race['展開_バイアス信頼度'] = 'low'
    race['展開_馬場速度ラベル'] = ''
    race['展開_馬場速度値'] = 0.0
    race['展開_相手強度ラベル'] = ''
    race['展開_相手強度T'] = 50.0
    race['展開_ペース補正'] = 0.0
    race['展開_位置補正'] = 0.0
    race['展開_補正理由'] = ''
    race['展開_着差秒'] = 0.0
    race['展開_上がり順位'] = None
    race['展開_上がりタイム'] = None
    race['展開_上がり頭数'] = 0
    race['展開_上がりラベル'] = ''
    race['展開_隊列'] = ''
    race['展開_隊列std'] = 0.0
    race['展開_判定'] = ''
    race['展開_判定詳細'] = ''
    race['展開_判定レベル'] = 0
