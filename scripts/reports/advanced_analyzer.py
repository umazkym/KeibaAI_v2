#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Advanced Race Analyzer v3
races.parquet (59万行) をフル活用し、コースバイアス・馬別プロファイル・
展開適性を連続正規化値で算出する。カテゴリ分類は一切行わない。
"""
import os
import pandas as pd
import numpy as np
import logging
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "keibaai", "data")
RACES_PARQUET = os.path.join(DATA_DIR, "parsed", "parquet", "races", "races.parquet")
RACE_DETAILS_PARQUET = os.path.join(DATA_DIR, "parsed", "parquet", "race_details", "race_details.parquet")

# ロードするカラム
RACES_COLS = [
    'race_id', 'race_date', 'venue', 'distance_m', 'track_surface',
    'track_condition', 'finish_position', 'horse_id', 'horse_name',
    'horse_number', 'bracket_number',
    'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
    'last_3f_time', 'last3f_rank', 'finish_time_seconds',
    'pace_index', 'final_corner_to_finish',
]


def _norm_pos(position, field_size):
    """通過順位を0-1に正規化 (0=先頭, 1=最後尾)"""
    if pd.isna(position) or pd.isna(field_size) or field_size <= 1:
        return np.nan
    return (position - 1) / (field_size - 1)


def _safe_round(val, n=3):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return round(float(val), n)


class AdvancedPaceAnalyzer:
    def __init__(self):
        self.races_df = None
        self._group_time_stats = None
        self._group_l3f_stats = None
        self._load_data()

    def _load_data(self):
        if not os.path.exists(RACES_PARQUET):
            logger.warning(f"races.parquet not found: {RACES_PARQUET}")
            return
        try:
            self.races_df = pd.read_parquet(RACES_PARQUET, columns=RACES_COLS)
            # 頭数算出
            fs = self.races_df.groupby('race_id').size().reset_index(name='field_size')
            self.races_df = self.races_df.merge(fs, on='race_id', how='left')
            logger.info(f"Loaded races: {len(self.races_df):,} rows")
        except Exception as e:
            logger.error(f"Failed to load races.parquet: {e}")

    def _ensure_group_stats(self):
        """Z-score計算用のグループ統計をキャッシュ"""
        if self._group_time_stats is not None:
            return
        gcols = ['venue', 'distance_m', 'track_surface', 'track_condition']
        vt = self.races_df.dropna(subset=['finish_time_seconds'])
        ts = vt.groupby(gcols)['finish_time_seconds'].agg(['mean', 'std', 'count']).reset_index()
        ts.columns = gcols + ['time_mean', 'time_std', 'time_n']
        self._group_time_stats = ts[ts['time_n'] >= 10]

        vl = self.races_df.dropna(subset=['last_3f_time'])
        ls = vl.groupby(gcols)['last_3f_time'].agg(['mean', 'std', 'count']).reset_index()
        ls.columns = gcols + ['l3f_mean', 'l3f_std', 'l3f_n']
        self._group_l3f_stats = ls[ls['l3f_n'] >= 10]

    def _filter_course(self, venue, distance_m, track_surface):
        if self.races_df is None:
            return pd.DataFrame()
        mask = (
            (self.races_df['venue'] == venue) &
            (self.races_df['distance_m'] == float(distance_m)) &
            (self.races_df['track_surface'] == track_surface)
        )
        return self.races_df[mask].copy()

    # ==========================================
    # ① K-Means ラップクラスタリング
    # ==========================================
    def get_course_lap_clusters(self, venue, distance_m, track_surface, n_clusters=4):
        if self.races_df is None or not os.path.exists(RACE_DETAILS_PARQUET):
            return []
        course_df = self._filter_course(venue, distance_m, track_surface)
        target_ids = course_df['race_id'].unique().tolist()
        if not target_ids:
            return []
        try:
            df_d = pd.read_parquet(RACE_DETAILS_PARQUET, columns=['race_id', 'lap_times'])
            df_d = df_d[df_d['race_id'].isin(target_ids)]
        except Exception:
            return []
        exp_len = int(distance_m) // 200
        valid = []
        for _, row in df_d.iterrows():
            laps = row['lap_times']
            if isinstance(laps, (np.ndarray, list)) and len(laps) == exp_len:
                valid.append([float(x) for x in laps])
        if len(valid) < 10:
            return []
        X = np.array(valid)
        nc = min(n_clusters, len(X))
        km = KMeans(n_clusters=nc, random_state=42, n_init=10)
        km.fit(X)
        info = []
        for i in range(nc):
            info.append({
                'cluster_id': i,
                'count': int(np.sum(km.labels_ == i)),
                'center_laps': [round(float(x), 2) for x in km.cluster_centers_[i]],
            })
        info.sort(key=lambda x: x['count'], reverse=True)
        return info

    # ==========================================
    # ① 追加: コースラップ統計 (平均±σ, 馬場別)
    # ==========================================
    def get_course_lap_stats(self, venue, distance_m, track_surface):
        if not os.path.exists(RACE_DETAILS_PARQUET):
            return None
        course_df = self._filter_course(venue, distance_m, track_surface)
        if course_df.empty:
            return None
        target_ids = course_df['race_id'].unique().tolist()
        cond_map = course_df.drop_duplicates('race_id').set_index('race_id')['track_condition'].to_dict()
        try:
            df_d = pd.read_parquet(RACE_DETAILS_PARQUET, columns=['race_id', 'lap_times'])
            df_d = df_d[df_d['race_id'].isin(target_ids)]
        except Exception:
            return None
        exp_len = int(distance_m) // 200
        all_laps, by_cond = [], {}
        for _, row in df_d.iterrows():
            laps = row['lap_times']
            if isinstance(laps, (np.ndarray, list)) and len(laps) == exp_len:
                fl = [float(x) for x in laps]
                all_laps.append(fl)
                c = cond_map.get(row['race_id'], '不明')
                by_cond.setdefault(c, []).append(fl)
        if not all_laps:
            return None
        X = np.array(all_laps)
        result = {
            'mean': [round(float(x), 3) for x in X.mean(axis=0)],
            'std': [round(float(x), 3) for x in X.std(axis=0)],
            'count': len(all_laps),
            'by_condition': {}
        }
        for c, lst in by_cond.items():
            if len(lst) >= 5:
                a = np.array(lst)
                result['by_condition'][c] = {
                    'mean': [round(float(x), 3) for x in a.mean(axis=0)],
                    'count': len(lst)
                }
        return result

    # ==========================================
    # ② コース位置取り×着順 相関データ
    # ==========================================
    def get_course_position_stats(self, venue, distance_m, track_surface, max_points=2000):
        course_df = self._filter_course(venue, distance_m, track_surface)
        if course_df.empty:
            return {'points': [], 'trend': [], 'race_count': 0}
        df = course_df.copy()
        # 最終コーナー位置を使用（4角優先、なければ3角、2角）
        df['last_corner'] = df['passing_order_4'].fillna(df['passing_order_3']).fillna(df['passing_order_2'])
        df['norm_lc'] = df.apply(lambda r: _norm_pos(r['last_corner'], r['field_size']), axis=1)
        df['norm_l3f'] = df.apply(lambda r: _norm_pos(r['last3f_rank'], r['field_size']), axis=1)
        valid = df.dropna(subset=['norm_lc', 'finish_position'])
        if len(valid) > max_points:
            valid = valid.sample(n=max_points, random_state=42)
        points = []
        for _, r in valid.iterrows():
            points.append({
                'nlc': _safe_round(r['norm_lc']),
                'fp': int(r['finish_position']),
                'nl3f': _safe_round(r.get('norm_l3f')),
                'cond': str(r['track_condition']),
            })
        # トレンド (ビン別)
        trend = []
        full = df.dropna(subset=['norm_lc', 'finish_position']).copy()
        if not full.empty:
            full['bin'] = pd.cut(full['norm_lc'], bins=10, labels=False)
            for _, g in full.groupby('bin'):
                if len(g) >= 5:
                    trend.append({
                        'x': _safe_round(g['norm_lc'].mean()),
                        'y_mean': _safe_round(g['finish_position'].mean(), 2),
                        'win_rate': _safe_round((g['finish_position'] == 1).mean(), 4),
                        'top3_rate': _safe_round((g['finish_position'] <= 3).mean(), 4),
                        'n': len(g)
                    })
        return {
            'points': points, 'trend': sorted(trend, key=lambda t: t['x'] or 0),
            'race_count': int(course_df['race_id'].nunique()),
        }

    # ==========================================
    # ③④⑤⑥ 馬別プロファイル
    # ==========================================
    def get_horse_profiles(self, horse_ids, cur_venue=None, cur_dist=None, cur_surface=None, max_runs=30):
        if self.races_df is None or not horse_ids:
            return {}
        self._ensure_group_stats()
        gcols = ['venue', 'distance_m', 'track_surface', 'track_condition']
        results = {}
        for hid in horse_ids:
            runs = self.races_df[self.races_df['horse_id'] == hid].copy()
            if runs.empty:
                results[hid] = self._empty_profile()
                continue
            runs = runs.sort_values('race_date', ascending=False).head(max_runs)
            hname = str(runs.iloc[0]['horse_name'])

            # --- 軌跡 ---
            trajs = []
            for _, r in runs.iterrows():
                fs = r['field_size']
                pos = []
                for c in [1, 2, 3, 4]:
                    pos.append(_safe_round(_norm_pos(r.get(f'passing_order_{c}'), fs)))
                pos.append(_safe_round(_norm_pos(r['finish_position'], fs)))
                is_same = bool(
                    cur_venue and cur_dist and cur_surface and
                    r['venue'] == cur_venue and
                    r['distance_m'] == float(cur_dist) and
                    r['track_surface'] == cur_surface
                )
                trajs.append({
                    'pos': pos, 'date': str(r['race_date']),
                    'venue': str(r['venue']), 'dist': int(r['distance_m']) if pd.notna(r['distance_m']) else 0,
                    'cond': str(r['track_condition']),
                    'finish': int(r['finish_position']) if pd.notna(r['finish_position']) else 0,
                    'same': is_same,
                })

            # --- Z-scores ---
            merged = runs.merge(self._group_time_stats, on=gcols, how='left')
            merged = merged.merge(self._group_l3f_stats, on=gcols, how='left')

            time_z, l3f_z, cond_z, same_z = [], [], {}, []
            for _, r in merged.iterrows():
                cond = str(r['track_condition'])
                # Time Z
                if pd.notna(r.get('time_std')) and r['time_std'] > 0.1 and pd.notna(r['finish_time_seconds']):
                    tz = (r['finish_time_seconds'] - r['time_mean']) / r['time_std']
                    tz = round(float(tz), 3)
                    time_z.append({'z': tz, 'date': str(r['race_date']), 'venue': str(r['venue']), 'cond': cond})
                    cond_z.setdefault(cond, []).append(tz)
                    if (cur_venue and r['venue'] == cur_venue and
                            r['distance_m'] == float(cur_dist) and r['track_surface'] == cur_surface):
                        same_z.append(tz)
                # 3F Z
                if pd.notna(r.get('l3f_std')) and r['l3f_std'] > 0.1 and pd.notna(r['last_3f_time']):
                    lz = (r['last_3f_time'] - r['l3f_mean']) / r['l3f_std']
                    l3f_z.append({'z': round(float(lz), 3), 'date': str(r['race_date']), 'venue': str(r['venue']), 'cond': cond})

            by_cond = {}
            for c, zs in cond_z.items():
                by_cond[c] = {'z_med': round(float(np.median(zs)), 3), 'zs': [round(z, 3) for z in zs], 'n': len(zs)}

            # --- ペース ---
            pace = []
            for _, r in runs.iterrows():
                if pd.notna(r.get('pace_index')) and pd.notna(r.get('finish_position')):
                    pace.append({
                        'pi': round(float(r['pace_index']), 3),
                        'fp': int(r['finish_position']),
                        'nf': _safe_round(_norm_pos(r['finish_position'], r['field_size'])),
                    })

            # --- 通算成績 ---
            all_r = self.races_df[self.races_df['horse_id'] == hid]
            total = len(all_r)
            results[hid] = {
                'name': hname, 'trajs': trajs, 'time_z': time_z, 'l3f_z': l3f_z,
                'by_cond': by_cond,
                'same_course': {'z_med': round(float(np.median(same_z)), 3) if same_z else None, 'n': len(same_z)},
                'pace': pace,
                'career': {'runs': total, 'wins': int((all_r['finish_position'] == 1).sum()),
                           'top2': int((all_r['finish_position'] <= 2).sum()),
                           'top3': int((all_r['finish_position'] <= 3).sum())},
            }
        return results

    def _empty_profile(self):
        return {'name': '', 'trajs': [], 'time_z': [], 'l3f_z': [], 'by_cond': {},
                'same_course': {'z_med': None, 'n': 0}, 'pace': [],
                'career': {'runs': 0, 'wins': 0, 'top2': 0, 'top3': 0}}

    # ==========================================
    # ⑦ 総合比較テーブル
    # ==========================================
    def get_summary_table(self, horse_profiles):
        summary = {}
        for hid, p in horse_profiles.items():
            tz = [x['z'] for x in p['time_z']]
            lz = [x['z'] for x in p['l3f_z']]
            pos3 = [t['pos'][2] for t in p['trajs'] if t['pos'][2] is not None]
            summary[hid] = {
                'name': p['name'],
                'tz_med': round(float(np.median(tz)), 3) if tz else None,
                'lz_med': round(float(np.median(lz)), 3) if lz else None,
                'pos3_med': round(float(np.median(pos3)), 3) if pos3 else None,
                'sc_z': p['same_course']['z_med'], 'sc_n': p['same_course']['n'],
                'pi_med': round(float(np.median([x['pi'] for x in p['pace']])), 3) if p['pace'] else None,
                'career': p['career'],
            }
        return summary


    # ==========================================
    # ⑧ 展開予測 (Race Dynamics Prediction)
    # ==========================================
    def predict_race_dynamics(self, horse_profiles, cur_venue, cur_dist, cur_surface, cur_cond='良'):
        """
        出走馬のプロファイルから展開(逃げ馬、ペース、有利/不利)を予測する。
        Returns dict with:
          - formation: 想定隊列 [{umaban, name, lead_score, position_label}, ...]
          - pace_prediction: {type, label, reason, pi_median}
          - advantages: [{umaban, name, verdict, reason, est_time, est_l3f}, ...]
          - course_baseline: {time_mean, time_std, l3f_mean, l3f_std, count}
        """
        if not horse_profiles:
            return {'formation': [], 'pace_prediction': {}, 'advantages': [], 'course_baseline': {}}

        self._ensure_group_stats()

        # --- コース基準値 ---
        baseline = {}
        if self._group_time_stats is not None:
            cond_match = self._group_time_stats[
                (self._group_time_stats['venue'] == cur_venue) &
                (self._group_time_stats['distance_m'] == float(cur_dist)) &
                (self._group_time_stats['track_surface'] == cur_surface) &
                (self._group_time_stats['track_condition'] == cur_cond)
            ]
            if not cond_match.empty:
                row = cond_match.iloc[0]
                baseline['time_mean'] = round(float(row['time_mean']), 2)
                baseline['time_std'] = round(float(row['time_std']), 3)
                baseline['count'] = int(row['time_n'])
            # fallback: 馬場条件なし
            if not baseline:
                any_match = self._group_time_stats[
                    (self._group_time_stats['venue'] == cur_venue) &
                    (self._group_time_stats['distance_m'] == float(cur_dist)) &
                    (self._group_time_stats['track_surface'] == cur_surface)
                ]
                if not any_match.empty:
                    baseline['time_mean'] = round(float(any_match['time_mean'].mean()), 2)
                    baseline['time_std'] = round(float(any_match['time_std'].mean()), 3)
                    baseline['count'] = int(any_match['time_n'].sum())

        if self._group_l3f_stats is not None:
            cond_match = self._group_l3f_stats[
                (self._group_l3f_stats['venue'] == cur_venue) &
                (self._group_l3f_stats['distance_m'] == float(cur_dist)) &
                (self._group_l3f_stats['track_surface'] == cur_surface) &
                (self._group_l3f_stats['track_condition'] == cur_cond)
            ]
            if not cond_match.empty:
                row = cond_match.iloc[0]
                baseline['l3f_mean'] = round(float(row['l3f_mean']), 2)
                baseline['l3f_std'] = round(float(row['l3f_std']), 3)
            elif not baseline.get('l3f_mean'):
                any_match = self._group_l3f_stats[
                    (self._group_l3f_stats['venue'] == cur_venue) &
                    (self._group_l3f_stats['distance_m'] == float(cur_dist)) &
                    (self._group_l3f_stats['track_surface'] == cur_surface)
                ]
                if not any_match.empty:
                    baseline['l3f_mean'] = round(float(any_match['l3f_mean'].mean()), 2)
                    baseline['l3f_std'] = round(float(any_match['l3f_std'].mean()), 3)

        # --- 各馬の先行指数 (lead_score) 算出 ---
        # 1角・2角の正規化位置の中央値が小さいほど先行する
        horse_leads = []
        for hid, prof in horse_profiles.items():
            trajs = prof.get('trajs', [])
            early_positions = []
            for t in trajs:
                p1, p2 = t['pos'][0], t['pos'][1]
                vals = [v for v in [p1, p2] if v is not None]
                if vals:
                    early_positions.append(np.mean(vals))

            if early_positions:
                lead_score = round(float(np.median(early_positions)), 3)
            else:
                lead_score = 0.5  # データなし→中団

            horse_leads.append({
                'hid': hid,
                'umaban': prof.get('umaban', '0'),
                'name': prof.get('name', ''),
                'lead_score': lead_score,
                'n_runs': len(trajs),
            })

        # 先行指数でソート（小さいほど前）
        horse_leads.sort(key=lambda x: x['lead_score'])

        # 位置ラベル付与
        n = len(horse_leads)
        formation = []
        for i, h in enumerate(horse_leads):
            ratio = i / max(n - 1, 1)
            if ratio <= 0.15:
                label = '逃げ'
            elif ratio <= 0.35:
                label = '先行'
            elif ratio <= 0.65:
                label = '中団'
            elif ratio <= 0.85:
                label = '差し'
            else:
                label = '追込'
            formation.append({
                'umaban': h['umaban'],
                'name': h['name'],
                'lead_score': h['lead_score'],
                'position_label': label,
                'n_runs': h['n_runs'],
            })

        # --- ペース予測 ---
        # 逃げ候補が多い→ハイペース、少ない→スロー
        escape_candidates = [h for h in horse_leads if h['lead_score'] < 0.2]
        n_escapees = len(escape_candidates)

        # コース全体のペース指数分布
        course_df = self._filter_course(cur_venue, float(cur_dist), cur_surface)
        pi_values = course_df['pace_index'].dropna().tolist() if not course_df.empty else []
        pi_median = round(float(np.median(pi_values)), 1) if pi_values else 50.0

        if n_escapees >= 3:
            pace_type = 'high'
            pace_label = 'ハイペース'
            pace_reason = f'逃げ/先行候補が{n_escapees}頭と多く、前半のペースが上がりやすい'
        elif n_escapees >= 2:
            pace_type = 'mid'
            pace_label = 'ミドルペース'
            pace_reason = f'逃げ候補{n_escapees}頭で平均的なペースが予想される'
        elif n_escapees == 1:
            pace_type = 'slow'
            pace_label = 'スローペース'
            pace_reason = f'逃げ馬は{escape_candidates[0]["name"]}のみ。単騎逃げでペースが落ち着く可能性が高い'
        else:
            pace_type = 'mid'
            pace_label = 'ミドルペース'
            pace_reason = '明確な逃げ馬不在。牽制し合い中盤からのペースアップが予想される'

        pace_prediction = {
            'type': pace_type,
            'label': pace_label,
            'reason': pace_reason,
            'pi_median': pi_median,
            'n_escapees': n_escapees,
        }

        # --- 各馬の有利/不利判定 ---
        advantages = []
        for h in formation:
            hid = h.get('hid') if 'hid' in h else None
            # formation doesn't have hid, find it
            for hl in horse_leads:
                if hl['umaban'] == h['umaban']:
                    hid = hl['hid']
                    break
            prof = horse_profiles.get(hid, {})

            # ペース適性: pace データからペース指数 vs 着順の相関を見る
            pace_data = prof.get('pace', [])
            verdict = '中立'
            reason = ''

            if pace_data and len(pace_data) >= 2:
                # ペースが予測ペースに近い時の着順を見る
                if pace_type == 'high':
                    # ハイペース（PI < 48）での成績
                    hi_pace = [p for p in pace_data if p['pi'] < 48]
                    lo_pace = [p for p in pace_data if p['pi'] >= 48]
                    if hi_pace:
                        hi_avg = np.mean([p['fp'] for p in hi_pace])
                        lo_avg = np.mean([p['fp'] for p in lo_pace]) if lo_pace else hi_avg
                        if hi_avg < lo_avg - 1:
                            verdict = '有利'
                            reason = f'ハイペース時の平均着順{hi_avg:.1f}（通常{lo_avg:.1f}）'
                        elif hi_avg > lo_avg + 1:
                            verdict = '不利'
                            reason = f'ハイペース時の平均着順{hi_avg:.1f}（通常{lo_avg:.1f}）'
                elif pace_type == 'slow':
                    # スローペース（PI > 50）での成績
                    sl_pace = [p for p in pace_data if p['pi'] > 50]
                    ot_pace = [p for p in pace_data if p['pi'] <= 50]
                    if sl_pace:
                        sl_avg = np.mean([p['fp'] for p in sl_pace])
                        ot_avg = np.mean([p['fp'] for p in ot_pace]) if ot_pace else sl_avg
                        if sl_avg < ot_avg - 1:
                            verdict = '有利'
                            reason = f'スローペース時の平均着順{sl_avg:.1f}（通常{ot_avg:.1f}）'
                        elif sl_avg > ot_avg + 1:
                            verdict = '不利'
                            reason = f'スローペース時の平均着順{sl_avg:.1f}（通常{ot_avg:.1f}）'

            # 位置取りによる補正
            pos_label = h['position_label']
            if pace_type == 'high' and pos_label in ('差し', '追込'):
                if verdict != '有利':
                    verdict = '有利'
                    reason = (reason + ' / ' if reason else '') + '差し有利のハイペース展開'
            elif pace_type == 'slow' and pos_label in ('逃げ', '先行'):
                if verdict != '有利':
                    verdict = '有利'
                    reason = (reason + ' / ' if reason else '') + '前有利のスローペース展開'
            elif pace_type == 'slow' and pos_label in ('追込',):
                if verdict != '不利':
                    verdict = '不利'
                    reason = (reason + ' / ' if reason else '') + '追込不利のスローペース'

            if not reason:
                reason = 'ペース適性データ不足'

            # 想定タイム・上がり3F
            est_time = None
            est_l3f = None
            time_z_list = prof.get('time_z', [])
            l3f_z_list = prof.get('l3f_z', [])
            if time_z_list and baseline.get('time_mean') and baseline.get('time_std'):
                z_med = float(np.median([x['z'] for x in time_z_list]))
                est_time = round(baseline['time_mean'] + z_med * baseline['time_std'], 1)
            if l3f_z_list and baseline.get('l3f_mean') and baseline.get('l3f_std'):
                z_med = float(np.median([x['z'] for x in l3f_z_list]))
                est_l3f = round(baseline['l3f_mean'] + z_med * baseline['l3f_std'], 1)

            advantages.append({
                'umaban': h['umaban'],
                'name': h['name'],
                'position_label': pos_label,
                'verdict': verdict,
                'reason': reason,
                'est_time': est_time,
                'est_l3f': est_l3f,
                'lead_score': h['lead_score'],
            })

        # umaban順にソート
        advantages.sort(key=lambda x: int(x['umaban']) if str(x['umaban']).isdigit() else 99)

        return {
            'formation': formation,
            'pace_prediction': pace_prediction,
            'advantages': advantages,
            'course_baseline': baseline,
        }


if __name__ == "__main__":
    analyzer = AdvancedPaceAnalyzer()
    # テスト: 中山ダート1400
    cs = analyzer.get_course_position_stats("中山", 1400, "ダート")
    print(f"Position stats: {cs['race_count']} races, {len(cs['points'])} points")
    cl = analyzer.get_course_lap_clusters("中山", 1400, "ダート")
    print(f"Clusters: {len(cl)}")
    ls = analyzer.get_course_lap_stats("中山", 1400, "ダート")
    print(f"Lap stats: {ls}")
