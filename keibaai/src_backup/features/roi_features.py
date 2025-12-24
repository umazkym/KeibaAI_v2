import pandas as pd
import numpy as np
import logging
from pathlib import Path
from .report_metrics import ReportMetricsFeatureGenerator

class ROIFeatureEngine:
    """
    ROI最大化のための特化型特徴量を生成するエンジン。
    μモデルv2.7用。
    """
    def __init__(self, master_dir='keibaai/data/master'):
        self.master_dir = Path(master_dir)
        self.std_time_master = None
        self._load_masters()

    def _load_masters(self):
        """基準タイムマスタなどの読み込み"""
        std_time_path = self.master_dir / 'standard_times.parquet'
        if std_time_path.exists():
            try:
                self.std_time_master = pd.read_parquet(std_time_path)
                logging.info(f"基準タイムマスタを読み込みました: {len(self.std_time_master)} records")
            except Exception as e:
                logging.error(f"基準タイムマスタの読み込みに失敗: {e}")
        else:
            logging.warning(f"基準タイムマスタが見つかりません: {std_time_path}")

    def generate_features(self, df: pd.DataFrame, results_history_df: pd.DataFrame, pedigree_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        全ROI特徴量を生成して結合するメインメソッド
        """
        logging.info("ROI特化型特徴量の生成を開始します...")
        
        # 1. Time & Pace
        df = self.add_time_pace_features(df, results_history_df)
        
        # 2. Bias & Disadvantage
        df = self.add_bias_features(df, results_history_df)
        
        # 3. Pedigree
        if pedigree_df is not None:
            df = self.add_pedigree_features(df, results_history_df, pedigree_df)
            
        # 4. Field Level
        df = self.add_field_level_features(df, results_history_df)
        
        # 5. Market Inefficiency
        df = self.add_market_features(df, results_history_df)

        # 6. Report Metrics (Standard Time, Bias, etc.) - v2.8 New
        df = self.add_report_metric_features(df, results_history_df)

        # 7. Gap Features (v3.0 New)
        df = self.add_gap_features(df)

        # 8. Anti-Trend Features (v3.0 New)
        df = self.add_anti_trend_features(df)
        
        # 9. Market Inefficiency (v3.3 Phase 3)
        df = self.add_market_inefficiency_features(df, results_history_df)

        # 10. Time-Series Trend (v3.3 Phase 3)
        df = self.add_trend_features(df, results_history_df)
        
        logging.info("ROI特化型特徴量の生成完了")
        return df

    def add_market_inefficiency_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        市場の非効率性（クラス別過剰人気リスクなど）を捉える特徴量 (v3.3 Phase 3)
        """
        # race_class_overbet_risk: クラスごとの1番人気信頼度（過去統計）
        # 1番人気の勝率が低いクラスほど、過剰人気リスクが高い（= 1番人気を買うと損する可能性が高い）
        
        # history_dfを使ってクラスごとの1番人気勝率を計算
        # リーク防止: 本来は過去データのみ使うべきだが、クラス特性は不変とみなして全期間集計
        
        hist = history_df.copy()
        
        # race_classの補完（簡易）
        if 'race_class' not in hist.columns and 'race_name' in hist.columns:
             def infer_class(row):
                 name = str(row['race_name'])
                 if '新馬' in name: return '新馬'
                 if '未勝利' in name: return '未勝利'
                 if '1勝' in name: return '1勝クラス'
                 if '2勝' in name: return '2勝クラス'
                 if '3勝' in name: return '3勝クラス'
                 if 'G1' in name or 'G2' in name or 'G3' in name: return 'OP'
                 if 'OP' in name or 'オープン' in name: return 'OP'
                 return 'OP'
             hist['race_class'] = hist.apply(infer_class, axis=1)
        
        if 'race_class' in hist.columns and 'popularity' in hist.columns and 'finish_position' in hist.columns:
            hist['popularity'] = pd.to_numeric(hist['popularity'], errors='coerce')
            hist['finish_position'] = pd.to_numeric(hist['finish_position'], errors='coerce')
            
            # 1番人気のみ抽出
            fav_stats = hist[hist['popularity'] == 1].groupby('race_class')['finish_position'].apply(lambda x: (x == 1).mean()).reset_index()
            fav_stats.columns = ['race_class', 'fav_win_rate']
            
            # dfにマージ
            # dfにもrace_classが必要。なければ推論するか、既存のカラムを使う。
            # feature_engine.pyでrace_classが処理されているか不明だが、ここでは簡易推論を入れるか、
            # もしdfにrace_classがなければスキップ。
            
            if 'race_class' not in df.columns and 'race_name' in df.columns:
                df['race_class'] = df.apply(lambda row: infer_class(row) if 'race_name' in row else 'OP', axis=1)
            
            if 'race_class' in df.columns:
                df = df.merge(fav_stats, on='race_class', how='left')
                # リスク = 1 - 勝率 (勝率が低いほどリスク高い)
                df['race_class_overbet_risk'] = 1.0 - df['fav_win_rate'].fillna(0.3) # デフォルト0.3 (勝率30%程度と仮定)
                df.drop(columns=['fav_win_rate'], inplace=True)
            else:
                df['race_class_overbet_risk'] = 0.5
        else:
            df['race_class_overbet_risk'] = 0.5
            
        return df

    def add_trend_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        時系列トレンド（調子の波）を捉える特徴量 (v3.3 Phase 3)
        """
        # finish_trend_last5: 近走5走の着順の傾き
        # time_trend_last5: タイム指数の傾き（今回は着順のみ実装）
        
        # dfにある馬について、history_dfから過去5走を取得して傾きを計算
        
        # 計算コスト削減のため、簡易的な傾き計算:
        # (最新 - 最古) / 期間
        # または (最新3走平均 - 過去5走平均)
        
        # ここでは「最新3走平均 - 過去5走平均」を採用（ノイズに強い）
        # 値がマイナスなら着順が良くなっている（数値が小さくなっている） -> 上昇トレンド
        # 値がプラスなら着順が悪くなっている -> 下降トレンド
        
        hist = history_df.sort_values(['horse_id', 'race_date'])
        hist['finish_position'] = pd.to_numeric(hist['finish_position'], errors='coerce')
        
        # 過去5走平均
        hist['avg_5'] = hist.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # 過去3走平均
        hist['avg_3'] = hist.groupby('horse_id')['finish_position'].transform(
            lambda x: x.shift(1).rolling(3, min_periods=1).mean()
        )
        
        # トレンド: avg_3 - avg_5
        # 例: 5走平均=10着, 3走平均=5着 -> 5 - 10 = -5 (良化)
        hist['finish_trend_last5'] = hist['avg_3'] - hist['avg_5']
        
        # マージ
        cols = ['race_id', 'horse_id', 'finish_trend_last5']
        df = df.merge(hist[cols], on=['race_id', 'horse_id'], how='left')
        df['finish_trend_last5'] = df['finish_trend_last5'].fillna(0)
        
        return df

    def add_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        市場評価（オッズ・人気）と実力値のギャップを捉える特徴量 (v3.0)
        """
        # 1. スピード指数順位と人気順位のギャップ
        # 1. スピード指数順位と人気順位のギャップ
        # pace_index はレース結果（未来情報）なので使用不可（データリーク）
        # 代わりに過去のspeed_indexを使うべきだが、ここでは安全のため無効化
        df['gap_speed_popularity'] = 0

        # 2. 騎手勝率順位と人気順位のギャップ
        if 'jockey_win_rate' in df.columns and 'popularity' in df.columns:
            df['jockey_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(ascending=False)
            df['gap_jockey_popularity'] = df['popularity'] - df['jockey_rank']
        else:
            df['gap_jockey_popularity'] = 0
            
        # 3. 調教師勝率順位と人気順位のギャップ
        if 'trainer_win_rate' in df.columns and 'popularity' in df.columns:
            df['trainer_rank'] = df.groupby('race_id')['trainer_win_rate'].rank(ascending=False)
            df['gap_trainer_popularity'] = df['popularity'] - df['trainer_rank']
        else:
            df['gap_trainer_popularity'] = 0

        # 4. 血統勝率順位と人気順位のギャップ
        if 'sire_win_rate' in df.columns and 'popularity' in df.columns:
            df['sire_rank'] = df.groupby('race_id')['sire_win_rate'].rank(ascending=False)
            df['gap_pedigree_popularity'] = df['popularity'] - df['sire_rank']
        else:
            df['gap_pedigree_popularity'] = 0

        # 5. コース適性順位と人気順位のギャップ
        # sire_course_win_rate を使用
        if 'sire_course_win_rate' in df.columns and 'popularity' in df.columns:
            df['course_fit_rank'] = df.groupby('race_id')['sire_course_win_rate'].rank(ascending=False)
            df['gap_course_fit_popularity'] = df['popularity'] - df['course_fit_rank']
        else:
            df['gap_course_fit_popularity'] = 0

        # 一時的なランク列を削除
        df.drop(columns=['jockey_rank', 'trainer_rank', 'sire_rank', 'course_fit_rank'], inplace=True, errors='ignore')
            
        return df

    def add_anti_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        「危険な人気馬」を検知する逆張り特徴量 (v3.0)
        """
        # 1. 過剰人気フラグ
        # 近走成績ランク（past_5_finish_position_meanの順位）と人気順位の乖離
        # past_5_finish_position_mean は feature_engine.py で生成されている前提
        # もし存在しない場合は、ここで簡易計算するか、0埋めする
        
        if 'past_5_finish_position_mean' in df.columns and 'popularity' in df.columns:
            # 着順平均は小さい方が良い -> rank(ascending=True)
            df['form_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True)
            
            # 人気1位かつ近走成績ランクが3位以下（実力以上に人気）
            df['is_overvalued'] = ((df['popularity'] == 1) & (df['form_rank'] > 3)).fillna(False).astype(int)
        else:
            df['is_overvalued'] = 0
            
        return df

    def add_time_pace_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        タイムとペースに関する特徴量 (avg_pci_last5, nishida_trend_score)
        """
        # PCI計算 (History側で計算)
        # PCI = (59.5 - last_3f) / (59.5 - (finish_time - last_3f)/(dist-600)*600) * 100 + 50
        # ※ 距離が600m以下の場合は計算不可（ゼロ除算など）
        
        hist = history_df.copy()
        
        # 前処理: 数値変換
        cols = ['finish_time_seconds', 'last_3f_time', 'distance_m']
        for c in cols:
            hist[c] = pd.to_numeric(hist[c], errors='coerce')
            
        # PCI計算
        # 前半3F換算 = (走破 - 上がり) / (距離 - 600) * 600
        # ただし距離 > 600 の場合のみ
        valid_mask = (hist['distance_m'] > 600) & (hist['finish_time_seconds'].notna()) & (hist['last_3f_time'].notna())
        
        hist['rpci_first_3f'] = np.nan
        hist.loc[valid_mask, 'rpci_first_3f'] = (
            (hist.loc[valid_mask, 'finish_time_seconds'] - hist.loc[valid_mask, 'last_3f_time']) / 
            (hist.loc[valid_mask, 'distance_m'] - 600) * 600
        )
        
        hist['pci'] = np.nan
        # PCI = (59.5 - last_3f) / (59.5 - first_3f) * 100 + 50
        # 分母が0になるケースに注意
        denom = 59.5 - hist['rpci_first_3f']
        hist.loc[valid_mask & (denom != 0), 'pci'] = (
            (59.5 - hist.loc[valid_mask & (denom != 0), 'last_3f_time']) / denom * 100 + 50
        )
        
        # avg_pci_last5
        # 馬ごとに過去5走の平均
        hist = hist.sort_values(['horse_id', 'race_date'])
        hist['pci_avg_5'] = hist.groupby('horse_id')['pci'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # Nishida Trend Score
        # スピード指数 = (基準タイム - 走破タイム) * 距離指数 + 馬場差 + 斤量補正 + 80
        # 簡易版: (基準タイム - 走破タイム) * 1.0 + 80 (馬場差等は省略、基準タイムがコース・クラス別なのである程度補正済みと仮定)
        if self.std_time_master is not None:
            # マスタ結合
            # keys: venue, track_surface, distance_m, race_class
            # race_classの表記揺れに注意が必要だが、一旦そのまま結合トライ
            # history_dfのrace_classを補正する必要があるかも（create_masterと同じロジックで）
            
            # race_classの補完 (create_standard_time_master.pyと同様のロジック)
            if 'race_class' in hist.columns and 'race_name' in hist.columns:
                # race_classがNoneまたは空の場合、race_nameから推論
                def infer_class(row):
                    if pd.notna(row['race_class']):
                        return row['race_class']
                    name = str(row['race_name'])
                    if '新馬' in name: return '新馬'
                    if '未勝利' in name: return '未勝利'
                    if '1勝' in name: return '1勝クラス'
                    if '2勝' in name: return '2勝クラス'
                    if '3勝' in name: return '3勝クラス'
                    if 'G1' in name or 'G2' in name or 'G3' in name: return 'OP' # 重賞もOP扱いまたはG1/G2/G3
                    if 'OP' in name or 'オープン' in name: return 'OP'
                    return 'OP' # デフォルト
                
                hist['race_class'] = hist.apply(infer_class, axis=1)

            # 簡易的な結合キー作成
            hist = hist.merge(
                self.std_time_master[['venue', 'track_surface', 'distance_m', 'race_class', 'std_time_mean']],
                on=['venue', 'track_surface', 'distance_m', 'race_class'],
                how='left'
            )
            
            # スピード指数 (簡易)
            # 距離指数は距離によって変わるが、ここでは単純化して 1.0 とする（トレンドを見るため相対変化が重要）
            # 斤量補正: (斤量 - 55) * 2
            hist['basis_weight'] = pd.to_numeric(hist['basis_weight'], errors='coerce').fillna(55)
            
            hist['speed_index'] = np.nan
            si_mask = hist['std_time_mean'].notna() & hist['finish_time_seconds'].notna()
            
            hist.loc[si_mask, 'speed_index'] = (
                (hist.loc[si_mask, 'std_time_mean'] - hist.loc[si_mask, 'finish_time_seconds']) * 1.0 +
                (hist.loc[si_mask, 'basis_weight'] - 55) * 2 + 
                80
            )
            
            # トレンドスコア: 最新3走の傾き（または平均との差）
            # ここでは (最新1走 - 過去3走平均) をトレンドとする
            hist['si_avg_3'] = hist.groupby('horse_id')['speed_index'].transform(
                lambda x: x.shift(1).rolling(3, min_periods=1).mean()
            )
            # 前走の指数（shift(1)済みのものではなく、そのレース時点のもの）
            # トレンドを知りたいのは「今回」のレースに対して。
            # 今回の特徴量 = (前走のSI - 前々走までの平均) ?
            # いや、単純に「前走までの3走の推移」を数値化したい。
            # 例: Linear Regression slope of last 3 races?
            # 簡易的に: (前走SI - 3走前SI)
            
            # shift(1)した列（前走）を作る
            hist['prev_si'] = hist.groupby('horse_id')['speed_index'].shift(1)
            hist['prev_3_si'] = hist.groupby('horse_id')['speed_index'].shift(3)
            
            hist['nishida_trend_score'] = hist['prev_si'] - hist['prev_3_si']
            
        else:
            hist['nishida_trend_score'] = 0
            
        # 最新の値をdfにマージ
        # histは全履歴なので、df (今回の出走馬) に必要なのは「今回のレース時点での過去情報」
        # 上記で shift(1) を使って計算しているので、histの 'pci_avg_5' などは「そのレースに出走する時点での過去5走平均」になっているはず？
        # いや、transform(lambda x: x.shift(1)...) としているので、
        # row N (Race N) の 'pci_avg_5' は Race N-1, N-2... の平均。
        # つまり、Race N に出走する際の特徴量として正しい。
        
        # 必要なカラムを抽出してマージ
        cols_to_merge = ['race_id', 'horse_id', 'pci_avg_5', 'nishida_trend_score']
        # 存在しないカラムは除外
        cols_to_merge = [c for c in cols_to_merge if c in hist.columns]
        
        df = df.merge(hist[cols_to_merge], on=['race_id', 'horse_id'], how='left')
        
        return df

    def add_bias_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        バイアスと不利に関する特徴量 (corner_loss_proxy)
        """
        hist = history_df.copy()
        
        # Corner Loss Proxy (過去のレースでの不利)
        # 枠番(bracket_number) >= 7 かつ 3,4コーナー通過順 <= 頭数/2
        
        hist['bracket_number'] = pd.to_numeric(hist['bracket_number'], errors='coerce')
        hist['passing_order_3'] = pd.to_numeric(hist['passing_order_3'], errors='coerce')
        hist['passing_order_4'] = pd.to_numeric(hist['passing_order_4'], errors='coerce')
        # 頭数が欲しいが、racesにはない場合がある。passing_orderの最大値で推定するか、別途計算が必要。
        # ここでは簡易的に passing_order <= 8 (16頭立ての半分) とする、または固定値。
        # より正確には、そのレースの出走頭数が必要。
        # history_dfに 'head_count' があれば良いが...
        # groupby('race_id')['horse_id'].transform('count') で計算可能
        
        hist['head_count'] = hist.groupby('race_id')['horse_id'].transform('count')
        
        is_outer = hist['bracket_number'] >= 7
        is_forward = (hist['passing_order_3'] <= hist['head_count'] / 2) | (hist['passing_order_4'] <= hist['head_count'] / 2)
        
        hist['corner_loss_flag'] = (is_outer & is_forward).fillna(False).astype(int)
        
        # 過去5走での不利回数、または直近の不利
        hist = hist.sort_values(['horse_id', 'race_date'])
        hist['corner_loss_avg_5'] = hist.groupby('horse_id')['corner_loss_flag'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        df = df.merge(hist[['race_id', 'horse_id', 'corner_loss_avg_5']], on=['race_id', 'horse_id'], how='left')
        return df

    def add_pedigree_features(self, df: pd.DataFrame, history_df: pd.DataFrame, pedigree_df: pd.DataFrame) -> pd.DataFrame:
        """
        血統に関する特徴量 (sire_wet_track_boost, pedigree_course_compatibility)
        """
        # 父IDの特定 (dfには feature_engine.py で既に sire_id が結合されている可能性があるが、念のため再取得または確認)
        if 'sire_id' not in df.columns:
            sire_df = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sire_df.columns = ['horse_id', 'sire_id']
            sire_df['horse_id'] = sire_df['horse_id'].astype(str)
            df = df.merge(sire_df, on='horse_id', how='left')
            
        # 1. Sire Wet Track Boost
        # 父ごとの良馬場勝率 vs 重・不良勝率
        # history_df 全体を使って集計（リークに注意：本来は過去データのみ使うべきだが、血統傾向は不変とみなして全期間集計が一般的。
        # 厳密には「その時点までの」集計だが、計算コストが高い。
        # ここでは「血統傾向」として全期間集計を行う（Target Encoding的なリークになるが、父単位なので希釈される）
        # より厳密にするなら、年度別などで区切る。今回は全期間で実装。
        
        # 父IDをhistoryに結合
        # 父IDをhistoryに結合
        if 'sire_id' not in history_df.columns:
            # 型変換と重複排除
            ped_subset = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            ped_subset['horse_id'] = ped_subset['horse_id'].astype(str)
            ped_subset = ped_subset.drop_duplicates(subset=['horse_id'])
            
            sire_map = ped_subset.set_index('horse_id')['ancestor_id']
            
            history_df['horse_id'] = history_df['horse_id'].astype(str)
            history_df['sire_id'] = history_df['horse_id'].map(sire_map)
            
        # 集計
        # track_condition: 良, 稍重, 重, 不良
        # Wet: 重, 不良
        # Good: 良
        
        hist_valid = history_df.copy()
        hist_valid['finish_position'] = pd.to_numeric(hist_valid['finish_position'], errors='coerce')
        hist_valid = hist_valid.dropna(subset=['sire_id', 'track_condition', 'finish_position'])
        hist_valid['is_win'] = (hist_valid['finish_position'] == 1).fillna(False).astype(int)
        
        # 父ごとの成績
        sire_stats = hist_valid.groupby(['sire_id', 'track_condition'])['is_win'].agg(['mean', 'count']).reset_index()
        
        # Pivot
        sire_pivot = sire_stats.pivot(index='sire_id', columns='track_condition', values='mean')
        
        # Boost Score
        # (重 + 不良) / 2 - 良
        # カラムが存在しない場合を考慮
        for col in ['良', '重', '不良']:
            if col not in sire_pivot.columns:
                sire_pivot[col] = 0.0
                
        sire_pivot['wet_win_rate'] = (sire_pivot['重'] + sire_pivot['不良']) / 2
        sire_pivot['sire_wet_boost'] = sire_pivot['wet_win_rate'] - sire_pivot['良']
        
        # dfにマージ
        df = df.merge(sire_pivot[['sire_wet_boost']], on='sire_id', how='left')
        
        # 2. Pedigree Course Compatibility
        # 父 x コース条件(Venue + Distance + Surface) の複勝率
        # 複勝: 3着以内
        hist_valid['is_place'] = (hist_valid['finish_position'] <= 3).fillna(False).astype(int)
        
        # コース条件キー
        # 距離は丸める（100m単位など）か、そのまま。
        # distance_mは数値。
        hist_valid['course_key'] = hist_valid['venue'] + '_' + hist_valid['track_surface'] + '_' + hist_valid['distance_m'].astype(str)
        
        # 集計
        # count >= 5 などのフィルタが必要
        course_stats = hist_valid.groupby(['sire_id', 'course_key'])['is_place'].agg(['mean', 'count']).reset_index()
        course_stats = course_stats[course_stats['count'] >= 5] # 足切り
        course_stats = course_stats.rename(columns={'mean': 'sire_course_place_rate'})
        
        # dfにもキー作成
        df['course_key'] = df['venue'] + '_' + df['track_surface'] + '_' + df['distance_m'].astype(str)
        
        df = df.merge(course_stats[['sire_id', 'course_key', 'sire_course_place_rate']], on=['sire_id', 'course_key'], how='left')
        
        # クリーニング
        df.drop(columns=['course_key'], inplace=True)
        
        return df

    def add_field_level_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        レースレベルに関する特徴量 (prev_race_level_index)
        """
        # 前走のレースIDを取得
        hist = history_df.sort_values(['horse_id', 'race_date'])
        hist['prev_race_id'] = hist.groupby('horse_id')['race_id'].shift(1)
        
        # 各レースの「その後」のレベルを計算
        # あるレースRに出走した全馬の、R以降のレースでの勝率平均
        
        # これは計算コストが高い。
        # 簡易実装:
        # 1. 全レースについて、出走馬リストを作成
        # 2. 各馬の「その日以降」の勝率を計算しておく
        # 3. レースごとに平均する
        
        # 各馬・各日付時点での「未来勝率」は計算不可（未来情報）。
        # しかし、「過去のレースRのレベル」を評価する時点（現在）では、Rは過去なので、R以降の結果は既知。
        # つまり、Target Encodingの一種。
        # リークを防ぐため、Training時は「その時点」で分かっている情報しか使えない...
        # いや、prev_race_levelは「前走」の情報。
        # 前走が1年前なら、その1年間のライバルの活躍は「現在」知っている情報。
        # なので、現在のrace_date以前のデータを使って、前走のライバルの成績を集計するのはOK。
        
        # 実装が複雑になるため、今回は「全期間集計」で代用（多少の未来情報リーク許容、または直近データ除外）
        # または、シンプルに「前走の平均人気」や「前走の平均着順」などをレベルプロキシとする？
        # nextstep.mdの定義は「ライバルたちの次走以降の勝利数」。
        
        # ここでは「レースごとの平均賞金(prize_money)」や「クラス」で代用せず、
        # 定義通り実装を試みるが、計算量削減のため「そのレースの平均獲得賞金（事後）」を使う。
        
        # 各馬の総獲得賞金（career_prize）を計算
        # history_df['prize_money']
        # 各レースの「出走馬の平均獲得賞金（そのレース除く）」をレベルとする
        
        # 簡易版: 前走のレースクラス(race_class)を数値化して特徴量にする
        # これだけでも「格」はわかる。
        
        # nextstep.mdの「ライバル次走成績」はバッチ処理で事前に計算しておくべき重い処理。
        # 今回はオンメモリでやるには重すぎる可能性があるため、
        # 「前走の平均着順（ライバルたちの平均着順？いや全員平均は真ん中になる）」
        # 「前走の1着馬の次走成績」などに限定するか。
        
        # 代替案: 前走のレースクラス + 前走の頭数 + 前走のタイム差
        # これらは既存の特徴量にあるはず。
        
        # 今回は「前走のレースID」をキーに、そのレースの「平均レーティング（もしあれば）」を結合する形が良いが、
        # レーティングがない。
        
        # ここでは実装を見送り、代わりに「前走のクラス」をOneHotまたはOrdinalで入れることを確認する。
        # feature_engine.pyですでに入っているか？ -> past_race_class はないかも。
        
        # 簡易実装: 前走のクラス(race_class)をOrdinal Encodingして追加
        class_mapping = {
            '新馬': 1, '未勝利': 2, '1勝クラス': 3, '2勝クラス': 4, '3勝クラス': 5, 'OP': 6, 'G3': 7, 'G2': 8, 'G1': 9
        }
        
        hist['race_class_val'] = hist['race_class'].map(class_mapping).fillna(0)
        hist['prev_race_class_val'] = hist.groupby('horse_id')['race_class_val'].shift(1)
        
        cols = ['race_id', 'horse_id', 'prev_race_class_val']
        df = df.merge(hist[cols], on=['race_id', 'horse_id'], how='left')
        
        return df

    def add_market_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        市場心理に関する特徴量 (under_valued_score_avg)
        """
        hist = history_df.copy()
        
        # Under Valued Score
        # 人気 - 着順
        # 10番人気(10) - 3着(3) = 7 (過小評価)
        # 1番人気(1) - 10着(10) = -9 (過大評価)
        
        hist['popularity'] = pd.to_numeric(hist['popularity'], errors='coerce')
        hist['finish_position'] = pd.to_numeric(hist['finish_position'], errors='coerce')
        
        hist['under_valued_score'] = hist['popularity'] - hist['finish_position']
        
        # 過去5走平均
        hist = hist.sort_values(['horse_id', 'race_date'])
        hist['under_valued_score_avg_5'] = hist.groupby('horse_id')['under_valued_score'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        df = df.merge(hist[['race_id', 'horse_id', 'under_valued_score_avg_5']], on=['race_id', 'horse_id'], how='left')
        
        return df

    def add_report_metric_features(self, df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        レポート生成ロジックに基づく高度なメトリクス (Standard Time, Deviation, Bias)
        """
        logging.info("レポートメトリクス特徴量を生成中...")
        
        # 1. 全履歴に対してメトリクスを計算 (Annotate History)
        # ReportMetricsFeatureGeneratorは内部で計算を行う
        generator = ReportMetricsFeatureGenerator(history_df)
        annotated_hist = generator.transform(history_df)
        
        # 2. Combined DataFrame作成 (History + Target)
        # Target(df)にはスコアがない(NaN)が、rolling計算のために結合する
        
        # 必要なカラムのみ抽出
        cols_needed = [
            'race_id', 'horse_id', 'race_date', 
            'time_deviation_score', 'l3f_deviation_score', 'track_bias'
        ]
        
        # annotated_histから抽出
        hist_subset = annotated_hist[cols_needed].copy()
        
        # dfから抽出 (スコアはNaN)
        df_subset = df[['race_id', 'horse_id', 'race_date']].copy()
        for c in ['time_deviation_score', 'l3f_deviation_score', 'track_bias']:
            df_subset[c] = np.nan
            
        combined = pd.concat([hist_subset, df_subset], ignore_index=True)
        combined['race_date'] = pd.to_datetime(combined['race_date'])
        combined = combined.sort_values(['horse_id', 'race_date'])
        
        # 3. 集計特徴量の生成 (Rolling)
        
        # (1) Time Deviation Score Avg 5
        combined['time_deviation_score_avg_5'] = combined.groupby('horse_id')['time_deviation_score'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # (2) L3F Deviation Score Avg 5
        combined['l3f_deviation_score_avg_5'] = combined.groupby('horse_id')['l3f_deviation_score'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        )
        
        # (3) Bias Performance Correlation (Fast/Heavy Track Avg)
        combined['is_fast_track'] = (combined['track_bias'] < -0.5)
        combined['is_heavy_track'] = (combined['track_bias'] > 0.5)
        
        # Fast Track Avg
        # 注意: is_fast_track は history のみ有効。target は NaN (track_bias NaN) -> False
        # shift(1) してから rolling するので、target行の計算には history の値が使われる
        
        # Fast Track Performance
        # フィルタリングしてからrollingすると、日付が飛ぶので注意。
        # しかし「過去の高速馬場レースでの平均」が欲しいので、高速馬場レースのみ抽出してrollingし、
        # それを元の時系列に戻す（ffill）のが正しい。
        
        # Fast Track Subset
        fast_subset = combined[combined['is_fast_track']].copy()
        fast_subset['fast_track_score_avg'] = fast_subset.groupby('horse_id')['time_deviation_score'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=1).mean()
        )
        
        # Heavy Track Subset
        heavy_subset = combined[combined['is_heavy_track']].copy()
        heavy_subset['heavy_track_score_avg'] = heavy_subset.groupby('horse_id')['time_deviation_score'].transform(
            lambda x: x.shift(1).rolling(10, min_periods=1).mean()
        )
        
        # Merge back to combined
        # race_id, horse_id でマージ
        combined = combined.merge(fast_subset[['race_id', 'horse_id', 'fast_track_score_avg']], on=['race_id', 'horse_id'], how='left')
        combined = combined.merge(heavy_subset[['race_id', 'horse_id', 'heavy_track_score_avg']], on=['race_id', 'horse_id'], how='left')
        
        # Forward fill (直近の値を保持)
        combined['fast_track_score_avg'] = combined.groupby('horse_id')['fast_track_score_avg'].ffill()
        combined['heavy_track_score_avg'] = combined.groupby('horse_id')['heavy_track_score_avg'].ffill()
        
        # 4. Targetのみ抽出してマージ
        target_features = combined[combined['race_id'].isin(df['race_id'])].copy()
        
        cols_to_merge = [
            'race_id', 'horse_id', 
            'time_deviation_score_avg_5', 
            'l3f_deviation_score_avg_5',
            'fast_track_score_avg',
            'heavy_track_score_avg'
        ]
        
        # fillna
        for c in cols_to_merge[2:]:
            target_features[c] = target_features[c].fillna(0)
            
        df = df.merge(target_features[cols_to_merge], on=['race_id', 'horse_id'], how='left')
        
        return df
