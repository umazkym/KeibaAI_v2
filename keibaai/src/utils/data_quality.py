# src/utils/data_quality.py (新規)

class DataQualityChecker:
    """データ品質チェックと自動修正"""
    
    def validate_and_fix_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """データ検証と修正"""
        
        # 1. 論理的整合性チェック
        # 着順は出走頭数以下
        mask = df['finish_position'] > df['head_count']
        if mask.any():
            logging.warning(f"{mask.sum()}件の着順異常を検出")
            df.loc[mask, 'finish_position'] = None
        
        # 2. 外れ値の検出と処理
        # タイムの異常値（3σ以上）
        if 'finish_time_seconds' in df.columns:
            grouped = df.groupby(['distance_m', 'track_surface'])
            for name, group in grouped:
                mean_time = group['finish_time_seconds'].mean()
                std_time = group['finish_time_seconds'].std()
                lower_bound = mean_time - 3 * std_time
                upper_bound = mean_time + 3 * std_time
                
                mask = (group['finish_time_seconds'] < lower_bound) | \
                       (group['finish_time_seconds'] > upper_bound)
                if mask.any():
                    df.loc[group[mask].index, 'time_outlier_flag'] = True
        
        # 3. 欠損パターンの分析
        missing_patterns = df.isnull().sum()
        logging.info(f"欠損値パターン:\n{missing_patterns[missing_patterns > 0]}")
        
        return df
    
    def handle_regional_races(self, df: pd.DataFrame) -> pd.DataFrame:
        """地方競馬データの特別処理"""
        
        # 地方競馬場の識別
        regional_venues = ['園田', '姫路', '高知', '佐賀', '大井', '川崎', 
                          '浦和', '船橋', '盛岡', '水沢', '金沢', '笠松', 
                          '名古屋', '門別', '帯広']
        
        df['is_regional'] = df['venue'].isin(regional_venues)
        
        # 地方競馬の特徴量調整
        if df['is_regional'].any():
            # 賞金スケールの調整
            df.loc[df['is_regional'], 'prize_scale_factor'] = 0.3
            
            # クラス分けの調整
            df.loc[df['is_regional'] & (df['race_class'] == '未勝利'), 
                   'race_class'] = '地方未勝利'
        
        return df