# src/features/time_series_features.py (新規)

class TimeSeriesFeatureEngine:
    """時系列パターンを捉える特徴量"""
    
    def generate_form_cycle_features(
        self, 
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """フォームサイクルの検出"""
        
        for horse_id in performance_df['horse_id'].unique():
            horse_data = performance_df[
                performance_df['horse_id'] == horse_id
            ].sort_values('race_date')
            
            if len(horse_data) < 5:
                continue
            
            # ローリング平均での好調・不調の波
            horse_data['finish_rolling_mean'] = \
                horse_data['finish_position'].rolling(3, min_periods=1).mean()
            
            # 波のピーク・ボトムを検出
            horse_data['form_derivative'] = \
                horse_data['finish_rolling_mean'].diff()
            
            # 現在のフォーム状態
            latest_form = horse_data['form_derivative'].iloc[-1]
            if latest_form < -0.5:
                form_state = 'improving'
            elif latest_form > 0.5:
                form_state = 'declining'
            else:
                form_state = 'stable'
            
            performance_df.loc[
                performance_df['horse_id'] == horse_id, 
                'form_state'
            ] = form_state
        
        return performance_df
    
    def calculate_growth_curve(
        self,
        df: pd.DataFrame,
        performance_df: pd.DataFrame
    ) -> pd.DataFrame:
        """若馬の成長曲線"""
        
        young_horses = df[df['age'] <= 3]['horse_id'].unique()
        
        for horse_id in young_horses:
            horse_perf = performance_df[
                performance_df['horse_id'] == horse_id
            ].sort_values('race_date')
            
            if len(horse_perf) < 3:
                continue
            
            # 走数に対する着順の回帰
            x = np.arange(len(horse_perf))
            y = horse_perf['finish_position'].values
            
            # 簡易的な線形回帰
            if len(x) > 1:
                slope, intercept = np.polyfit(x, y, 1)
                df.loc[df['horse_id'] == horse_id, 'growth_slope'] = slope
                
                # 成長の安定性（残差の標準偏差）
                predicted = slope * x + intercept
                residuals = y - predicted
                df.loc[df['horse_id'] == horse_id, 'growth_stability'] = \
                    np.std(residuals)
        
        return df