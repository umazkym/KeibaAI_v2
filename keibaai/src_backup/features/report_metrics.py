import pandas as pd
import numpy as np
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

class ReportMetricsFeatureGenerator:
    def __init__(self, history_df: pd.DataFrame):
        """
        Args:
            history_df: DataFrame containing PAST race results.
                        Used to learn standard times and regression models.
        """
        self.history_df = history_df.copy()
        self.history_df['race_date'] = pd.to_datetime(self.history_df['race_date'])
        self._prepare_lookups()

    def _prepare_lookups(self):
        """Pre-calculates static lookups and groups data from HISTORY."""
        logger.info("Preparing metric lookups from history...")
        
        # Filter for 1-3rd place for standard time calculation
        df_top3 = self.history_df[self.history_df['finish_position'].isin([1, 2, 3])].copy()
        
        # 1. Static Averages (Venue, Dist, Surf)
        self.avg_time_lookup = df_top3.groupby(['venue', 'distance_m', 'track_surface'])['finish_time_seconds'].mean().to_dict()
        
        # 2. Group Data for Time-Dependent Queries
        self.grouped_data = {}
        for name, group in df_top3.groupby(['venue', 'distance_m', 'track_surface']):
            self.grouped_data[name] = group.sort_values('race_date')
            
        # 3. Regression Models
        self.regression_models = {}
        for name, group in self.grouped_data.items():
            self._fit_regression(name, group)

    def _fit_regression(self, key, group):
        """Fits a linear regression model for 3F prediction."""
        if len(group) < 2:
            return

        try:
            # X = (Distance - 600) / (Finish Time - Last 3F)
            # Y = Last 3F
            dist_minus_600 = group['distance_m'] - 600
            time_minus_3f = group['finish_time_seconds'] - group['last_3f_time']
            
            valid_mask = (time_minus_3f > 0) & (dist_minus_600 > 0) & (group['last_3f_time'] > 0)
            if not valid_mask.any():
                return

            X = (dist_minus_600[valid_mask] / time_minus_3f[valid_mask]).values
            Y = group.loc[valid_mask, 'last_3f_time'].values

            if len(X) < 2:
                return

            slope, intercept = np.polyfit(X, Y, 1)
            self.regression_models[key] = (slope, intercept)
        except Exception as e:
            pass

    def calculate_standard_time(self, row, window_days=3):
        """
        Calculates Standard Time using HISTORY data around the row's date.
        """
        key = (row['venue'], row['distance_m'], row['track_surface'])
        group = self.grouped_data.get(key)
        
        if group is None or group.empty:
            return np.nan
            
        date_obj = row['race_date']
        start_date = date_obj - timedelta(days=window_days)
        end_date = date_obj + timedelta(days=window_days)
        
        # Look for races in history within window
        mask = (group['race_date'] >= start_date) & (group['race_date'] <= end_date)
        sub_group = group[mask]
        
        if sub_group.empty:
            return np.nan
            
        return sub_group['finish_time_seconds'].mean()

    def predict_standard_3f(self, row):
        """Predicts Standard 3F based on regression models learned from HISTORY."""
        key = (row['venue'], row['distance_m'], row['track_surface'])
        model = self.regression_models.get(key)
        if not model:
            return np.nan
        
        slope, intercept = model
        dist = row['distance_m']
        finish_time = row['finish_time_seconds']
        last_3f = row['last_3f_time']
        
        if pd.isna(finish_time) or pd.isna(last_3f) or (finish_time - last_3f <= 0):
            return np.nan
            
        try:
            X = (dist - 600) / (finish_time - last_3f)
            predicted_3f = slope * X + intercept
            return predicted_3f
        except:
            return np.nan

    def transform(self, target_df: pd.DataFrame) -> pd.DataFrame:
        """
        Annotates the target_df with calculated metrics using history lookups.
        """
        logger.info("Annotating race metrics for target dataframe...")
        df = target_df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])

        # 1. Global Average Time
        df['global_avg_time'] = df.apply(
            lambda x: self.avg_time_lookup.get((x['venue'], x['distance_m'], x['track_surface']), np.nan), axis=1
        )
        
        # 2. Standard Time (Local Window)
        unique_conditions = df[['venue', 'distance_m', 'track_surface', 'race_date']].drop_duplicates()
        unique_conditions['standard_time'] = unique_conditions.apply(
            lambda x: self.calculate_standard_time(x), axis=1
        )
        df = pd.merge(df, unique_conditions, on=['venue', 'distance_m', 'track_surface', 'race_date'], how='left')
        
        # 3. Track Bias
        df['track_bias'] = df['standard_time'] - df['global_avg_time']
        
        # 4. Standard 3F
        df['standard_3f'] = df.apply(lambda x: self.predict_standard_3f(x), axis=1)
        
        # 5. Deviation Scores
        df['time_deviation_score'] = (df['standard_time'] - df['finish_time_seconds']) / df['standard_time']
        df['l3f_deviation_score'] = (df['standard_3f'] - df['last_3f_time']) / df['standard_3f']
        
        return df
