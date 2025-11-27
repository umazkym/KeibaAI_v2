import pytest
import pandas as pd
import numpy as np
from keibaai.src.features.advanced_features import AdvancedFeatureEngine

class TestAdvancedFeaturesNew:
    @pytest.fixture
    def sample_data(self):
        # Create a sample dataset simulating multiple meetings
        data = {
            'race_id': [
                '202001010101', '202001010102', '202001010201', # Meeting 1, Day 1 & 2
                '202001020101', '202001020102', # Meeting 2 (Seasonal check)
                '202101010101' # Next year same meeting
            ],
            'race_date': pd.to_datetime([
                '2020-01-01', '2020-01-01', '2020-01-02',
                '2020-02-01', '2020-02-01',
                '2021-01-01'
            ]),
            'venue': ['札幌', '札幌', '札幌', '函館', '函館', '札幌'],
            'round_of_year': [1, 1, 1, 1, 1, 1], # 1回札幌, 1回函館
            'day_of_meeting': [1, 1, 2, 1, 1, 1],
            'track_surface': ['芝', '芝', '芝', '芝', '芝', '芝'],
            'distance_m': [1800, 1800, 1800, 1800, 1800, 1800],
            'bracket_number': [1, 8, 1, 1, 8, 1], # Inner vs Outer
            'finish_position': [1, 10, 1, 10, 1, 2], # Inner wins in Sapporo, Outer wins in Hakodate
            'passing_order_1': [1, 10, 1, 10, 1, 1], # Front runner check
            'horse_id': ['H1', 'H2', 'H3', 'H4', 'H5', 'H6']
        }
        df = pd.DataFrame(data)
        # Add distance_category
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 3000, 4000],
            labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
        )
        return df

    def test_seasonal_bias_features(self, sample_data):
        """Test if seasonal bias correctly captures trends per meeting (round_of_year)"""
        engine = AdvancedFeatureEngine()
        
        # Generate features
        # For testing, we use the same df as both 'df' (target) and 'performance_df' (history)
        # In reality, we should be careful about leakage, but seasonal bias is static historical stats.
        # Here we check if it aggregates correctly by group.
        
        result = engine.generate_seasonal_bias_features(sample_data, sample_data)
        
        # Check columns exist
        assert 'bias_seasonal_score' in result.columns
        
        # Sapporo (Venue=札幌, Round=1) Inner(1) should have high win rate (1st, 1st, 2nd -> good avg)
        sapporo_inner = result[
            (result['venue'] == '札幌') & (result['bracket_number'] == 1)
        ]
        # Hakodate (Venue=函館, Round=1) Inner(1) should have low win rate (10th)
        hakodate_inner = result[
            (result['venue'] == '函館') & (result['bracket_number'] == 1)
        ]
        
        # The score is based on average finish position (lower is better)
        # Sapporo Inner Avg Finish: (1+1+2)/3 = 1.33
        # Hakodate Inner Avg Finish: 10.0
        
        # Since we merge back, the values should be correct
        assert sapporo_inner['bias_seasonal_score'].iloc[0] < hakodate_inner['bias_seasonal_score'].iloc[0]

    def test_dynamic_bias_features_leakage(self, sample_data):
        """Test dynamic bias to ensure NO LEAKAGE (Day 1 should not know Day 1 results)"""
        engine = AdvancedFeatureEngine()
        
        # Add year for grouping
        sample_data['year'] = sample_data['race_date'].dt.year
        
        result = engine.generate_dynamic_bias_features(sample_data, sample_data)
        
        # Check Day 1 (2020-01-01)
        day1 = result[
            (result['race_date'] == '2020-01-01') & 
            (result['venue'] == '札幌')
        ]
        # Day 1 should have NaN or default value because there is no "previous day" in this meeting
        assert day1['bias_dynamic_score'].isnull().all() or (day1['bias_dynamic_score'] == 0).all()
        
        # Check Day 2 (2020-01-02)
        day2 = result[
            (result['race_date'] == '2020-01-02') & 
            (result['venue'] == '札幌')
        ]
        # Day 2 should know about Day 1's results
        # Day 1 Inner(1) won (Finish=1). So Day 2 Inner(1) should have a good score.
        assert not day2['bias_dynamic_score'].isnull().all()
        
        # Verify the value reflects Day 1
        # Day 1: Bracket 1 -> Finish 1. Bracket 8 -> Finish 10.
        # Day 2 horse is Bracket 1. Should have favorable score (low avg finish).
        score = day2['bias_dynamic_score'].iloc[0]
        assert score == 1.0 # Average finish of bracket 1 in previous days of this meeting

    def test_pace_features(self):
        """Test pace feature generation"""
        engine = AdvancedFeatureEngine()
        
        # Create a race with 10 horses
        data = {
            'race_id': ['R1'] * 10,
            'horse_id': [f'H{i}' for i in range(10)],
            'passing_order_1': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], # 1-3 are leaders
            'running_style': [1, 1, 2, 2, 3, 3, 3, 4, 4, 4] # 1:Lead, 2:Front, 3:Mid, 4:Back
        }
        df = pd.DataFrame(data)
        
        # We need a history df to calculate 'running_style' if it's not provided, 
        # but here we mock the internal logic or assume running_style is already there 
        # or calculated from passing_order_1 for the test.
        # Let's assume generate_pace_features calculates n_leaders based on passing_order_1 stats
        
        # Mocking necessary columns for the method
        # The method usually calculates 'n_leaders' from the race members' past stats.
        # For this test, we'll assume we pass a df that already has 'early_speed_index' or similar.
        df['early_speed_index'] = [60, 58, 55, 50, 45, 40, 35, 30, 25, 20] # High = Fast
        
        result = engine.generate_pace_features(df)
        
        assert 'n_leaders' in result.columns
        assert 'pace_fit_score' in result.columns
        
        # Check n_leaders count (assuming threshold is e.g. > 55)
        # This depends on the implementation threshold
        assert result['n_leaders'].iloc[0] > 0
