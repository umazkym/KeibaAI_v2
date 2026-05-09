import os
import sys

import pandas as pd


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from keibaai.src.features.report_metrics import ReportMetricsFeatureGenerator


def _sample_history():
    rows = []
    dates = pd.date_range('2024-01-01', periods=8, freq='14D')
    for i, date in enumerate(dates):
        rows.append({
            'race_id': f'R{i}',
            'horse_id': f'H{i}',
            'race_date': date,
            'venue': '東京',
            'distance_m': 1600,
            'track_surface': '芝',
            'finish_position': 1 if i % 3 == 0 else 2,
            'finish_time_seconds': 96.0 + i * 0.2,
            'last_3f_time': 34.0 + i * 0.05,
        })
    return pd.DataFrame(rows)


def test_metrics_generation_no_future_window():
    df = _sample_history()
    generator = ReportMetricsFeatureGenerator(df)
    annotated = generator.transform(df)

    target = annotated[annotated['race_id'] == 'R6'].iloc[0]
    past = df[pd.to_datetime(df['race_date']) < pd.Timestamp('2024-03-25')]
    expected = past['finish_time_seconds'].mean()

    assert abs(target['standard_time'] - expected) < 1e-9
    assert target['standard_time'] < df.loc[df['race_id'] == 'R7', 'finish_time_seconds'].iloc[0]


def test_annotate_race_metrics_backward_compatibility():
    df = _sample_history()
    generator = ReportMetricsFeatureGenerator(df)
    annotated = generator.annotate_race_metrics()

    expected_cols = {
        'standard_time',
        'track_bias',
        'time_deviation_score',
        'standard_3f',
        'l3f_deviation_score',
    }
    assert expected_cols.issubset(annotated.columns)


if __name__ == "__main__":
    test_metrics_generation_no_future_window()
    test_annotate_race_metrics_backward_compatibility()
    print("report_metrics consistency checks passed")
