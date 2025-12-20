"""
Parquetファイルのデータ品質を詳細検証するスクリプト

検証項目:
1. 各ファイルの基本統計（件数、カラム数、NULL率）
2. 年別データ分布（2014-2025）
3. 主キーの重複チェック
4. 参照整合性（race_id, horse_id等の一貫性）
5. 値の範囲チェック（異常値検出）
6. 拡張データ（2014-2019）と既存データ（2020-2025）の比較
7. 時系列の連続性確認
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def load_parquet(subdir, filename):
    """Parquetファイルを読み込む"""
    path = DATA_DIR / subdir / filename
    if path.exists():
        return pd.read_parquet(path)
    # ディレクトリ内の全parquetを読む場合
    dir_path = DATA_DIR / subdir
    if dir_path.is_dir():
        files = list(dir_path.glob("*.parquet"))
        if files:
            return pd.read_parquet(files[0])
    return None

def verify_races():
    """races.parquetの検証"""
    print_section("1. races.parquet の詳細検証")
    
    df = load_parquet("races", "races.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム数: {len(df.columns)}")
    print(f"  カラム一覧: {list(df.columns)}")
    
    # 年別分布
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        print(f"\n  年別レコード数:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count:,}件")
    
    # ユニークレース数
    if 'race_id' in df.columns:
        unique_races = df['race_id'].nunique()
        print(f"\n  ユニークレース数: {unique_races:,}")
    
    # NULL率
    print(f"\n  NULL率の高いカラム (>5%):")
    null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    for col, pct in null_pct[null_pct > 5].items():
        print(f"    {col}: {pct:.1f}%")
    
    # 主キー重複チェック
    if 'race_id' in df.columns and 'horse_number' in df.columns:
        dup_count = df.duplicated(subset=['race_id', 'horse_number']).sum()
        print(f"\n  主キー (race_id, horse_number) 重複: {dup_count}件")
        if dup_count > 0:
            print("    ⚠️ 重複データあり！")
            dups = df[df.duplicated(subset=['race_id', 'horse_number'], keep=False)]
            print(f"    サンプル: {dups[['race_id', 'horse_number', 'race_date']].head()}")
    
    # 着順の確認
    if 'finish_position' in df.columns:
        print(f"\n  着順 (finish_position) の分布:")
        print(f"    min: {df['finish_position'].min()}, max: {df['finish_position'].max()}")
        print(f"    NULL数: {df['finish_position'].isnull().sum()}")
        # 異常値
        abnormal = df[df['finish_position'] > 20]
        if len(abnormal) > 0:
            print(f"    ⚠️ 20着超のレコード: {len(abnormal)}件")
    
    # オッズの確認
    if 'win_odds' in df.columns:
        print(f"\n  単勝オッズ (win_odds) の分布:")
        print(f"    min: {df['win_odds'].min()}, max: {df['win_odds'].max()}")
        print(f"    mean: {df['win_odds'].mean():.2f}, median: {df['win_odds'].median():.2f}")
        print(f"    NULL数: {df['win_odds'].isnull().sum()}")
        # 異常値
        abnormal_odds = df[df['win_odds'] > 1000]
        if len(abnormal_odds) > 0:
            print(f"    ⚠️ 1000倍超のオッズ: {len(abnormal_odds)}件")
    
    return df

def verify_corners():
    """corner_positions.parquetの検証"""
    print_section("2. corner_positions.parquet の詳細検証")
    
    df = load_parquet("corners", "corner_positions.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 年別分布（race_idから年を抽出）
    if 'race_id' in df.columns:
        df['year'] = df['race_id'].astype(str).str[:4].astype(int)
        print(f"\n  年別レコード数:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count:,}件")
        
        # ユニークレース数
        unique_races = df['race_id'].nunique()
        print(f"\n  ユニークレース数: {unique_races:,}")
    
    # コーナー番号の分布
    if 'corner' in df.columns:
        print(f"\n  コーナー番号の分布:")
        corner_counts = df['corner'].value_counts().sort_index()
        for corner, count in corner_counts.items():
            print(f"    コーナー{corner}: {count:,}件")
    
    # gap_from_leaderの検証
    if 'gap_from_leader' in df.columns:
        print(f"\n  gap_from_leader の分布:")
        print(f"    min: {df['gap_from_leader'].min():.1f}")
        print(f"    max: {df['gap_from_leader'].max():.1f}")
        print(f"    mean: {df['gap_from_leader'].mean():.2f}")
        print(f"    NULL数: {df['gap_from_leader'].isnull().sum()}")
        # 異常値
        abnormal_gap = df[df['gap_from_leader'] > 50]
        if len(abnormal_gap) > 0:
            print(f"    ⚠️ 50馬身超のギャップ: {len(abnormal_gap)}件")
            print(f"    サンプル:")
            print(abnormal_gap[['race_id', 'corner', 'horse_number', 'gap_from_leader']].head())
    
    # 主キー重複チェック
    if all(c in df.columns for c in ['race_id', 'corner', 'horse_number']):
        dup_count = df.duplicated(subset=['race_id', 'corner', 'horse_number']).sum()
        print(f"\n  主キー (race_id, corner, horse_number) 重複: {dup_count}件")
        if dup_count > 0:
            print("    ⚠️ 重複データあり！")
    
    return df

def verify_race_details():
    """race_details.parquetの検証"""
    print_section("3. race_details.parquet の詳細検証")
    
    df = load_parquet("race_details", "race_details.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 年別分布
    if 'race_id' in df.columns:
        df['year'] = df['race_id'].astype(str).str[:4].astype(int)
        print(f"\n  年別レコード数:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count:,}件")
        
        # 主キー重複チェック
        dup_count = df.duplicated(subset=['race_id']).sum()
        print(f"\n  主キー (race_id) 重複: {dup_count}件")
        if dup_count > 0:
            print("    ⚠️ 重複データあり！")
    
    # ペースデータの検証
    for col in ['first_half', 'second_half']:
        if col in df.columns:
            print(f"\n  {col} の分布:")
            print(f"    min: {df[col].min():.1f}, max: {df[col].max():.1f}")
            print(f"    mean: {df[col].mean():.2f}")
            print(f"    NULL数: {df[col].isnull().sum()} ({df[col].isnull().sum()/len(df)*100:.1f}%)")
            # 異常値
            abnormal = df[(df[col] < 20) | (df[col] > 80)]
            if len(abnormal) > 0:
                print(f"    ⚠️ 異常値（20秒未満 or 80秒超）: {len(abnormal)}件")
    
    # corner_X_rawの確認
    for i in range(1, 5):
        col = f'corner_{i}_raw'
        if col in df.columns:
            null_pct = df[col].isnull().sum() / len(df) * 100
            print(f"\n  {col}: NULL率 {null_pct:.1f}%")
    
    return df

def verify_returns():
    """returns.parquetの検証"""
    print_section("4. returns.parquet の詳細検証")
    
    df = load_parquet("returns", "returns.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 年別分布
    if 'race_id' in df.columns:
        df['year'] = df['race_id'].astype(str).str[:4].astype(int)
        print(f"\n  年別レコード数:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count:,}件")
        
        # ユニークレース数
        unique_races = df['race_id'].nunique()
        print(f"\n  ユニークレース数: {unique_races:,}")
    
    # bet_typeの分布
    if 'bet_type' in df.columns:
        print(f"\n  bet_type の分布:")
        bet_counts = df['bet_type'].value_counts()
        for bet_type, count in bet_counts.items():
            print(f"    {bet_type}: {count:,}件")
    
    # payoutの検証
    if 'payout' in df.columns:
        print(f"\n  payout の分布:")
        print(f"    min: {df['payout'].min()}")
        print(f"    max: {df['payout'].max()}")
        print(f"    mean: {df['payout'].mean():.0f}")
        print(f"    NULL数: {df['payout'].isnull().sum()}")
        # 異常値
        abnormal = df[df['payout'] > 10000000]  # 1000万円超
        if len(abnormal) > 0:
            print(f"    ⚠️ 1000万円超の払戻: {len(abnormal)}件")
    
    return df

def verify_horses():
    """horses.parquetの検証"""
    print_section("5. horses.parquet の詳細検証")
    
    df = load_parquet("horses", "horses.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # horse_id重複チェック
    if 'horse_id' in df.columns:
        dup_count = df.duplicated(subset=['horse_id']).sum()
        print(f"\n  主キー (horse_id) 重複: {dup_count}件")
        if dup_count > 0:
            print("    ⚠️ 重複データあり！")
    
    # NULL率
    print(f"\n  NULL率:")
    null_pct = (df.isnull().sum() / len(df) * 100)
    for col, pct in null_pct.items():
        print(f"    {col}: {pct:.1f}%")
    
    return df

def verify_pedigrees():
    """pedigrees.parquetの検証"""
    print_section("6. pedigrees.parquet の詳細検証")
    
    df = load_parquet("pedigrees", "pedigrees.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # ユニーク馬数
    if 'horse_id' in df.columns:
        unique_horses = df['horse_id'].nunique()
        print(f"\n  ユニーク馬数: {unique_horses:,}")
    
    # 世代の分布
    if 'generation' in df.columns:
        print(f"\n  世代 (generation) の分布:")
        gen_counts = df['generation'].value_counts().sort_index()
        for gen, count in gen_counts.items():
            print(f"    世代{gen}: {count:,}件")
    
    return df

def verify_shutuba():
    """shutuba.parquetの検証"""
    print_section("7. shutuba.parquet の詳細検証")
    
    df = load_parquet("shutuba", "shutuba.parquet")
    if df is None:
        print("  ❌ ファイルが見つかりません")
        return None
    
    print(f"\n  総レコード数: {len(df):,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 年別分布
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        print(f"\n  年別レコード数:")
        year_counts = df.groupby('year').size()
        for year, count in year_counts.items():
            print(f"    {year}: {count:,}件")
    
    # NULL率
    print(f"\n  NULL率:")
    null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
    for col, pct in null_pct.items():
        if pct > 0:
            print(f"    {col}: {pct:.1f}%")
    
    return df

def cross_validate_data(races_df, corners_df, returns_df, race_details_df):
    """データ間の整合性チェック"""
    print_section("8. データ間の整合性チェック")
    
    if races_df is None:
        print("  races.parquetが読み込めていないためスキップ")
        return
    
    # racesのユニークrace_id
    races_race_ids = set(races_df['race_id'].unique())
    print(f"\n  races.parquetのユニークrace_id数: {len(races_race_ids):,}")
    
    # cornersとの照合
    if corners_df is not None:
        corners_race_ids = set(corners_df['race_id'].unique())
        print(f"  corners.parquetのユニークrace_id数: {len(corners_race_ids):,}")
        
        # racesにあってcornersにないレース
        missing_in_corners = races_race_ids - corners_race_ids
        print(f"  racesにあってcornersにないレース: {len(missing_in_corners):,}件")
        
        # 年別の欠損確認
        if len(missing_in_corners) > 0:
            missing_df = races_df[races_df['race_id'].isin(missing_in_corners)]
            if 'year' in missing_df.columns:
                print(f"    年別欠損数:")
                miss_by_year = missing_df.groupby('year')['race_id'].nunique()
                for year, count in miss_by_year.items():
                    print(f"      {year}: {count:,}件")
    
    # race_detailsとの照合
    if race_details_df is not None:
        details_race_ids = set(race_details_df['race_id'].unique())
        print(f"\n  race_details.parquetのユニークrace_id数: {len(details_race_ids):,}")
        
        missing_in_details = races_race_ids - details_race_ids
        print(f"  racesにあってrace_detailsにないレース: {len(missing_in_details):,}件")
        
        if len(missing_in_details) > 0:
            missing_df = races_df[races_df['race_id'].isin(missing_in_details)]
            if 'year' in missing_df.columns:
                print(f"    年別欠損数:")
                miss_by_year = missing_df.groupby('year')['race_id'].nunique()
                for year, count in miss_by_year.items():
                    print(f"      {year}: {count:,}件")
    
    # returnsとの照合
    if returns_df is not None:
        returns_race_ids = set(returns_df['race_id'].unique())
        print(f"\n  returns.parquetのユニークrace_id数: {len(returns_race_ids):,}")
        
        missing_in_returns = races_race_ids - returns_race_ids
        print(f"  racesにあってreturnsにないレース: {len(missing_in_returns):,}件")

def compare_old_new_data(races_df, corners_df):
    """2014-2019（拡張データ）と2020-2025（既存データ）の比較"""
    print_section("9. 拡張データ（2014-2019）vs 既存データ（2020-2025）比較")
    
    if races_df is None or 'year' not in races_df.columns:
        print("  racesデータがないためスキップ")
        return
    
    old_races = races_df[races_df['year'] < 2020]
    new_races = races_df[races_df['year'] >= 2020]
    
    print(f"\n  2014-2019 レコード数: {len(old_races):,}")
    print(f"  2020-2025 レコード数: {len(new_races):,}")
    
    # 基本統計の比較
    for col in ['finish_position', 'win_odds', 'horse_weight', 'last_3f_time']:
        if col in races_df.columns:
            old_mean = old_races[col].mean()
            new_mean = new_races[col].mean()
            diff_pct = (new_mean - old_mean) / old_mean * 100 if old_mean != 0 else 0
            print(f"\n  {col}:")
            print(f"    2014-2019 平均: {old_mean:.2f}")
            print(f"    2020-2025 平均: {new_mean:.2f}")
            print(f"    差異: {diff_pct:+.1f}%")
    
    # cornersデータの比較
    if corners_df is not None and 'year' in corners_df.columns:
        old_corners = corners_df[corners_df['year'] < 2020]
        new_corners = corners_df[corners_df['year'] >= 2020]
        
        print(f"\n  コーナーデータ比較:")
        print(f"    2014-2019: {len(old_corners):,}件、{old_corners['race_id'].nunique():,}レース")
        print(f"    2020-2025: {len(new_corners):,}件、{new_corners['race_id'].nunique():,}レース")
        
        # gap_from_leaderの比較
        if 'gap_from_leader' in corners_df.columns:
            old_gap_mean = old_corners['gap_from_leader'].mean()
            new_gap_mean = new_corners['gap_from_leader'].mean()
            print(f"\n    gap_from_leader平均:")
            print(f"      2014-2019: {old_gap_mean:.2f}")
            print(f"      2020-2025: {new_gap_mean:.2f}")

def check_potential_issues():
    """潜在的な問題のチェック"""
    print_section("10. 潜在的な問題のチェック")
    
    # races.parquetを再読込
    races_df = load_parquet("races", "races.parquet")
    if races_df is None:
        return
    
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    # 1. 同一馬が同日に複数レース出走
    if 'horse_id' in races_df.columns:
        races_df['date_str'] = races_df['race_date'].dt.strftime('%Y-%m-%d')
        multi_race = races_df.groupby(['horse_id', 'date_str']).size()
        multi_race = multi_race[multi_race > 1]
        if len(multi_race) > 0:
            print(f"\n  同一馬が同日に複数レース出走: {len(multi_race)}件")
            print(f"    これは通常あり得ない（障害+平地の場合を除く）")
            # サンプル表示
            sample = multi_race.head()
            for (horse_id, date), count in sample.items():
                print(f"      {horse_id} on {date}: {count}回")
        else:
            print(f"\n  ✓ 同一馬の同日複数出走: なし")
    
    # 2. 着順の欠損パターン
    if 'finish_position' in races_df.columns:
        null_finish = races_df[races_df['finish_position'].isnull()]
        if len(null_finish) > 0:
            print(f"\n  着順がNULLのレコード: {len(null_finish)}件")
            year_dist = null_finish.groupby(null_finish['race_date'].dt.year).size()
            print(f"    年別分布:")
            for year, count in year_dist.items():
                print(f"      {year}: {count}件")
        else:
            print(f"\n  ✓ 着順NULLのレコード: なし")
    
    # 3. 1着が複数いるレース（同着以外の異常）
    if all(c in races_df.columns for c in ['race_id', 'finish_position']):
        first_place = races_df[races_df['finish_position'] == 1]
        multi_winners = first_place.groupby('race_id').size()
        multi_winners = multi_winners[multi_winners > 1]
        if len(multi_winners) > 0:
            print(f"\n  1着が複数いるレース: {len(multi_winners)}件")
            print(f"    （同着は正常だが、3頭以上は要確認）")
            three_plus = multi_winners[multi_winners >= 3]
            if len(three_plus) > 0:
                print(f"    3頭以上1着: {len(three_plus)}件")
        else:
            print(f"\n  ✓ 1着複数のレース: なし（全て単勝）")
    
    # 4. 出走取消・競走中止の確認
    if 'finish_position' in races_df.columns:
        # finish_positionが0や99など異常値
        abnormal = races_df[(races_df['finish_position'] == 0) | (races_df['finish_position'] > 18)]
        if len(abnormal) > 0:
            print(f"\n  異常着順（0 or 18超）: {len(abnormal)}件")
            print(f"    分布: {abnormal['finish_position'].value_counts().head()}")

def main():
    print("=" * 80)
    print("  KeibaAI_v2 Parquetデータ品質検証レポート")
    print("  生成日時: 2025-12-17")
    print("=" * 80)
    
    # 各ファイルの検証
    races_df = verify_races()
    corners_df = verify_corners()
    race_details_df = verify_race_details()
    returns_df = verify_returns()
    horses_df = verify_horses()
    pedigrees_df = verify_pedigrees()
    shutuba_df = verify_shutuba()
    
    # データ間整合性チェック
    cross_validate_data(races_df, corners_df, returns_df, race_details_df)
    
    # 新旧データ比較
    compare_old_new_data(races_df, corners_df)
    
    # 潜在的問題チェック
    check_potential_issues()
    
    print("\n" + "=" * 80)
    print("  検証完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
