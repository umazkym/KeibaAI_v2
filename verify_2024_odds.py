import pandas as pd
from pathlib import Path

# horses_performanceデータを読み込み
perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')

if perf_path.exists():
    print("=== 2024年データのオッズ確認 ===")
    df = pd.read_parquet(perf_path)
    
    # 2024年のデータのみを抽出
    df_2024 = df[df['race_date'] >= '2024-01-01'].copy()
    
    print(f"総レコード数: {len(df):,}行")
    print(f"2024年データ: {len(df_2024):,}行")
    
    if len(df_2024) > 0:
        print(f"\n=== 2024年データのwin_odds統計 ===")
        print(f"非null数: {df_2024['win_odds'].notna().sum():,}/{len(df_2024):,}")
        print(f"null数: {df_2024['win_odds'].isna().sum():,}")
        
        if df_2024['win_odds'].notna().any():
            print(f"\nオッズ統計:")
            print(f"  最小値: {df_2024['win_odds'].min():.1f}倍")
            print(f"  最大値: {df_2024['win_odds'].max():.1f}倍")
            print(f"  平均値: {df_2024['win_odds'].mean():.1f}倍")
            print(f"  中央値: {df_2024['win_odds'].median():.1f}倍")
            
            print(f"\nサンプルデータ（2024年、最初の5行）:")
            print(df_2024[['race_date', 'horse_name', 'finish_position', 'win_odds']].head())
        else:
            print("\n⚠️ 警告: 2024年データでもwin_oddsが全てNullです！")
    else:
        print("\n❌ エラー: 2024年のデータが見つかりません")
else:
    print("❌ エラー: horses_performance.parquetが見つかりません")
