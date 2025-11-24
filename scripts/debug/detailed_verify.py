"""
最新の特徴量を詳細確認
"""
import pandas as pd
from pathlib import Path

features_dir = Path('keibaai/data/features/parquet/year=2024')

if not features_dir.exists():
    print("ERROR: 2024年データが見つかりません")
    exit(1)

# 最初のparquetファイルを探す
parquet_files = list(features_dir.rglob('*.parquet'))
if not parquet_files:
    print("ERROR: parquetファイルが見つかりません")
    exit(1)

print(f"読み込むファイル: {parquet_files[0]}")

df = pd.read_parquet(parquet_files[0])

print(f"\n総レコード数: {len(df):,}件")
print(f"列数: {len(df.columns)}")

# 賞金関連の統計
prize_cols = ['prize_money', 'prize_total']
for col in prize_cols:
    if col in df.columns:
        non_zero = (df[col] > 0).sum()
        mean_val = df[col].mean()
        max_val = df[col].max()
        print(f"\n{col}:")
        print(f"  非ゼロ: {non_zero:,}/{len(df):,} ({non_zero/len(df)*100:.1f}%)")
        print(f"  平均: {mean_val:.2f}")
        print(f"  最大: {max_val:.2f}")
        
        if non_zero > 0:
            print(f"  サンプル（上位5件）:")
            top5 = df.nlargest(5, col)[['horse_id', col, 'career_starts']]
            print(top5)

# 過去走特徴量
past_cols = [col for col in df.columns if col.startswith('past_')]
if past_cols:
    print(f"\n過去走特徴量（サンプル5個）:")
    for col in past_cols[:5]:
        if pd.api.types.is_numeric_dtype(df[col]):
            non_zero = (df[col] > 0).sum()
            print(f"  {col}: 非ゼロ={non_zero:,}/{len(df):,} ({non_zero/len(df)*100:.1f}%)")

print("\n" + "=" * 60)
print("結論")
print("=" * 60)

if 'prize_total' in df.columns:
    non_zero_total = (df['prize_total'] > 0).sum()
    if non_zero_total > 0:
        print("✅ prize_totalが正常に生成されました！")
        print(f"   非ゼロ: {non_zero_total:,}/{len(df):,} ({non_zero_total/len(df)*100:.1f}%)")
        print(f"   平均: {df[df['prize_total'] > 0]['prize_total'].mean():.2f}万円")
        
        # 過去走もチェック
        if past_cols:
            past_nonzero_count = sum((df[col] != 0).sum() for col in past_cols[:3])
            if past_nonzero_count > 0:
                print("\n✅ 過去走特徴量も正常に生成されました！")
            else:
                print("\n⚠️  過去走特徴量はまだゼロです")
        
        print("\n🎉 賞金データ問題が解決しました！")
    else:
        print("❌ prize_totalがまだゼロです")
else:
    print("❌ prize_total列が存在しません")
