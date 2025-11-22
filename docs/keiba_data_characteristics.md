# 競馬データの特性

このドキュメントは、KeibaAI_v2プロジェクトで扱う競馬データの特性をまとめたものです。

## データの時間的特性

### レース開催頻度

**重要**: 競馬のレースは毎日開催されるわけではありません。

- **年間開催日数**: 約180日（5年間で約900日）
- **開催パターン**: 主に土日祝日に集中
- **未開催日**: 平日の多く、年末年始、特定の祝日

### 実データ例（2020-2024）

```
Total races: 5,992,679行
Date range: 2020-01-01 ~ 2024-10-03
Unique dates: 180日分（5年間で1,825日中）
開催率: 約10%（週に約2日ペース）
```

### 月別分布

各月でレースが開催されるのは数日のみ：
- 1月: 2-3日程度
- 週末中心: 土日祝日が主
- 特別開催: GW、夏競馬、年末など

## データ生成への影響

### 特徴量データ（features.parquet）

**パーティション構造**:
```
data/features/parquet/
  year=2020/
    month=1/
      *.parquet  # 2020年1月にレースが開催された日のデータ
    month=2/
      *.parquet  # 2020年2月にレースが開催された日のデータ
  year=2021/
    ...
```

**重要な特性**:
1. 各parquetファイルは1-2日分のデータを含む
2. 連続した日付ではない（レース開催日のみ）
3. `race_date`カラムで実際の開催日を確認できる

## モデル推論時の注意点

### ❌ 失敗例

```python
# 存在しない日付を指定
predict.py --date 2020-01-15  # 火曜日、レース開催なし
predict.py --date 2023-10-31  # 火曜日、レース開催なし
```

エラー:
```
WARNING: 指定期間のデータが見つかりませんでした
```

### ✅ 正しいアプローチ

#### Option 1: 実在する日付を指定

```python
# 2020-01-01は確実に存在（年始の特別開催）
predict.py --date 2020-01-01
```

#### Option 2: 存在する日付リストを取得

```python
import pandas as pd
from pathlib import Path

# 全データから存在する日付を取得
features_dir = Path('keibaai/data/features/parquet')
df = pd.read_parquet(features_dir)
available_dates = sorted(df['race_date'].unique())

print(f"Available dates: {len(available_dates)}")
print(f"Latest: {available_dates[-1]}")
print(f"Sample dates: {available_dates[:10]}")
```

#### Option 3: 期間指定で複数日を処理

```python
# 月全体を指定すると、実際に存在する日のみが処理される
predict.py --start_date 2020-01-01 --end_date 2020-01-31
# → 2020年1月にレースが開催された2日分のみ処理
```

## データ取得時の考慮事項

### スクレイピング

netkeiba.comから取得する際：
- レース開催カレンダーAPIを活用
- 存在しないrace_idへのアクセスを避ける
- 404エラーは正常（未開催日）

### データ検証

```python
# データが実際に存在するか確認
from utils.data_utils import load_parquet_data_by_date

features_df = load_parquet_data_by_date(
    features_dir,
    target_dt,
    target_dt,
    date_col='race_date'
)

if features_df.empty:
    # この日はレース開催なし
    logging.info(f"{target_dt}はレース未開催")
else:
    logging.info(f"{target_dt}: {len(features_df)}件のレース")
```

## モデル訓練への影響

### 時系列分割

通常の時系列分割（例：過去3年→次の1年）は問題なく機能：
```python
# 訓練: 2020-2023（約720日分のレース）
# テスト: 2024（約180日分のレース）
```

実際の日数は少ないが、レース数は十分：
- 1日あたり平均レース数: 約30-40レース
- 180日 × 35レース = 約6,300レース/年

### ウィンドウサイズ

「過去48ヶ月」などのウィンドウ指定は、実際の開催日ベースで動作：
- カレンダー月ではなく、実際にレースがあった月
- PyArrow Datasetのパーティションフィルタが適切に処理

## トラブルシューティング

### 「データが見つかりません」エラー

**原因チェックリスト**:
1. ☑ 指定日付にレースが開催されているか？
2. ☑ features.parquetに該当データが存在するか？
3. ☑ race_dateカラムの型は正しいか？（timestamp[ns]）
4. ☑ PyArrow Datasetのフィルタは動作しているか？

**デバッグスクリプト**:
```python
# 利用可能な日付を確認
python -c "
import pandas as pd
from pathlib import Path
df = pd.read_parquet('keibaai/data/features/parquet')
dates = sorted(df['race_date'].unique())
print(f'Available: {len(dates)} days')
print(f'First: {dates[0]}')
print(f'  Last: {dates[-1]}')
"
```

## ベストプラクティス

1. **日付指定前に存在確認**
   - 盲目的に日付を指定しない
   - available_datesリストを参照

2. **エラーハンドリング**
   - 存在しない日付の場合、最寄りの開催日を提案
   - ログに開催スケジュール情報を記録

3. **ドキュメント化**
   - 開催カレンダーをプロジェクトに保存
   - 特別開催日（G1レースなど）をマーク

4. **テスト**
   - 確実に存在する日付でテスト（年始、週末など）
   - 存在しない日付での動作も検証

---

**最終更新**: 2025-11-22  
**参照**: data_loading_investigation.md（詳細調査レポート）
