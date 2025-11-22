# predict.py修正・DataFrame保存の知見

**最終更新**: 2025-11-22  
**対象**: keibaai/src/models/predict.py

---

## 1. μモデルロード処理

### 問題
`mu_model`ディレクトリを探すが、実際は`mu_model.pkl`ファイルが存在。

### 解決策
```python
# mu_model.pklが直接存在する場合はそれをロード
mu_model_pkl = model_dir_path / 'mu_model.pkl'
mu_model_dir = model_dir_path / 'mu_model'

if mu_model_pkl.exists():
    mu_model = MuEstimator(mu_model_config)
    mu_model.load_model(str(mu_model_pkl.parent))  # 親ディレクトリを渡す
elif mu_model_dir.exists():
    mu_model = load_model_safely(MuEstimator, mu_model_config, str(mu_model_dir))
```

## 2. horse_numberカラム問題

### 問題
特徴量データに`horse_number`カラムが存在しない。

### 原因
特徴量生成時にhorse_numberが含まれていない（race_id, horse_idのみ）。

### 解決策  
結果DataFrameからhorse_number参照を削除：
```python
result_df = pd.DataFrame({
    'race_id': race_id,
    'horse_id': race_features_df['horse_id'],
    # 'horse_number': race_features_df['horse_number'],  # 削除
    'mu': mu_pred,
    'sigma': sigma_pred,
    'nu': nu_pred
})
```

## 3. パーティション保存でのカラム欠落

### 問題
パーティション保存（`partition_cols=['year', 'month', 'day']`）使用時、race_id, sigma, nuカラムが出力から欠落。

### 原因
1. パーティション保存はディレクトリとして保存され、指定カラムがパス名になる
2. 二重保存処理（パーティション + 単一ファイル）で、単一ファイルが`['horse_id', 'mu']`のみ抽出していた

### 解決策
単一ファイル保存に統一：
```python
# パーティション保存ではなく、単一ファイルで全カラムを保存
output_file = output_dir / args.output_filename
predictions_df.to_parquet(
    output_file,
    engine='pyarrow',
    compression='snappy',
    index=False  # インデックスを除外
)
```

## 4. try-exceptブロックのSyntaxError

### 問題
修正中にtry-exceptブロックが不完全になり、`SyntaxError: expected 'except' or 'finally' block`発生。

### 原因
tryブロックの後にexcept/finallyブロックがない状態でコードが続く。

### 解決策
必ずtryブロックにはexcept/finallyブロックを対応させる：
```python
try:
    # 処理
    predictions_df.to_parquet(...)
    logging.info("保存完了")

except Exception as e:
    logging.error(f"エラー: {e}", exc_info=True)
    sys.exit(1)
```

## 5. 保存前の検証ログ

### ベストプラクティス
保存前にDataFrameの状態をログ出力：
```python
# 保存前にカラムを確認
logging.info(f"保存前のカラム: {list(predictions_df.columns)}")
logging.info(f"保存前のサンプル:\n{predictions_df.head(2)}")
```

これにより、保存処理の問題を早期発見できる。

## 6. 学んだ教訓

1. **DataFrame保存形式の理解**
   - パーティション保存は複雑な構造を生成
   - シンプルなユースケースでは単一ファイルが推奨

2. **デバッグの重要性**
   - 保存前後でデータを検証
   - ログを充実させる

3. **編集の慎重さ**
   - try-exceptブロックなどの構文は一度に修正
   - 部分的な編集は新たなエラーを生む

4. **ユーザーとの協力**
   - AIツールの編集が適用されない場合、ユーザーに手動修正を依頼
   - 修正方法を明確に提示

---

**関連ドキュメント**:
- docs/troubleshooting.md
- docs/keiba_data_characteristics.md
