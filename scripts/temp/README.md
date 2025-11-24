# scripts/temp/

一時的な作業用スクリプト置き場

## 用途

- 検証用の使い捨てスクリプト
- デバッグ用の一時コード  
- 実験的なアイデアの試作
- ワンオフの分析スクリプト

## ルール

### 配置判断

```
新しいスクリプトを作成
    ↓
Q1: 本番環境で使用する？
    YES → scripts/<category>/xxx.py へ
    NO  → Q2へ
          ↓
    Q2: 実験的で保存価値がある？
        YES → scripts/training/experimental/ へ
        NO  → scripts/temp/ へ（ここ）
```

### Git管理

- このディレクトリの**中身は Git管理しない**（.gitignore で除外）
- README.md のみ Git管理する
- 一時ファイルは各自のローカルのみに存在

### クリーンアップ

不要になったら随時削除：
```bash
# 定期的に実行
rm scripts/temp/*.py
```

## 例

```python
# scripts/temp/test_new_algorithm.py
# 新しいアルゴリズムの検証（一時的）

# scripts/temp/debug_data_quality.py  
# データ品質の確認（使い捨て）

# scripts/temp/quick_analysis.py
# 急ぎの分析（ワンオフ）
```

---

**Note**: 本番コードになったら適切な場所に移動してください。
