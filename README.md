# KeibaAI_v2

競馬予想AIプロジェクト (Version 2)

## プロジェクト構造

```
Keiba_AI_v2/
├── keibaai/                # Pythonパッケージルート
│   ├── src/                # ソースコード
│   ├── configs/            # 設定ファイル
│   └── data/               # データディレクトリ（.gitignore対象）
├── scripts/                # 実行用スクリプト
│   ├── training/           # モデル学習・推論
│   ├── pipelines/          # データパイプライン
│   ├── maintenance/        # メンテナンス・クリーンアップ
│   └── debug/              # デバッグ用
├── docs/                   # ドキュメント
└── tests/                  # テストコード
```

## 実行方法

### 推論 (Prediction)
```bash
python scripts/training/predict.py --date YYYY-MM-DD --model_dir data/models/latest
```

### パイプライン実行
```bash
python scripts/pipelines/run_pipeline.py
```

## 開発ガイド
詳細なドキュメントは `docs/` ディレクトリを参照してください。
- `docs/system/`: システム詳細設計
- `CLAUDE.md`: 開発ガイドライン
