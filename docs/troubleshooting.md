# トラブルシューティングガイド

このドキュメントは、KeibaAI_v2プロジェクト開発で遭遇した問題と解決策をまとめたものです。

## 目次
- [1. Pythonインポートエラー](#1-pythonインポートエラー)
- [2. 設定ファイル読み込み問題](#2-設定ファイル読み込み問題)
- [3. Parquetデータ破損](#3-parquetデータ破損)
- [4. PowerShellコマンド実行](#4-powershellコマンド実行)
- [5. モデルファイル形式不整合](#5-モデルファイル形式不整合)

---

## 1. Pythonインポートエラー

### 問題
`keibaai/src/models/predict.py`実行時に以下のエラー:
```
ImportError: No module named 'src.pipeline_core'
```

### 根本原因
`project_root`の計算が不正確。`predict.py`の位置は`keibaai/src/models/predict.py`で、以下の計算が必要：
- `parent`: `keibaai/src/models`
- `parent.parent`: `keibaai/src`
- `parent.parent.parent`: `keibaai` ← **正しいproject_root**

### 解決策

```python
# 誤った実装
project_root = Path(__file__).resolve().parent.parent  # keibaai/src
sys.path.append(str(project_root))
from src.pipeline_core import setup_logging  # NG: keibaai/src/src/pipeline_core.py を探してしまう

# 正しい実装
project_root = Path(__file__).resolve().parent.parent.parent  # keibaai
sys.path.insert(0, str(project_root / 'src'))  # keibaai/src をパスに追加
from pipeline_core import setup_logging  # OK: keibaai/src/pipeline_core.py
```

**ポイント**:
- `sys.path.insert(0, ...)` を使ってパスの先頭に追加
- インポート文から`src.`プレフィックスを削除

---

## 2. 設定ファイル読み込み問題

### 問題A: ファイルが見つからない
```
FileNotFoundError: [Errno 2] No such file or directory: 'configs/default.yaml'
```

### 根本原因
相対パスで設定ファイルを指定すると、実行ディレクトリに依存する。

### 解決策

```python
# project_root基準の絶対パスに変換
config_path = project_root / args.config.replace('configs/', '')
if not config_path.exists():
    config_path = project_root / 'configs' / 'default.yaml'

config = load_config(str(config_path))
```

### 問題B: 変数置換がされない
```
KeyError: 'logs_path'
```

`default.yaml`には`${data_path}`のようなプレースホルダーが含まれるが、`load_config()`関数は変数置換を行わない。

### 解決策

```python
def replace_variables(config_dict, variables=None):
    if variables is None:
        variables = {}
    # data_pathを取得
    if 'data_path' in config_dict:
        variables['data_path'] = config_dict['data_path']
    
    # 再帰的に置換
    for key, value in config_dict.items():
        if isinstance(value, str):
            for var_name, var_value in variables.items():
                value = value.replace(f'${{{var_name}}}', var_value)
            config_dict[key] = value
            # 新しい変数として登録（xxx_pathパターン）
            if key.endswith('_path'):
                variables[key] = value
        elif isinstance(value, dict):
            config_dict[key] = replace_variables(value, variables)
    return config_dict

config = load_config(str(config_path))
config = replace_variables(config)  # 変数置換を実行
```

---

## 3. Parquetデータ破損

### 問題
```
parquet.lib.ArrowInvalid: Parquet file is corrupted or this is not a parquet file
```

### 根本原因
複数のフローや異なるバージョンのスクリプトで同じディレクトリに特徴量データを生成した場合、スキーマ不整合が発生する可能性がある。

### 診断方法

```python
from pathlib import Path
import pandas as pd

features_dir = Path('keibaai/data/features/parquet')
corrupted_files = []

for parquet_file in features_dir.rglob('*.parquet'):
    try:
        df = pd.read_parquet(parquet_file)
        print(f"✓ {parquet_file} ({len(df)} rows)")
    except Exception as e:
        print(f"✗ {parquet_file}: {e}")
        corrupted_files.append(parquet_file)

print(f"\n破損ファイル: {len(corrupted_files)}")
```

### 解決策

**Option 1**: クリーンな再生成（推奨）
```bash
# バックアップ
cd keibaai
Rename-Item -Path "data\features\parquet" -NewName "parquet_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss')"

# 新規生成
python src\features\generate_features.py \
  --start_date 2020-01-01 \
  --end_date 2024-12-31
```

**Option 2**: 破損ファイルのみ削除
```python
for f in corrupted_files:
    f.unlink()  # 削除
    print(f"Deleted: {f}")
```

**予防策**:
- 特徴量生成スクリプトのバージョンを統一
- 生成前に既存データをバックアップ
- スキーマ変更時は全データを再生成

---

## 4. PowerShellコマンド実行

### 問題
```powershell
cd keibaai && python src\models\predict.py --date 2023-12-31
```
エラー:
```
AmpersandNotAllowed: アンパサンド (&) 文字は許可されていません
```

### 根本原因
PowerShellでは`&&`演算子がサポートされていない（Bash構文）。

### 解決策

**方法1**: セミコロンを使う
```powershell
cd keibaai; python src\models\predict.py --date 2023-12-31
```

**方法2**: 別々のコマンドとして実行
```powershell
cd keibaai
python src\models\predict.py --date 2023-12-31
```

**方法3**: バッチファイルを使う（推奨）
```batch
@echo off
cd /d %~dp0keibaai
python src\models\predict.py --date 2023-12-31
```

---

## 5. モデルファイル形式不整合

### 問題
`sigma_model.pkl`が存在せず、`sigma_model.txt`のみが存在。

### 根本原因
`train_sigma_nu_models.py`で`json.dump()`を使用しているが、`json`モジュールがインポートされていない。そのため、保存処理が失敗し、`.pkl`ファイルが生成されない。

### 診断方法
```python
from pathlib import Path
import pickle
import json

model_dir = Path('keibaai/data/models')

# 必要なファイルをチェック
required_files = [
    'sigma_model.pkl',
    'sigma_features.json',
    'nu_model.pkl',
    'nu_features.json'
]

for filename in required_files:
    filepath = model_dir / filename
    status = "✓" if filepath.exists() else "✗"
    print(f"{status} {filename}")
```

### 解決策

**train_sigma_nu_models.py修正**:
```python
import argparse
import logging
import os
import pickle
import sys
import json  # ← 追加
from datetime import datetime, timedelta
from pathlib import Path
```

**predict.pyでのフォールバック処理**:
```python
# σモデルロード（エラー時は代替値使用）
sigma_model = None
try:
    sigma_model, sigma_features = load_plain_model(
        str(model_dir_path / 'sigma_model.pkl'),
        str(model_dir_path / 'sigma_features.json')
    )
except FileNotFoundError as e:
    logging.warning(f"σモデルが見つかりません: {e}")
    logging.warning("グローバル値（σ=1.0）を使用します")

# 推論時
if sigma_model is not None:
    sigma_pred = sigma_model.predict(X_sigma)
else:
    sigma_pred = np.full(len(race_features_df), 1.0)
```

---

## 6. データパス解決のベストプラクティス

### 問題
スクリプトを異なるディレクトリから実行すると、相対パスが正しく解決されない。

### 解決策

**常にproject_root基準の絶対パスを使う**:
```python
# スクリプト内でproject_rootを特定
project_root = Path(__file__).resolve().parent.parent.parent

# データパスを絶対パスに変換
def resolve_data_path(path_str, root):
    """相対パスを絶対パスに変換"""
    path = Path(path_str)
    if not path.is_absolute():
        path = root / path
    return path

features_path = resolve_data_path(config['features_path'], project_root)
models_path = resolve_data_path(args.model_dir, project_root)
```

---

## 7. よくあるエラーとクイックフィックス

| エラーメッセージ | 原因 | 解決策 |
|---|---|---|
| `ModuleNotFoundError: No module named 'src'` | インポートパスが不正確 | [#1](#1-pythonインポートエラー)参照 |
| `FileNotFoundError: configs/default.yaml` | 相対パス問題 | [#2](#2-設定ファイル読み込み問題)参照 |
| `KeyError: 'logs_path'` | 変数置換未実行 | [#2](#2-設定ファイル読み込み問題)参照 |
| `parquet.lib.ArrowInvalid` | Parquetファイル破損 | [#3](#3-parquetデータ破損)参照 |
| `AmpersandNotAllowed` | PowerShell構文エラー | [#4](#4-powershellコマンド実行)参照 |
| `sigma_model.pkl not found` | jsonインポート欠如 | [#5](#5-モデルファイル形式不整合)参照 |

---

## 8. デバッグのワークフロー

1. **エラーメッセージを正確に読む**
   - ファイルパス、行番号、エラータイプを確認

2. **パスを検証**
   ```python
   from pathlib import Path
   print(f"__file__: {Path(__file__).resolve()}")
   print(f"project_root: {project_root}")
   print(f"features_path: {features_path}")
   print(f"Exists: {features_path.exists()}")
   ```

3. **最小限のテストケースを作成**
   ```python
   # debug_test.py
   import sys
   from pathlib import Path
   
   project_root = Path(__file__).resolve().parent.parent.parent
   sys.path.insert(0, str(project_root / 'src'))
   
   try:
       from pipeline_core import load_config
       print("Import OK")
   except ImportError as e:
       print(f"Import Failed: {e}")
       print(f"sys.path: {sys.path}")
   ```

4. **ログを有効化**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

5. **段階的に機能を追加**
   - インポート → 設定読み込み → データロード → モデルロード → 推論

---

**最終更新**: 2025-11-22  
**メンテナ**: このドキュメントは開発中に発見した問題を随時追記してください。
