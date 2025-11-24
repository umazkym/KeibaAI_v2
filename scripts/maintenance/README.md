# scripts/maintenance/

プロジェクトメンテナンス用スクリプト集

## 概要

このディレクトリには、プロジェクトの構造標準化・クリーンアップ・修正などのメンテナンス作業用スクリプトが格納されています。

## スクリプト一覧

### クリーンアップ系

| スクリプト | 用途 | 実行タイミング |
|-----------|------|--------------|
| `cleanup_data_dirs.ps1` | データディレクトリのクリーンアップ | ルート data/ 削除時（2025-11-24実行済み） |
| `deep_cleanup.ps1` | 古いスクリプト・レポートのアーカイブ | Phase 1クリーンアップ |
| `deep_cleanup_phase2.ps1` | Phase 2クリーンアップ | Phase 2実施時 |

### 構造標準化系

| スクリプト | 用途 | 実行タイミング |
|-----------|------|--------------|
| `standardize_structure.ps1` | プロジェクト構造標準化 | 2025-11-24実行済み |
| `cleanup_script.ps1` | 初期クリーンアップ | 初期整理時 |
| `cleanup_pass2.ps1` | 第2回クリーンアップ | 追加整理時 |

### Import修正系

| スクリプト | 用途 | 実行タイミング |
|-----------|------|--------------|
| `fix_moved_imports.py` | 移動したスクリプトのimport修正 | スクリプト移動後 |
| `fix_imports.py` | Import パス修正（初期版） | 初期移動時 |
| `fix_imports_v2.py` | Import パス修正（v2） | 改良版 |
| `fix_imports_phase2.py` | Phase 2のimport修正 | Phase 2実施時 |

### ドキュメント更新系

| スクリプト | 用途 | 実行タイミング |
|-----------|------|--------------|
| `update_docs_paths.ps1` | ドキュメント内パス一括更新 | 2025-11-24実行済み |

## 使用方法

### PowerShellスクリプト

```powershell
# 管理者権限不要
powershell -ExecutionPolicy Bypass -File scripts/maintenance/<script>.ps1
```

### Pythonスクリプト

```bash
python scripts/maintenance/<script>.py
```

## 注意事項

⚠️ **これらのスクリプトは既に実行済み**です。  
⚠️ 再実行すると予期しない動作をする可能性があります。  
⚠️ 実行前に必ずバックアップを取るか、Git commit してください。

## 履歴保存の目的

これらのスクリプトを `scripts/maintenance/` に保存している理由：

1. **再現性**: 過去の整理作業を追跡・再現可能
2. **参照**: 将来の類似作業の参考資料
3. **ドキュメント**: プロジェクト構造変更の記録

## 新規メンテナンススクリプト

今後、新しいメンテナンススクリプトを作成した場合もこのディレクトリに配置してください：

```
scripts/maintenance/
├── <new_cleanup_script>.ps1
└── README.md  # このファイル（適宜更新）
```

---

**最終更新**: 2025-11-24  
**管理者**: プロジェクトメンテナー
