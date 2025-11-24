# プロジェクト構成標準化スクリプト
# モデル開発段階を考慮し、実験的スクリプトも保持

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "プロジェクト構成標準化開始" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# 新ディレクトリ作成
Write-Host "`n[Phase 1] ディレクトリ作成" -ForegroundColor Yellow
$dirs = @(
    "scripts/features",
    "scripts/optimization",
    "scripts/simulation",
    "scripts/training/experimental"  # 実験的スクリプト用
)

foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Write-Host "  作成: $dir" -ForegroundColor Green
}

# ファイル移動
Write-Host "`n[Phase 2] ファイル移動" -ForegroundColor Yellow

# 2-1. features (現在アクティブなスクリプト)
Write-Host "`n  features/ からの移動:" -ForegroundColor Cyan
if (Test-Path "keibaai/src/features/generate_features.py") {
    Move-Item "keibaai/src/features/generate_features.py" "scripts/features/" -Force
    Write-Host "    ✓ generate_features.py → scripts/features/" -ForegroundColor Green
}

# 2-2. models → scripts/training (アクティブなスクリプト)
Write-Host "`n  models/ からの移動 (アクティブ):" -ForegroundColor Cyan

$activeScripts = @(
    "train_mu_model.py",
    "train_sigma_nu_models.py",
    "optimize_hyperparameters.py",
    "optimize_sigma_nu.py",
    "evaluate_model.py",
    "evaluate_model_advanced.py",
    "predict.py"
)

foreach ($script in $activeScripts) {
    $src = "keibaai/src/models/$script"
    if (Test-Path $src) {
        Move-Item $src "scripts/training/" -Force
        Write-Host "    ✓ $script → scripts/training/" -ForegroundColor Green
    }
}

# 2-3. models → scripts/training/experimental (実験的・過去バージョン)
Write-Host "`n  models/ からの移動 (実験的):" -ForegroundColor Cyan

$experimentalScripts = @(
    "train_mu_model_fixed.py",
    "train_sigma_nu_phase_d.py",
    "train_full_pipeline.py"
)

foreach ($script in $experimentalScripts) {
    $src = "keibaai/src/models/$script"
    if (Test-Path $src) {
        Move-Item $src "scripts/training/experimental/" -Force
        Write-Host "    ✓ $script → scripts/training/experimental/ (実験的)" -ForegroundColor Magenta
    }
}

# 2-4. optimizer → scripts/optimization
Write-Host "`n  optimizer/ からの移動:" -ForegroundColor Cyan

$optimizerScripts = @(
    "optimize_daily_races.py",
    "daily_allocator.py"
)

foreach ($script in $optimizerScripts) {
    $src = "keibaai/src/optimizer/$script"
    if (Test-Path $src) {
        Move-Item $src "scripts/optimization/" -Force
        Write-Host "    ✓ $script → scripts/optimization/" -ForegroundColor Green
    }
}

# 2-5. sim → scripts/simulation
Write-Host "`n  sim/ からの移動:" -ForegroundColor Cyan
if (Test-Path "keibaai/src/sim/simulate_daily_races.py") {
    Move-Item "keibaai/src/sim/simulate_daily_races.py" "scripts/simulation/" -Force
    Write-Host "    ✓ simulate_daily_races.py → scripts/simulation/" -ForegroundColor Green
}

# READMEファイル作成（実験的スクリプトの説明）
Write-Host "`n[Phase 3] ドキュメント作成" -ForegroundColor Yellow

$experimentalReadme = @"
# experimental/

このディレクトリには、過去の実験的スクリプトや特定バージョンのモデル訓練スクリプトが保存されています。

## 目的

- **履歴保存**: モデル開発の試行錯誤の記録
- **比較分析**: 異なるアプローチの比較
- **再現性**: 過去のモデルバージョンの再現

## ファイル一覧

| ファイル | 説明 | 作成日 |
|---------|------|--------|
| ``train_mu_model_fixed.py`` | μモデル訓練の修正版 | - |
| ``train_sigma_nu_phase_d.py`` | σ/νモデル Phase D実装 | - |
| ``train_full_pipeline.py`` | フルパイプライン訓練（実験的） | - |

## 使用方法

これらのスクリプトは参照・分析用です。現在の開発では使用されていませんが、
過去のアプローチを理解したり、特定のアルゴリズムを再試行する際に参照してください。

## 注意事項

- これらのスクリプトは動作保証されていません
- import パスが古い可能性があります
- 新規開発では ``scripts/training/`` の現行スクリプトを使用してください
"@

Set-Content -Path "scripts/training/experimental/README.md" -Value $experimentalReadme -Encoding UTF8
Write-Host "  作成: scripts/training/experimental/README.md" -ForegroundColor Green

Write-Host "`n================================" -ForegroundColor Cyan
Write-Host "移動完了" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

Write-Host "`n次のステップ:" -ForegroundColor Yellow
Write-Host "  1. import修正スクリプトを実行" -ForegroundColor White
Write-Host "  2. 全スクリプトの動作確認 (--help)" -ForegroundColor White
Write-Host "  3. ドキュメント更新" -ForegroundColor White
