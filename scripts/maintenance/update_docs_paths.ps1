# ドキュメント内のパス一括更新スクリプト
# keibaai/src/models/ → scripts/training/ へのパス変更

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "ドキュメントパス一括更新" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

$replacements = @{
    "keibaai/src/models/predict.py"                  = "scripts/training/predict.py"
    "keibaai/src/models/train_mu_model.py"           = "scripts/training/train_mu_model.py"
    "keibaai/src/models/train_sigma_nu_models.py"    = "scripts/training/train_sigma_nu_models.py"
    "keibaai/src/models/evaluate_model.py"           = "scripts/training/evaluate_model.py"
    "keibaai/src/models/evaluate_model_advanced.py"  = "scripts/training/evaluate_model_advanced.py"
    "keibaai/src/models/optimize_hyperparameters.py" = "scripts/training/optimize_hyperparameters.py"
    "keibaai/src/models/train_full_pipeline.py"      = "scripts/training/experimental/train_full_pipeline.py"
    # 以下はモジュールとして残存
    # "keibaai/src/models/model_train.py" は変更なし（モジュール）
    # "keibaai/src/models/generate_model_info.py" → 存在確認が必要
    # "keibaai/src/models/compare_models.py" → 存在確認が必要
}

$docs = Get-ChildItem "docs/system" -Filter "*.md"
$totalChanges = 0

foreach ($doc in $docs) {
    Write-Host "`n処理中: $($doc.Name)" -ForegroundColor Yellow
    
    $content = Get-Content $doc.FullName -Raw -Encoding UTF8
    $original = $content
    $changes = 0
    
    foreach ($old in $replacements.Keys) {
        $new = $replacements[$old]
        if ($content -match [regex]::Escape($old)) {
            $content = $content -replace [regex]::Escape($old), $new
            $count = ([regex]::Matches($original, [regex]::Escape($old))).Count
            $changes += $count
            Write-Host "  ✓ $old → $new ($count 箇所)" -ForegroundColor Green
        }
    }
    
    if ($changes -gt 0) {
        Set-Content -Path $doc.FullName -Value $content -Encoding UTF8 -NoNewline
        $totalChanges += $changes
        Write-Host "  → $changes 箇所を更新" -ForegroundColor Cyan
    }
    else {
        Write-Host "  → 変更なし" -ForegroundColor Gray
    }
}

Write-Host "`n================================" -ForegroundColor Cyan
Write-Host "更新完了: 合計 $totalChanges 箇所" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
