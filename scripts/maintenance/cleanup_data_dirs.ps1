# データディレクトリクリーンアップ
# 古いデータと不要なデバッグディレクトリを削除

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "データディレクトリクリーンアップ" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# 1. ルート直下の古い data/ ディレクトリを削除
Write-Host "`n[1] ルート data/ ディレクトリを削除中..." -ForegroundColor Yellow
if (Test-Path "data") {
    $fileCount = (Get-ChildItem "data" -Recurse -File).Count
    Write-Host "  ファイル数: $fileCount"
    
    Remove-Item "data" -Recurse -Force
    Write-Host "  ✓ 削除完了: data/" -ForegroundColor Green
}
else {
    Write-Host "  → data/ は存在しません（スキップ）" -ForegroundColor Gray
}

# 2. keibaai/data/ 配下のデバッグディレクトリを削除
Write-Host "`n[2] デバッグディレクトリを削除中..." -ForegroundColor Yellow
$debugDirs = Get-ChildItem "keibaai/data/simulations_debug*" -Directory -ErrorAction SilentlyContinue

if ($debugDirs) {
    foreach ($dir in $debugDirs) {
        $fileCount = (Get-ChildItem $dir.FullName -File -ErrorAction SilentlyContinue).Count
        Write-Host "  削除: $($dir.Name) ($fileCount ファイル)"
        Remove-Item $dir.FullName -Recurse -Force
    }
    Write-Host "  ✓ デバッグディレクトリ削除完了" -ForegroundColor Green
}
else {
    Write-Host "  → デバッグディレクトリは存在しません（スキップ）" -ForegroundColor Gray
}

# 3. evaluation ディレクトリも削除（一時的な評価結果）
Write-Host "`n[3] evaluation ディレクトリを削除中..." -ForegroundColor Yellow
if (Test-Path "keibaai/data/evaluation") {
    Remove-Item "keibaai/data/evaluation" -Recurse -Force
    Write-Host "  ✓ 削除完了: keibaai/data/evaluation/" -ForegroundColor Green
}
else {
    Write-Host "  → evaluation/ は存在しません（スキップ）" -ForegroundColor Gray
}

Write-Host "`n================================" -ForegroundColor Cyan
Write-Host "クリーンアップ完了！" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

Write-Host "`n削除されたディレクトリ:"
Write-Host "  - data/ (ルート直下の古いデータ)"
Write-Host "  - keibaai/data/simulations_debug_*"
Write-Host "  - keibaai/data/evaluation/"
Write-Host "`n正規のデータは keibaai/data/ に保持されています。" -ForegroundColor Green
