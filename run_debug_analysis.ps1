# KeibaAI_v2 デバッグ分析スクリプト（PowerShell用）
# 使用方法: .\run_debug_analysis.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "KeibaAI_v2 デバッグ分析ツール" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# UTF-8エンコーディング設定
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

# 1. HTMLファイルの統計分析
Write-Host "[1/3] HTMLファイルの統計分析を実行中..." -ForegroundColor Yellow
Write-Host "----------------------------------------" -ForegroundColor Gray
try {
    python debug_html_files_analysis.py
    if ($LASTEXITCODE -ne 0) {
        throw "HTMLファイル分析でエラーが発生しました"
    }
} catch {
    Write-Host "エラー: $_" -ForegroundColor Red
    Read-Host "Enterキーを押して終了"
    exit 1
}

Write-Host ""

# 2. 2025年のパースエラー分析
Write-Host "[2/3] 2025年のパースエラー分析を実行中..." -ForegroundColor Yellow
Write-Host "----------------------------------------" -ForegroundColor Gray
try {
    python debug_parse_errors_analysis.py --year 2025 --max-files 50 --race
    if ($LASTEXITCODE -ne 0) {
        throw "パースエラー分析でエラーが発生しました"
    }
} catch {
    Write-Host "エラー: $_" -ForegroundColor Red
    Read-Host "Enterキーを押して終了"
    exit 1
}

Write-Host ""

# 3. 失敗ファイルの検出
Write-Host "[3/3] 失敗ファイルの検出を実行中..." -ForegroundColor Yellow
Write-Host "----------------------------------------" -ForegroundColor Gray
try {
    python recovery_failed_scraping.py --detect
    if ($LASTEXITCODE -ne 0) {
        throw "失敗ファイル検出でエラーが発生しました"
    }
} catch {
    Write-Host "エラー: $_" -ForegroundColor Red
    Read-Host "Enterキーを押して終了"
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "分析完了！" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "出力ファイル:" -ForegroundColor Cyan
Write-Host "- keibaai\data\logs\analysis\html_analysis_*.json"
Write-Host "- keibaai\data\logs\analysis\parse_errors_2025_*.json"
Write-Host "- keibaai\data\logs\recovery\recovery_list_*.json"
Write-Host ""

# ログディレクトリを開く
$logsPath = "keibaai\data\logs\analysis"
if (Test-Path $logsPath) {
    Write-Host "ログディレクトリを開きますか？ (Y/N): " -NoNewline -ForegroundColor Yellow
    $response = Read-Host
    if ($response -eq "Y" -or $response -eq "y") {
        Invoke-Item $logsPath
    }
}

Read-Host "Enterキーを押して終了"
