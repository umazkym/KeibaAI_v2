@echo off
REM KeibaAI_v2 デバッグ分析スクリプト（Windows用）
REM 使用方法: run_debug_analysis.bat

echo ========================================
echo KeibaAI_v2 デバッグ分析ツール
echo ========================================
echo.

REM UTF-8エンコーディング設定
chcp 65001 > nul

echo [1/3] HTMLファイルの統計分析を実行中...
echo ----------------------------------------
python debug_html_files_analysis.py
if errorlevel 1 (
    echo エラーが発生しました。
    pause
    exit /b 1
)

echo.
echo [2/3] 2025年のパースエラー分析を実行中...
echo ----------------------------------------
python debug_parse_errors_analysis.py --year 2025 --max-files 50 --race
if errorlevel 1 (
    echo エラーが発生しました。
    pause
    exit /b 1
)

echo.
echo [3/3] 失敗ファイルの検出を実行中...
echo ----------------------------------------
python recovery_failed_scraping.py --detect
if errorlevel 1 (
    echo エラーが発生しました。
    pause
    exit /b 1
)

echo.
echo ========================================
echo 分析完了！
echo ========================================
echo.
echo 出力ファイル:
echo - keibaai\data\logs\analysis\html_analysis_*.json
echo - keibaai\data\logs\analysis\parse_errors_2025_*.json
echo - keibaai\data\logs\recovery\recovery_list_*.json
echo.

pause
