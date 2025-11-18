@echo off
REM KeibaAI_v2 GUI Dashboard 起動スクリプト (Windows)
REM 使い方: run_gui.bat

echo 🐴 KeibaAI_v2 GUI Dashboard を起動します...
echo.

REM プロジェクトルートに移動
cd /d %~dp0

REM 依存関係のチェック
echo 📦 依存関係をチェック中...
where streamlit >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ Streamlitがインストールされていません
    echo 以下のコマンドでインストールしてください:
    echo   pip install -r keibaai\gui\requirements.txt
    pause
    exit /b 1
)

echo ✅ Streamlitが見つかりました
echo.

REM Streamlitアプリを起動
echo 🚀 ダッシュボードを起動しています...
echo ブラウザで http://localhost:8501 にアクセスしてください
echo.
echo 終了するには Ctrl+C を押してください
echo.

streamlit run keibaai\gui\app.py ^
    --server.port 8501 ^
    --server.address localhost ^
    --browser.gatherUsageStats false

pause
