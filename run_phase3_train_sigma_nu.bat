@echo off
REM Phase 3: σ/νモデル訓練

cd /d %~dp0keibaai

echo ==========================================
echo Phase 3: σ/νモデル訓練
echo ==========================================

REM PYTHONPATHを設定
set "PYTHONPATH=%CD%\src"

echo PYTHONPATH: %PYTHONPATH%
echo.
echo 訓練期間: 48ヶ月 (2020-2023)
echo.

python src\models\train_sigma_nu_models.py ^
  --training_window_months 48 ^
  --output_dir data\models ^
  --mu_predictions_path data\predictions\parquet\mu_predictions.parquet

if %ERRORLEVEL% equ 0 (
    echo.
    echo ==========================================
    echo Phase 3完了: σ/νモデル訓練成功
    echo ==========================================
    echo.
    echo 生成されたファイル:
    dir /b data\models\sigma*.* 2>nul
    dir /b data\models\nu*.* 2>nul
    echo.
    echo 次のステップ:
    echo   Phase 4: 完全推論（2024年評価期間）を実行してください
) else (
    echo.
    echo ==========================================
    echo エラー: σ/νモデル訓練に失敗しました
    echo ==========================================
    exit /b 1
)
