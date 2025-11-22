@echo off
REM Phase 2: μモデルでの推論実行（σ/ν訓練用）

cd /d %~dp0keibaai

echo ==========================================
echo Phase 2: μモデル推論実行
echo ==========================================

REM PYTHONPATHを設定
set "PYTHONPATH=%CD%\src"

echo PYTHONPATH: %PYTHONPATH%
echo.

python src\models\predict.py ^
  --date 2023-12-31 ^
  --model_dir data\models ^
  --output_filename mu_predictions.parquet

if %ERRORLEVEL% equ 0 (
    echo.
    echo ==========================================
    echo Phase 2完了: μ推論成功
    echo ==========================================
    echo.
    echo 次のステップ:
    echo   Phase 3: σ/νモデル訓練を実行してください
    echo   python keibaai\src\models\train_sigma_nu_models.py --training_window_months 48 --output_dir keibaai\data\models
) else (
    echo.
    echo ==========================================
    echo エラー: μ推論に失敗しました
    echo ==========================================
    exit /b 1
)
