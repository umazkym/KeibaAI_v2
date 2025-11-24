#!/usr/bin/env python3
# tests/unit/test_calibration.py
"""
確率較正モジュールのユニットテスト
"""

import pytest
import numpy as np
import tempfile
from pathlib import Path
import sys

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from src.models.calibration import IsotonicCalibrator


class TestIsotonicCalibrator:
    """IsotonicCalibratorクラスのテスト"""
    
    def test_initialization(self):
        """初期化のテスト"""
        calibrator = IsotonicCalibrator()
        assert not calibrator.is_fitted
        assert calibrator.brier_score_before is None
        assert calibrator.brier_score_after is None
    
    def test_fit_basic(self):
        """基本的なfitのテスト"""
        # ダミーデータ：低い予測→負け、高い予測→勝ち
        y_true = np.array([0, 0, 0, 1, 1, 1])
        y_pred = np.array([0.1, 0.2, 0.3, 0.7, 0.8, 0.9])
        
        calibrator = IsotonicCalibrator()
        calibrator.fit(y_true, y_pred)
        
        assert calibrator.is_fitted
        assert calibrator.brier_score_before is not None
        assert calibrator.brier_score_after is not None
        # 較正によりBrier Scoreが改善または維持されることを確認
        assert calibrator.brier_score_after <= calibrator.brier_score_before
    
    def test_predict_proba(self):
        """predict_probaのテスト"""
        y_true = np.array([0, 0, 1, 1, 0, 1])
        y_pred = np.array([0.1, 0.2, 0.7, 0.8, 0.3, 0.9])
        
        calibrator = IsotonicCalibrator()
        calibrator.fit(y_true, y_pred)
        
        # 較正済み確率を取得
        test_pred = np.array([0.15, 0.5, 0.85])
        calibrated = calibrator.predict_proba(test_pred)
        
        # 確率の範囲チェック
        assert np.all((calibrated >= 0.0) & (calibrated <= 1.0))
        # 配列の長さチェック
        assert len(calibrated) == len(test_pred)
    
    def test_predict_without_fit(self):
        """未学習状態でのpredict_probaエラーテスト"""
        calibrator = IsotonicCalibrator()
        
        with pytest.raises(ValueError, match="未学習"):
            calibrator.predict_proba(np.array([0.5]))
    
    def test_save_and_load(self):
        """保存と読み込みのテスト"""
        y_true = np.array([0, 0, 1, 1, 0, 1, 1, 0])
        y_pred = np.array([0.1, 0.2, 0.7, 0.8, 0.3, 0.9, 0.75, 0.15])
        
        calibrator = IsotonicCalibrator()
        calibrator.fit(y_true, y_pred)
        
        # 一時ファイルに保存
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as tmp:
            tmp_path = Path(tmp.name)
        
        try:
            calibrator.save(tmp_path)
            
            # 読み込み
            loaded_calibrator = IsotonicCalibrator.load(tmp_path)
            
            # 同じ予測結果が得られることを確認
            test_pred = np.array([0.2, 0.5, 0.8])
            original_result = calibrator.predict_proba(test_pred)
            loaded_result = loaded_calibrator.predict_proba(test_pred)
            
            np.testing.assert_allclose(original_result, loaded_result, rtol=1e-5)
            
            # 属性も保存されていることを確認
            assert loaded_calibrator.is_fitted
            assert loaded_calibrator.brier_score_before == calibrator.brier_score_before
            assert loaded_calibrator.brier_score_after == calibrator.brier_score_after
            
        finally:
            # 一時ファイル削除
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_nan_handling(self):
        """NaN処理のテスト"""
        y_true = np.array([0, 0, np.nan, 1, 1, 1])
        y_pred = np.array([0.1, 0.2, 0.3, np.nan, 0.8, 0.9])
        
        calibrator = IsotonicCalibrator()
        # NaNがあっても学習できることを確認
        calibrator.fit(y_true, y_pred)
        
        assert calibrator.is_fitted
        
        # 予測時のNaN処理
        test_pred = np.array([0.5, np.nan, 0.8])
        calibrated = calibrator.predict_proba(test_pred)
        
        # NaNは0.5に置換される
        assert calibrated[1] == 0.5
        # 他の値は正常に較正される
        assert 0.0 <= calibrated[0] <= 1.0
        assert 0.0 <= calibrated[2] <= 1.0
    
    def test_input_validation(self):
        """入力検証のテスト"""
        calibrator = IsotonicCalibrator()
        
        # 長さが一致しない場合
        with pytest.raises(ValueError, match="長さが一致しません"):
            calibrator.fit(np.array([0, 1]), np.array([0.1, 0.5, 0.9]))
    
    def test_monotonicity(self):
        """単調性のテスト"""
        y_true = np.array([0, 0, 0, 1, 1, 1, 1, 0, 0, 1])
        y_pred = np.array([0.1, 0.15, 0.2, 0.6, 0.7, 0.8, 0.9, 0.3, 0.25, 0.85])
        
        calibrator = IsotonicCalibrator()
        calibrator.fit(y_true, y_pred)
        
        # 入力が単調増加なら、出力も単調増加であることを確認
        test_pred = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
        calibrated = calibrator.predict_proba(test_pred)
        
        # 単調増加をチェック
        assert np.all(np.diff(calibrated) >= 0), "較正後の確率が単調増加ではありません"
    
    def test_repr(self):
        """__repr__のテスト"""
        calibrator = IsotonicCalibrator()
        repr_str = repr(calibrator)
        assert "not fitted" in repr_str
        
        y_true = np.array([0, 0, 1, 1])
        y_pred = np.array([0.1, 0.2, 0.7, 0.8])
        calibrator.fit(y_true, y_pred)
        
        repr_str = repr(calibrator)
        assert "fitted" in repr_str
        assert "Brier" in repr_str


if __name__ == '__main__':
    # pytestがない環境でも実行できるように
    print("Running basic tests...")
    test = TestIsotonicCalibrator()
    
    try:
        test.test_initialization()
        print("✓ test_initialization passed")
        
        test.test_fit_basic()
        print("✓ test_fit_basic passed")
        
        test.test_predict_proba()
        print("✓ test_predict_proba passed")
        
        test.test_save_and_load()
        print("✓ test_save_and_load passed")
        
        test.test_nan_handling()
        print("✓ test_nan_handling passed")
        
        test.test_monotonicity()
        print("✓ test_monotonicity passed")
        
        test.test_repr()
        print("✓ test_repr passed")
        
        print("\nAll tests passed!")
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
