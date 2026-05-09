"""
既存のモデルをmu_model.pkl形式で再保存するスクリプト
"""
import joblib
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.models.model_train import MuEstimator

def resave_model(model_dir):
    """既存のモデルをmu_model.pkl形式で再保存"""
    model_path = Path(model_dir)
    
    print(f"モデルディレクトリ: {model_path}")
    
    # 既存のモデルをロード
    ranker_path = model_path / "ranker.pkl"
    regressor_path = model_path / "regressor.pkl"
    
    if not ranker_path.exists() or not regressor_path.exists():
        print(f"エラー: ranker.pkl または regressor.pkl が見つかりません")
        return False
    
    try:
        print("既存のモデルをロード中...")
        ranker = joblib.load(ranker_path)
        regressor = joblib.load(regressor_path)
        print("✓ モデルのロードに成功")
    except Exception as e:
        print(f"エラー: モデルのロードに失敗しました: {e}")
        return False
    
    # MuEstimatorオブジェクトを作成
    estimator = MuEstimator({})
    estimator.model_ranker = ranker
    estimator.model_regressor = regressor
    
    # feature_names.jsonから特徴量名をロード
    import json
    feature_names_path = model_path / "feature_names.json"
    if feature_names_path.exists():
        try:
            with open(feature_names_path, 'r', encoding='utf-8') as f:
                estimator.feature_names = json.load(f)
            print(f"✓ 特徴量数: {len(estimator.feature_names)}")
        except Exception as e:
            print(f"警告: feature_names.json の読み込みに失敗: {e}")
            estimator.feature_names = []
    else:
        print("警告: feature_names.json が見つかりません")
        estimator.feature_names = []
    
    # mu_model.pklとして保存
    try:
        mu_model_path = model_path / "mu_model.pkl"
        joblib.dump(estimator, mu_model_path)
        print(f"✅ mu_model.pkl を保存しました: {mu_model_path}")
    except Exception as e:
        print(f"エラー: mu_model.pklの保存に失敗: {e}")
        return False
    
    # ファイルサイズを表示
    print(f"\n保存されたファイル:")
    for file in sorted(model_path.glob("*.pkl")):
        size_kb = file.stat().st_size / 1024
        print(f"  - {file.name}: {size_kb:.1f} KB")
    
    for file in sorted(model_path.glob("*.json")):
        size_kb = file.stat().st_size / 1024
        print(f"  - {file.name}: {size_kb:.1f} KB")
    
    return True

if __name__ == "__main__":
    model_dir = "data/models/mu_model_v1"
    success = resave_model(model_dir)
    
    if success:
        print("\n✅ モデルの再保存が完了しました")
        print(f"評価スクリプトを実行できます:")
        print(f"  python keibaai/src/models/evaluate_model.py --model_dir {model_dir} --start_date 2023-01-01 --end_date 2023-12-31")
    else:
        print("\n❌ モデルの再保存に失敗しました")
        sys.exit(1)
