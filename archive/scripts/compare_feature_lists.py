import yaml
from pathlib import Path

def compare_feature_lists():
    """新旧モデルの特徴量リストを比較"""
    
    old_model_dir = Path("keibaai/data/models/mu_v2.1_new_features")
    new_model_dir = Path("keibaai/data/models/mu_v2.1_fixed")
    
    old_feature_file = old_model_dir / "feature_names.yaml"
    new_feature_file = new_model_dir / "feature_names.yaml"
    
    if not old_feature_file.exists():
        print(f"旧モデルの特徴量リストが見つかりません: {old_feature_file}")
        return
    
    if not new_feature_file.exists():
        print(f"新モデルの特徴量リストが見つかりません: {new_feature_file}")
        return
    
    with open(old_feature_file, 'r', encoding='utf-8') as f:
        old_features = yaml.safe_load(f)
    
    with open(new_feature_file, 'r', encoding='utf-8') as f:
        new_features = yaml.safe_load(f)
    
    print("=== 特徴量リスト比較 ===\n")
    print(f"旧モデル特徴量数: {len(old_features)}")
    print(f"新モデル特徴量数: {len(new_features)}\n")
    
    # 差分を確認
    old_set = set(old_features)
    new_set = set(new_features)
    
    only_old = old_set - new_set
    only_new = new_set - old_set
    common = old_set & new_set
    
    print(f"共通特徴量: {len(common)}")
    print(f"旧モデルのみ: {len(only_old)}")
    print(f"新モデルのみ: {len(only_new)}\n")
    
    if only_old:
        print("⚠️ 旧モデルのみに存在する特徴量 (最初の20個):")
        for feat in list(only_old)[:20]:
            print(f"  - {feat}")
        print()
    
    if only_new:
        print("✅ 新モデルのみに存在する特徴量 (最初の20個):")
        for feat in list(only_new)[:20]:
            print(f"  - {feat}")
        print()
    
    # データリークの疑いがある特徴量を検出
    print("=== データリーク調査 ===")
    suspicious_keywords = ['finish_position', 'win_odds', 'popularity', 'finish_time', 'prize']
    
    suspicious_old = [f for f in old_features if any(k in f.lower() for k in suspicious_keywords) and not f.startswith('past_')]
    suspicious_new = [f for f in new_features if any(k in f.lower() for k in suspicious_keywords) and not f.startswith('past_')]
    
    if suspicious_old:
        print(f"\n⚠️ 旧モデルの疑わしい特徴量 ({len(suspicious_old)}個):")
        for feat in suspicious_old[:10]:
            print(f"  - {feat}")
    
    if suspicious_new:
        print(f"\n⚠️ 新モデルの疑わしい特徴量 ({len(suspicious_new)}個):")
        for feat in suspicious_new[:10]:
            print(f"  - {feat}")
    
    if not suspicious_old and not suspicious_new:
        print("✅ データリークの疑いがある特徴量は検出されませんでした")

if __name__ == "__main__":
    compare_feature_lists()
