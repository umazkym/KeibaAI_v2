#!/usr/bin/env python3
"""
モデル構成の詳細調査スクリプト
各モデルディレクトリの内容を調査し、μ/σ/νの有無、データリークの可能性を確認
"""

import json
from pathlib import Path
import pandas as pd

def investigate_models():
    """全モデルディレクトリを調査"""
    
    models_dir = Path('keibaai/data/models')
    
    print("=" * 80)
    print("モデル構成の詳細調査")
    print("=" * 80)
    print()
    
    # モデルディレクトリのみを取得
    model_dirs = [d for d in models_dir.iterdir() if d.is_dir()]
    
    results = []
    
    for model_dir in sorted(model_dirs):
        print(f"📁 {model_dir.name}")
        print("-" * 80)
        
        # モデルファイルの確認
        has_mu = False
        has_sigma = False
        has_nu = False
        has_ranker = False
        has_regressor = False
        
        # μモデル関連
        mu_files = list(model_dir.glob("mu_model*"))
        if mu_files or (model_dir / "regressor.pkl").exists():
            has_mu = True
            print(f"  ✓ μモデル: {[f.name for f in mu_files] if mu_files else ['regressor.pkl']}")
        
        # ranker (μモデルの一部)
        if (model_dir / "ranker.pkl").exists():
            has_ranker = True
            print(f"  ✓ Ranker: ranker.pkl")
        
        # regressor (μモデルの一部)
        if (model_dir / "regressor.pkl").exists():
            has_regressor = True
            print(f"  ✓ Regressor: regressor.pkl")
        
        # σモデル
        sigma_files = list(model_dir.glob("sigma_model*"))
        if sigma_files:
            has_sigma = True
            print(f"  ✓ σモデル: {[f.name for f in sigma_files]}")
        
        # νモデル
        nu_files = list(model_dir.glob("nu_model*"))
        if nu_files:
            has_nu = True
            print(f"  ✓ νモデル: {[f.name for f in nu_files]}")
        
        # 特徴量リスト
        feature_files = list(model_dir.glob("*features.json"))
        if feature_files:
            print(f"  📋 特徴量ファイル: {[f.name for f in feature_files]}")
            
            # 特徴量の中身を確認（データリークチェック）
            for feat_file in feature_files:
                try:
                    with open(feat_file, 'r', encoding='utf-8') as f:
                        features = json.load(f)
                    
                    # 禁止カラム（リークの可能性）
                    forbidden = ['win_odds', 'place_odds', 'finish_position', 'finish_time_seconds', 
                                'final_odds', 'popularity', 'bracket_odds']
                    
                    if isinstance(features, list):
                        leaked = [f for f in features if any(forbidden_word in f for forbidden_word in forbidden)]
                        if leaked:
                            print(f"    ⚠️  データリーク疑い ({feat_file.name}): {leaked[:5]}")
                        else:
                            print(f"    ✓ リークなし ({len(features)}特徴量)")
                except Exception as e:
                    print(f"    ❌ 読み込みエラー: {e}")
        
        # model_info.json
        if (model_dir / "model_info.json").exists():
            try:
                with open(model_dir / "model_info.json", 'r') as f:
                    info = json.load(f)
                print(f"  📄 model_info.json:")
                for key, value in info.items():
                    if key not in ['feature_names']:  # feature_namesは長いので除外
                        print(f"    {key}: {value}")
            except Exception as e:
                print(f"    ❌ 読み込みエラー: {e}")
        
        # 全ファイル一覧
        all_files = list(model_dir.glob("*"))
        print(f"  📦 全ファイル数: {len(all_files)}")
        
        # 分類
        model_type = "Unknown"
        if has_mu and has_sigma and has_nu:
            model_type = "Complete (μ+σ+ν)"
        elif has_mu and (has_sigma or has_nu):
            components = []
            if has_mu: components.append("μ")
            if has_sigma: components.append("σ")
            if has_nu: components.append("ν")
            model_type = f"Partial ({'+'.join(components)})"
        elif has_mu:
            model_type = "μ Only"
        else:
            model_type = "Empty or Unknown"
        
        print(f"  🏷️  分類: {model_type}")
        print()
        
        results.append({
            'model_name': model_dir.name,
            'has_mu': has_mu,
            'has_sigma': has_sigma,
            'has_nu': has_nu,
            'has_ranker': has_ranker,
            'has_regressor': has_regressor,
            'model_type': model_type,
            'file_count': len(all_files)
        })
    
    # 結果サマリー
    print("=" * 80)
    print("調査結果サマリー")
    print("=" * 80)
    
    df = pd.DataFrame(results)
    
    print("\nモデルタイプ別集計:")
    print(df['model_type'].value_counts())
    
    print("\nμモデルを持つディレクトリ:")
    mu_models = df[df['has_mu']]
    for idx, row in mu_models.iterrows():
        print(f"  - {row['model_name']} ({row['model_type']})")
    
    print("\nComplete (μ+σ+ν) モデル:")
    complete_models = df[df['model_type'] == 'Complete (μ+σ+ν)']
    for idx, row in complete_models.iterrows():
        print(f"  - {row['model_name']}")
    
    # CSV保存
    output_path = 'keibaai/data/models/model_structure_analysis.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n分析結果を保存: {output_path}")
    
    return df

if __name__ == '__main__':
    investigate_models()
