import optuna
import pandas as pd
import sys

# 標準出力のエンコーディングをUTF-8に設定
sys.stdout.reconfigure(encoding='utf-8')

study_name = "mu_v3_3_ranker"
storage = "sqlite:///c:/Users/zk-ht/Keiba/Keiba_AI_v2/keibaai/models/mu_v3_3/optuna_study.db"

try:
    study = optuna.load_study(study_name=study_name, storage=storage)
    print(f"Best ROI: {study.best_value*100:.2f}%")
    print(f"Best Params: {study.best_params}")
    
    # 全トライアルの結果を表示
    df = study.trials_dataframe()
    # カラム名を確認してから表示
    print("Columns:", df.columns)
    
    # 主要カラムを表示
    cols = ['number', 'value', 'params_weight_1st', 'params_weight_2nd', 'params_weight_3rd', 'params_odds_clip_upper']
    # 存在するカラムのみ選択
    cols = [c for c in cols if c in df.columns]
    
    print(df[cols].sort_values('value', ascending=False).head(10))
    
except Exception as e:
    print(f"Error: {e}")
