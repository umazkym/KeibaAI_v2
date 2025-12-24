import pandas as pd
import lightgbm as lgb
import json
from pathlib import Path
import streamlit as st
import sys
import os
from pathlib import Path

# プロジェクトルート（Keiba_AI_v2）をパスに追加
# 現在のファイル: keibaai/src/ui/components/data_loader.py
# 4つ上がプロジェクトルート
project_root = Path(__file__).resolve().parents[4]
sys.path.append(str(project_root))

from keibaai.src.utils.data_utils import load_parquet_data_by_date

class DataLoader:
    def __init__(self, base_dir='.'):
        self.base_dir = Path(base_dir)
        self.models_dir = self.base_dir / 'keibaai/data/models'
        self.predictions_dir = self.base_dir / 'keibaai/data/predictions/parquet'
        self.simulations_dir = self.base_dir / 'data/simulations'
        self.races_path = self.base_dir / 'keibaai/data/parsed/parquet/races/races.parquet'
        
    def get_available_models(self):
        """利用可能なモデルディレクトリのリストを取得"""
        if not self.models_dir.exists():
            return []
        
        models = [d.name for d in self.models_dir.iterdir() if d.is_dir()]
        # 日付順などでソートすると良い
        return sorted(models, reverse=True)

    @st.cache_resource
    def load_model(_self, model_dir_name):
        """LightGBMモデルをロード (キャッシュ対応)"""
        model_path = _self.models_dir / model_dir_name / 'mu_model.txt'
        if not model_path.exists():
            # 統合モデルの場合は mu_model/mu_model.txt かもしれない
            model_path = _self.models_dir / model_dir_name / 'mu_model' / 'mu_model.txt'
            
        if not model_path.exists():
            st.error(f"モデルファイルが見つかりません: {model_path}")
            return None
            
        try:
            model = lgb.Booster(model_file=str(model_path))
            return model
        except Exception as e:
            st.error(f"モデル読み込みエラー: {e}")
            return None

    @st.cache_data
    def load_feature_importance(_self, model_dir_name):
        """特徴量重要度を取得"""
        model = _self.load_model(model_dir_name)
        if model is None:
            return pd.DataFrame()
            
        importance_split = model.feature_importance(importance_type='split')
        importance_gain = model.feature_importance(importance_type='gain')
        feature_names = model.feature_name()
        
        df = pd.DataFrame({
            'feature': feature_names,
            'split': importance_split,
            'gain': importance_gain
        })
        return df.sort_values('gain', ascending=False)

    @st.cache_data
    def load_races(_self, year=None):
        """レース情報をロード"""
        if not _self.races_path.exists():
            return pd.DataFrame()
            
        df = pd.read_parquet(_self.races_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        if year:
            df = df[df['race_date'].dt.year == year]
            
        return df

    def load_predictions(self, date_str, model_dir_name):
        """指定日の予測データをロード"""
        # predictions_YYYYMMDD.parquet を探す
        # ただし、現状のパイプラインでは予測ファイル名にモデル名は含まれていないことが多い
        # そのため、日付で探す
        # TODO: モデルごとに予測出力先を変える設計にするのが理想だが、
        # 現状は `keibaai/data/predictions/parquet` にフラットに置かれていると仮定
        
        date_clean = date_str.replace('-', '')
        file_name = f"predictions_{date_clean}.parquet"
        file_path = self.predictions_dir / file_name
        
        if not file_path.exists():
            # mu_predictions_YYYYMMDD.parquet の可能性も
            file_name = f"mu_predictions_{date_clean}.parquet"
            file_path = self.predictions_dir / file_name
            
        if not file_path.exists():
            return pd.DataFrame()
            
        return pd.read_parquet(file_path)

    def load_simulation_results(self, race_id):
        """指定レースのシミュレーション結果(JSON)をロード"""
        # ファイル名パターン: *_{race_id}.json
        files = list(self.simulations_dir.glob(f"*_{race_id}.json"))
        if not files:
            return None
            
        # 最新のものを返す
        latest_file = sorted(files)[-1]
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
