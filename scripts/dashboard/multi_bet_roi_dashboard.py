#!/usr/bin/env python3
"""
全券種ROI可視化ダッシュボード

V7モデルの予測に基づいて、全券種（単勝/複勝/馬連/馬単/ワイド/三連複/三連単）の
ROIをインタラクティブに可視化します。

【使用方法】
streamlit run scripts/dashboard/multi_bet_roi_dashboard.py
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import json
import pickle
import lightgbm as lgb

# =====================================================================
# データ読み込みとキャッシュ
# =====================================================================

@st.cache_data
def load_data():
    """データを読み込み（キャッシュ付き）"""
    data_dir = Path('keibaai/data/parsed/parquet')
    
    returns_df = pd.read_parquet(data_dir / 'returns/returns.parquet')
    returns_df['race_id'] = returns_df['race_id'].astype(str)
    
    races_df = pd.read_parquet(data_dir / 'races/races.parquet')
    races_df['race_id'] = races_df['race_id'].astype(str)
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees/pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corners/corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details/race_details.parquet')
    
    return returns_df, races_df, pedigrees_df, corners_df, race_details_df


@st.cache_resource
def train_v7_model(train_end_date: str):
    """V7モデルを学習（キャッシュ付き）"""
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    data_dir = Path('keibaai/data/parsed/parquet')
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / 'races/races.parquet')
    races_df['race_id'] = races_df['race_id'].astype(str)
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees/pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corners/corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details/race_details.parquet')
    returns_df = pd.read_parquet(data_dir / 'returns/returns.parquet')
    
    # 学習データ
    train_df = races_df[races_df['race_date'] < train_end_date].copy()
    
    # V7特徴量エンジニア
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    train_features = fe.transform(train_df)
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    
    # モデル学習
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    y_rel = np.zeros(len(y_train))
    y_rel[y_train.values == 1] = 5
    y_rel[y_train.values == 2] = 4
    y_rel[y_train.values == 3] = 3
    y_rel[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=1000,
        learning_rate=0.01,
        max_depth=3,
        num_leaves=8,
        min_child_samples=100,
        reg_alpha=2.0,
        reg_lambda=3.0,
        subsample=0.6,
        colsample_bytree=0.6,
        random_state=42,
        verbose=-1
    )
    
    model.fit(X_train, y_rel, group=groups_train)
    
    return fe, model, feature_cols


@st.cache_data
def get_v7_predictions(races_df, _fe, _model, _feature_cols, start_date, end_date):
    """V7モデルで予測を実行（キャッシュ付き）"""
    test_df = races_df[
        (races_df['race_date'] >= pd.Timestamp(start_date)) &
        (races_df['race_date'] <= pd.Timestamp(end_date))
    ].copy()
    
    if len(test_df) == 0:
        return pd.DataFrame()
    
    # 特徴量生成と予測
    test_features = _fe.transform(test_df)
    available_cols = [c for c in _feature_cols if c in test_features.columns]
    
    X_test = test_features[available_cols].fillna(0)
    test_features['score'] = _model.predict(X_test)
    test_features['rank_pred'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    return test_features


# =====================================================================
# ROI計算関数
# =====================================================================

def calculate_tansho_roi(test_data, returns_df):
    """単勝ROI計算"""
    tansho_returns = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho_returns.columns = ['race_id', 'winner_horse_number', 'tansho_payout']
    tansho_returns['winner_horse_number'] = pd.to_numeric(tansho_returns['winner_horse_number'], errors='coerce')
    
    top1 = test_data[test_data['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position', 'race_date']].copy()
    top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    top1 = top1.merge(
        tansho_returns, 
        left_on=['race_id', 'horse_number'], 
        right_on=['race_id', 'winner_horse_number'], 
        how='left'
    )
    top1['is_hit'] = ~top1['tansho_payout'].isna()
    top1['payout'] = top1['tansho_payout'].fillna(0)
    
    return top1


def calculate_fukusho_roi(test_data, returns_df):
    """複勝ROI計算"""
    fukusho_returns = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho_returns.columns = ['race_id', 'fukusho_horse_number', 'fukusho_payout']
    fukusho_returns['fukusho_horse_number'] = pd.to_numeric(fukusho_returns['fukusho_horse_number'], errors='coerce')
    
    top1 = test_data[test_data['rank_pred'] == 1][['race_id', 'horse_number', 'finish_position', 'race_date']].copy()
    top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    top1 = top1.merge(
        fukusho_returns, 
        left_on=['race_id', 'horse_number'], 
        right_on=['race_id', 'fukusho_horse_number'], 
        how='left'
    )
    top1['is_hit'] = ~top1['fukusho_payout'].isna()
    top1['payout'] = top1['fukusho_payout'].fillna(0)
    
    return top1


def calculate_umaren_roi(test_data, returns_df):
    """馬連ROI計算"""
    umaren_returns = returns_df[returns_df['bet_type'] == 'umaren'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
    
    top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred', 'race_date']].copy()
    top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
    
    try:
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
    except:
        return pd.DataFrame()
    
    # 日付情報を追加
    race_dates = test_data[['race_id', 'race_date']].drop_duplicates()
    top2_pivot = top2_pivot.merge(race_dates, on='race_id', how='left')
    
    # 馬連は順不同なのでソート
    top2_pivot['pred_min'] = top2_pivot[['pred_1', 'pred_2']].min(axis=1)
    top2_pivot['pred_max'] = top2_pivot[['pred_1', 'pred_2']].max(axis=1)
    
    umaren_returns['h_min'] = umaren_returns[['horse_1', 'horse_2']].min(axis=1)
    umaren_returns['h_max'] = umaren_returns[['horse_1', 'horse_2']].max(axis=1)
    
    result = top2_pivot.merge(
        umaren_returns[['race_id', 'h_min', 'h_max', 'payout']], 
        left_on=['race_id', 'pred_min', 'pred_max'], 
        right_on=['race_id', 'h_min', 'h_max'], 
        how='left'
    )
    result['is_hit'] = ~result['payout'].isna()
    result['payout'] = result['payout'].fillna(0)
    
    return result


def calculate_umatan_roi(test_data, returns_df):
    """馬単ROI計算"""
    umatan_returns = returns_df[returns_df['bet_type'] == 'umatan'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
    
    top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred', 'race_date']].copy()
    top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
    
    try:
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
    except:
        return pd.DataFrame()
    
    race_dates = test_data[['race_id', 'race_date']].drop_duplicates()
    top2_pivot = top2_pivot.merge(race_dates, on='race_id', how='left')
    
    # 馬単は順序あり
    result = top2_pivot.merge(
        umatan_returns, 
        left_on=['race_id', 'pred_1', 'pred_2'], 
        right_on=['race_id', 'horse_1', 'horse_2'], 
        how='left'
    )
    result['is_hit'] = ~result['payout'].isna()
    result['payout'] = result['payout'].fillna(0)
    
    return result


def calculate_wide_roi(test_data, returns_df):
    """ワイドROI計算"""
    wide_returns = returns_df[returns_df['bet_type'] == 'wide'][['race_id', 'horse_1', 'horse_2', 'payout']].copy()
    
    top2 = test_data[test_data['rank_pred'] <= 2][['race_id', 'horse_number', 'rank_pred', 'race_date']].copy()
    top2['horse_number'] = pd.to_numeric(top2['horse_number'], errors='coerce')
    
    try:
        top2_pivot = top2.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top2_pivot.columns = ['race_id', 'pred_1', 'pred_2']
    except:
        return pd.DataFrame()
    
    race_dates = test_data[['race_id', 'race_date']].drop_duplicates()
    top2_pivot = top2_pivot.merge(race_dates, on='race_id', how='left')
    
    top2_pivot['pred_min'] = top2_pivot[['pred_1', 'pred_2']].min(axis=1)
    top2_pivot['pred_max'] = top2_pivot[['pred_1', 'pred_2']].max(axis=1)
    
    wide_returns['h_min'] = wide_returns[['horse_1', 'horse_2']].min(axis=1)
    wide_returns['h_max'] = wide_returns[['horse_1', 'horse_2']].max(axis=1)
    
    result = top2_pivot.merge(
        wide_returns[['race_id', 'h_min', 'h_max', 'payout']], 
        left_on=['race_id', 'pred_min', 'pred_max'], 
        right_on=['race_id', 'h_min', 'h_max'], 
        how='left'
    )
    result['is_hit'] = ~result['payout'].isna()
    result['payout'] = result['payout'].fillna(0)
    
    return result


def calculate_sanrenpuku_roi(test_data, returns_df):
    """三連複ROI計算"""
    sanrenpuku_returns = returns_df[returns_df['bet_type'] == 'sanrenpuku'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
    
    top3 = test_data[test_data['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred', 'race_date']].copy()
    top3['horse_number'] = pd.to_numeric(top3['horse_number'], errors='coerce')
    
    try:
        top3_pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top3_pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
    except:
        return pd.DataFrame()
    
    race_dates = test_data[['race_id', 'race_date']].drop_duplicates()
    top3_pivot = top3_pivot.merge(race_dates, on='race_id', how='left')
    
    # 三連複は順不同なのでソート
    top3_pivot['pred_a'] = top3_pivot[['pred_1', 'pred_2', 'pred_3']].min(axis=1)
    top3_pivot['pred_c'] = top3_pivot[['pred_1', 'pred_2', 'pred_3']].max(axis=1)
    top3_pivot['pred_b'] = top3_pivot['pred_1'] + top3_pivot['pred_2'] + top3_pivot['pred_3'] - top3_pivot['pred_a'] - top3_pivot['pred_c']
    
    sanrenpuku_returns['h_a'] = sanrenpuku_returns[['horse_1', 'horse_2', 'horse_3']].min(axis=1)
    sanrenpuku_returns['h_c'] = sanrenpuku_returns[['horse_1', 'horse_2', 'horse_3']].max(axis=1)
    sanrenpuku_returns['h_b'] = sanrenpuku_returns['horse_1'] + sanrenpuku_returns['horse_2'] + sanrenpuku_returns['horse_3'] - sanrenpuku_returns['h_a'] - sanrenpuku_returns['h_c']
    
    result = top3_pivot.merge(
        sanrenpuku_returns[['race_id', 'h_a', 'h_b', 'h_c', 'payout']], 
        left_on=['race_id', 'pred_a', 'pred_b', 'pred_c'], 
        right_on=['race_id', 'h_a', 'h_b', 'h_c'], 
        how='left'
    )
    result['is_hit'] = ~result['payout'].isna()
    result['payout'] = result['payout'].fillna(0)
    
    return result


def calculate_sanrentan_roi(test_data, returns_df):
    """三連単ROI計算"""
    sanrentan_returns = returns_df[returns_df['bet_type'] == 'sanrentan'][['race_id', 'horse_1', 'horse_2', 'horse_3', 'payout']].copy()
    
    top3 = test_data[test_data['rank_pred'] <= 3][['race_id', 'horse_number', 'rank_pred', 'race_date']].copy()
    top3['horse_number'] = pd.to_numeric(top3['horse_number'], errors='coerce')
    
    try:
        top3_pivot = top3.pivot(index='race_id', columns='rank_pred', values='horse_number').reset_index()
        top3_pivot.columns = ['race_id', 'pred_1', 'pred_2', 'pred_3']
    except:
        return pd.DataFrame()
    
    race_dates = test_data[['race_id', 'race_date']].drop_duplicates()
    top3_pivot = top3_pivot.merge(race_dates, on='race_id', how='left')
    
    # 三連単は順序あり
    result = top3_pivot.merge(
        sanrentan_returns, 
        left_on=['race_id', 'pred_1', 'pred_2', 'pred_3'], 
        right_on=['race_id', 'horse_1', 'horse_2', 'horse_3'], 
        how='left'
    )
    result['is_hit'] = ~result['payout'].isna()
    result['payout'] = result['payout'].fillna(0)
    
    return result


# =====================================================================
# メインダッシュボード
# =====================================================================

def main():
    st.set_page_config(
        page_title="KeibaAI 全券種ROI分析",
        page_icon="🏇",
        layout="wide"
    )
    
    st.title("🏇 KeibaAI 全券種ROI分析ダッシュボード")
    st.markdown("---")
    
    # データ読み込み
    with st.spinner("データ読み込み中..."):
        returns_df, races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    # サイドバー
    st.sidebar.header("📊 分析設定")
    
    # 期間選択
    min_date = races_df['race_date'].min().date()
    max_date = races_df['race_date'].max().date()
    
    start_date = st.sidebar.date_input(
        "開始日",
        value=pd.Timestamp('2025-01-01').date(),
        min_value=min_date,
        max_value=max_date
    )
    end_date = st.sidebar.date_input(
        "終了日",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )
    
    # 予測モード選択
    pred_mode = st.sidebar.radio(
        "予測モード",
        ["人気順位（ベースライン）", "V7モデル予測"],
        help="人気順位はJRAの公式オッズに基づくベースライン"
    )
    
    # 予測データ準備
    if pred_mode == "V7モデル予測":
        with st.spinner("🔄 V7モデル学習中（初回は数分かかります）..."):
            try:
                # Train期間は開始日より前
                train_end_date = str(start_date)
                fe, model, feature_cols = train_v7_model(train_end_date)
                st.sidebar.success(f"✅ V7モデル学習完了\n特徴量数: {len(feature_cols)}")
                
                # 予測実行
                races_clean = races_df.dropna(subset=['finish_position', 'win_odds'])
                test_data = get_v7_predictions(
                    races_clean, fe, model, feature_cols,
                    str(start_date), str(end_date)
                )
                
                if len(test_data) == 0:
                    st.error("対象期間にデータがありません")
                    return
                    
            except Exception as e:
                st.sidebar.error(f"❌ V7エラー: {str(e)[:100]}")
                st.sidebar.info("📌 人気順位モードにフォールバック")
                pred_mode = "人気順位（ベースライン）"
    
    if pred_mode == "人気順位（ベースライン）":
        # データフィルタ
        test_data = races_df[
            (races_df['race_date'] >= pd.Timestamp(start_date)) &
            (races_df['race_date'] <= pd.Timestamp(end_date))
        ].copy()
        
        test_data['finish_position'] = pd.to_numeric(test_data['finish_position'], errors='coerce')
        test_data['popularity'] = pd.to_numeric(test_data['popularity'], errors='coerce')
        test_data['win_odds'] = pd.to_numeric(test_data['win_odds'], errors='coerce')
        
        test_data['score'] = -test_data['popularity'].fillna(99)
        test_data['rank_pred'] = test_data.groupby('race_id')['score'].rank(ascending=False, method='first')
        st.sidebar.info("📌 人気順位を予測スコアとして使用")
    
    # 対象レースのreturns_dfをフィルタ
    test_race_ids = set(test_data['race_id'].unique())
    filtered_returns = returns_df[returns_df['race_id'].isin(test_race_ids)].copy()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**対象期間**: {start_date} 〜 {end_date}")
    st.sidebar.markdown(f"**対象レース数**: {len(test_race_ids):,}")
    
    # =====================================================================
    # メインコンテンツ
    # =====================================================================
    
    # 全券種のROI計算
    bet_results = {}
    
    # 単勝
    tansho_detail = calculate_tansho_roi(test_data, filtered_returns)
    if len(tansho_detail) > 0:
        bet_results['単勝'] = {
            'n_bets': len(tansho_detail),
            'n_hits': tansho_detail['is_hit'].sum(),
            'hit_rate': tansho_detail['is_hit'].mean(),
            'roi': tansho_detail['payout'].sum() / (len(tansho_detail) * 100),
            'detail': tansho_detail
        }
    
    # 複勝
    fukusho_detail = calculate_fukusho_roi(test_data, filtered_returns)
    if len(fukusho_detail) > 0:
        bet_results['複勝'] = {
            'n_bets': len(fukusho_detail),
            'n_hits': fukusho_detail['is_hit'].sum(),
            'hit_rate': fukusho_detail['is_hit'].mean(),
            'roi': fukusho_detail['payout'].sum() / (len(fukusho_detail) * 100),
            'detail': fukusho_detail
        }
    
    # 馬連
    umaren_detail = calculate_umaren_roi(test_data, filtered_returns)
    if len(umaren_detail) > 0:
        bet_results['馬連'] = {
            'n_bets': len(umaren_detail),
            'n_hits': umaren_detail['is_hit'].sum(),
            'hit_rate': umaren_detail['is_hit'].mean(),
            'roi': umaren_detail['payout'].sum() / (len(umaren_detail) * 100),
            'detail': umaren_detail
        }
    
    # 馬単
    umatan_detail = calculate_umatan_roi(test_data, filtered_returns)
    if len(umatan_detail) > 0:
        bet_results['馬単'] = {
            'n_bets': len(umatan_detail),
            'n_hits': umatan_detail['is_hit'].sum(),
            'hit_rate': umatan_detail['is_hit'].mean(),
            'roi': umatan_detail['payout'].sum() / (len(umatan_detail) * 100),
            'detail': umatan_detail
        }
    
    # ワイド
    wide_detail = calculate_wide_roi(test_data, filtered_returns)
    if len(wide_detail) > 0:
        bet_results['ワイド'] = {
            'n_bets': len(wide_detail),
            'n_hits': wide_detail['is_hit'].sum(),
            'hit_rate': wide_detail['is_hit'].mean(),
            'roi': wide_detail['payout'].sum() / (len(wide_detail) * 100),
            'detail': wide_detail
        }
    
    # 三連複
    sanrenpuku_detail = calculate_sanrenpuku_roi(test_data, filtered_returns)
    if len(sanrenpuku_detail) > 0:
        bet_results['三連複'] = {
            'n_bets': len(sanrenpuku_detail),
            'n_hits': sanrenpuku_detail['is_hit'].sum(),
            'hit_rate': sanrenpuku_detail['is_hit'].mean(),
            'roi': sanrenpuku_detail['payout'].sum() / (len(sanrenpuku_detail) * 100),
            'detail': sanrenpuku_detail
        }
    
    # 三連単
    sanrentan_detail = calculate_sanrentan_roi(test_data, filtered_returns)
    if len(sanrentan_detail) > 0:
        bet_results['三連単'] = {
            'n_bets': len(sanrentan_detail),
            'n_hits': sanrentan_detail['is_hit'].sum(),
            'hit_rate': sanrentan_detail['is_hit'].mean(),
            'roi': sanrentan_detail['payout'].sum() / (len(sanrentan_detail) * 100),
            'detail': sanrentan_detail
        }
    
    # =====================================================================
    # 1. サマリーカード
    # =====================================================================
    st.header("📈 全券種ROIサマリー")
    
    cols = st.columns(7)
    bet_names = ['単勝', '複勝', '馬連', '馬単', 'ワイド', '三連複', '三連単']
    
    for i, bet_name in enumerate(bet_names):
        with cols[i]:
            if bet_name in bet_results:
                r = bet_results[bet_name]
                roi_pct = r['roi'] * 100
                color = "green" if roi_pct >= 100 else ("orange" if roi_pct >= 80 else "red")
                
                st.metric(
                    label=bet_name,
                    value=f"{roi_pct:.1f}%",
                    delta=f"的中{r['hit_rate']*100:.1f}%"
                )
            else:
                st.metric(label=bet_name, value="N/A", delta="データなし")
    
    st.markdown("---")
    
    # =====================================================================
    # 2. ROI比較バーチャート
    # =====================================================================
    st.header("📊 券種別ROI比較")
    
    if bet_results:
        roi_data = pd.DataFrame([
            {'券種': name, 'ROI': r['roi'] * 100, '的中率': r['hit_rate'] * 100, '投資数': r['n_bets']}
            for name, r in bet_results.items()
        ])
        
        # 並べ替え（ROI順）
        roi_data = roi_data.sort_values('ROI', ascending=True)
        
        fig = go.Figure()
        
        # バーチャート
        colors = ['#e74c3c' if x < 80 else '#f39c12' if x < 100 else '#27ae60' for x in roi_data['ROI']]
        
        fig.add_trace(go.Bar(
            y=roi_data['券種'],
            x=roi_data['ROI'],
            orientation='h',
            marker_color=colors,
            text=[f"{x:.1f}%" for x in roi_data['ROI']],
            textposition='outside',
            hovertemplate="<b>%{y}</b><br>ROI: %{x:.1f}%<extra></extra>"
        ))
        
        # 100%ライン
        fig.add_vline(x=100, line_dash="dash", line_color="gray", annotation_text="損益分岐点")
        
        fig.update_layout(
            title="券種別ROI（%）",
            xaxis_title="ROI (%)",
            yaxis_title="",
            height=400,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # =====================================================================
    # 3. 時系列分析（累積ROI推移・月別推移・日次タイムライン）
    # =====================================================================
    st.header("📅 時系列分析")
    
    time_tab1, time_tab2, time_tab3, time_tab4 = st.tabs([
        "📈 累積ROI推移", "📊 月別ROI", "📍 日次的中タイムライン", "💰 資金推移シミュレーション"
    ])
    
    selected_bet = st.sidebar.selectbox("分析対象券種", list(bet_results.keys()), key="time_bet")
    
    if selected_bet in bet_results:
        detail_df = bet_results[selected_bet]['detail'].copy()
        
        if 'race_date' in detail_df.columns:
            detail_df['race_date'] = pd.to_datetime(detail_df['race_date'])
            detail_df = detail_df.sort_values('race_date')
            
            # Tab 1: 累積ROI推移
            with time_tab1:
                st.subheader("累積ROI推移グラフ")
                
                # 累積計算
                detail_df['cum_bets'] = range(1, len(detail_df) + 1)
                detail_df['cum_payout'] = detail_df['payout'].cumsum()
                detail_df['cum_roi'] = detail_df['cum_payout'] / (detail_df['cum_bets'] * 100) * 100
                detail_df['cum_hits'] = detail_df['is_hit'].cumsum()
                detail_df['cum_hit_rate'] = detail_df['cum_hits'] / detail_df['cum_bets'] * 100
                
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # 累積ROIライン
                fig.add_trace(
                    go.Scatter(
                        x=detail_df['race_date'],
                        y=detail_df['cum_roi'],
                        mode='lines',
                        name='累積ROI',
                        line=dict(color='#2ecc71', width=2),
                        fill='tozeroy',
                        fillcolor='rgba(46, 204, 113, 0.2)'
                    ),
                    secondary_y=False
                )
                
                # 累積的中率ライン
                fig.add_trace(
                    go.Scatter(
                        x=detail_df['race_date'],
                        y=detail_df['cum_hit_rate'],
                        mode='lines',
                        name='累積的中率',
                        line=dict(color='#3498db', width=2, dash='dot')
                    ),
                    secondary_y=True
                )
                
                # 100%ライン
                fig.add_hline(y=100, line_dash="dash", line_color="red", 
                             annotation_text="損益分岐点", secondary_y=False)
                
                fig.update_layout(
                    title=f"{selected_bet} 累積ROI推移",
                    height=450,
                    hovermode='x unified'
                )
                fig.update_xaxes(title_text="日付")
                fig.update_yaxes(title_text="累積ROI (%)", secondary_y=False)
                fig.update_yaxes(title_text="累積的中率 (%)", secondary_y=True)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 最終値を表示
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("最終ROI", f"{detail_df['cum_roi'].iloc[-1]:.1f}%")
                with col2:
                    st.metric("総ベット数", f"{len(detail_df):,}")
                with col3:
                    st.metric("総的中数", f"{int(detail_df['cum_hits'].iloc[-1]):,}")
                with col4:
                    st.metric("最終的中率", f"{detail_df['cum_hit_rate'].iloc[-1]:.1f}%")
            
            # Tab 2: 月別ROI
            with time_tab2:
                st.subheader("月別ROI・的中率推移")
                
                detail_df['month'] = detail_df['race_date'].dt.to_period('M')
                
                monthly = detail_df.groupby('month').agg({
                    'is_hit': ['sum', 'count'],
                    'payout': 'sum'
                }).reset_index()
                monthly.columns = ['month', 'hits', 'bets', 'payout']
                monthly['roi'] = monthly['payout'] / (monthly['bets'] * 100) * 100
                monthly['hit_rate'] = monthly['hits'] / monthly['bets'] * 100
                monthly['month_str'] = monthly['month'].astype(str)
                
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                # ROI棒グラフ
                colors = ['#e74c3c' if x < 80 else '#f39c12' if x < 100 else '#27ae60' for x in monthly['roi']]
                
                fig.add_trace(
                    go.Bar(name='ROI', x=monthly['month_str'], y=monthly['roi'], 
                          marker_color=colors, text=[f"{x:.0f}%" for x in monthly['roi']],
                          textposition='outside'),
                    secondary_y=False
                )
                
                # 的中率折れ線
                fig.add_trace(
                    go.Scatter(name='的中率', x=monthly['month_str'], y=monthly['hit_rate'], 
                              mode='lines+markers', line=dict(color='blue', width=2),
                              marker=dict(size=8)),
                    secondary_y=True
                )
                
                # 100%ライン
                fig.add_hline(y=100, line_dash="dash", line_color="gray", secondary_y=False)
                
                fig.update_layout(title=f"{selected_bet}の月別推移", height=400)
                fig.update_yaxes(title_text="ROI (%)", secondary_y=False)
                fig.update_yaxes(title_text="的中率 (%)", secondary_y=True)
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 月別テーブル
                st.dataframe(monthly[['month_str', 'bets', 'hits', 'roi', 'hit_rate']].rename(
                    columns={'month_str': '月', 'bets': 'ベット数', 'hits': '的中数', 
                            'roi': 'ROI(%)', 'hit_rate': '的中率(%)'}
                ), use_container_width=True)
            
            # Tab 3: 日次的中タイムライン
            with time_tab3:
                st.subheader("日次的中タイムライン")
                
                detail_df['date'] = detail_df['race_date'].dt.date
                
                daily = detail_df.groupby('date').agg({
                    'is_hit': ['sum', 'count'],
                    'payout': 'sum'
                }).reset_index()
                daily.columns = ['date', 'hits', 'bets', 'payout']
                daily['roi'] = daily['payout'] / (daily['bets'] * 100) * 100
                daily['profit'] = daily['payout'] - (daily['bets'] * 100)
                
                # 的中・不的中の散布図
                fig = go.Figure()
                
                # 利益の棒グラフ
                fig.add_trace(go.Bar(
                    x=daily['date'],
                    y=daily['profit'],
                    marker_color=['#27ae60' if p >= 0 else '#e74c3c' for p in daily['profit']],
                    name='日次収支',
                    text=[f"¥{int(p):,}" for p in daily['profit']],
                    hovertemplate="日付: %{x}<br>収支: ¥%{y:,.0f}<extra></extra>"
                ))
                
                fig.add_hline(y=0, line_dash="solid", line_color="gray")
                
                fig.update_layout(
                    title="日次収支（100円/ベット）",
                    height=400,
                    xaxis_title="日付",
                    yaxis_title="収支（円）"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 的中日のハイライト
                hit_days = daily[daily['hits'] > 0].sort_values('payout', ascending=False).head(10)
                if len(hit_days) > 0:
                    st.subheader("🎯 高配当日 Top 10")
                    hit_display = hit_days[['date', 'bets', 'hits', 'payout', 'profit']].copy()
                    hit_display['payout'] = hit_display['payout'].apply(lambda x: f"¥{int(x):,}")
                    hit_display['profit'] = hit_display['profit'].apply(lambda x: f"¥{int(x):+,}")
                    hit_display.columns = ['日付', 'ベット数', '的中数', '払戻', '収支']
                    st.dataframe(hit_display, use_container_width=True)
            
            # Tab 4: 資金推移シミュレーション
            with time_tab4:
                st.subheader("資金推移シミュレーション")
                
                initial_capital = st.slider("初期資金（円）", 10000, 1000000, 100000, 10000)
                bet_unit = st.slider("1回のベット額（円）", 100, 10000, 1000, 100)
                
                detail_df['bet_amount'] = bet_unit
                detail_df['return_amount'] = detail_df['payout'] * (bet_unit / 100)
                detail_df['net_profit'] = detail_df['return_amount'] - bet_unit
                detail_df['cum_profit'] = detail_df['net_profit'].cumsum()
                detail_df['capital'] = initial_capital + detail_df['cum_profit']
                
                fig = go.Figure()
                
                # 資金推移ライン
                fig.add_trace(go.Scatter(
                    x=detail_df['race_date'],
                    y=detail_df['capital'],
                    mode='lines',
                    name='資金残高',
                    line=dict(color='#9b59b6', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(155, 89, 182, 0.2)'
                ))
                
                # 初期資金ライン
                fig.add_hline(y=initial_capital, line_dash="dash", line_color="gray",
                             annotation_text=f"初期資金: ¥{initial_capital:,}")
                
                fig.update_layout(
                    title=f"資金推移（初期: ¥{initial_capital:,}, 1ベット: ¥{bet_unit:,}）",
                    height=450,
                    xaxis_title="日付",
                    yaxis_title="資金残高（円）",
                    yaxis=dict(tickformat=",")
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # サマリー
                final_capital = detail_df['capital'].iloc[-1]
                total_profit = final_capital - initial_capital
                profit_rate = (total_profit / initial_capital) * 100
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("最終資金", f"¥{final_capital:,.0f}")
                with col2:
                    st.metric("総損益", f"¥{total_profit:+,.0f}")
                with col3:
                    st.metric("利益率", f"{profit_rate:+.1f}%")
                with col4:
                    max_dd = (detail_df['capital'].cummax() - detail_df['capital']).max()
                    st.metric("最大ドローダウン", f"¥{max_dd:,.0f}")
    
    # =====================================================================
    # 4. オッズ帯別・人気別分析（単勝のみ）
    # =====================================================================
    st.header("🔍 詳細分析（単勝）")
    
    if '単勝' in bet_results:
        tansho = bet_results['単勝']['detail'].copy()
        
        # win_oddsとpopularityを追加
        if 'win_odds' not in tansho.columns:
            tansho = tansho.merge(
                test_data[['race_id', 'horse_number', 'win_odds', 'popularity']].drop_duplicates(),
                on=['race_id', 'horse_number'],
                how='left'
            )
        
        col1, col2 = st.columns(2)
        
        # オッズ帯別ROI
        with col1:
            st.subheader("📊 オッズ帯別ROI")
            
            odds_ranges = [
                (1, 3, '1-3倍（本命）'),
                (3, 5, '3-5倍'),
                (5, 10, '5-10倍'),
                (10, 20, '10-20倍'),
                (20, 50, '20-50倍（穴馬）'),
                (50, 100, '50-100倍（大穴）'),
                (100, 9999, '100倍以上')
            ]
            
            odds_results = []
            for low, high, label in odds_ranges:
                subset = tansho[(tansho['win_odds'] >= low) & (tansho['win_odds'] < high)]
                if len(subset) > 0:
                    n_bets = len(subset)
                    n_hits = subset['is_hit'].sum()
                    roi = subset['payout'].sum() / (n_bets * 100) * 100
                    odds_results.append({
                        'オッズ帯': label,
                        'ベット数': n_bets,
                        '的中数': n_hits,
                        '的中率': f"{n_hits/n_bets*100:.1f}%",
                        'ROI': f"{roi:.1f}%",
                        'roi_val': roi
                    })
            
            if odds_results:
                odds_df = pd.DataFrame(odds_results)
                
                # ヒートマップ的な色付け
                fig = go.Figure(data=[
                    go.Bar(
                        x=[r['オッズ帯'] for r in odds_results],
                        y=[r['roi_val'] for r in odds_results],
                        marker_color=['#e74c3c' if r['roi_val'] < 80 else '#f39c12' if r['roi_val'] < 100 else '#27ae60' for r in odds_results],
                        text=[f"{r['roi_val']:.1f}%" for r in odds_results],
                        textposition='outside'
                    )
                ])
                fig.add_hline(y=100, line_dash="dash", line_color="gray")
                fig.update_layout(title="オッズ帯別ROI", height=350,
                                xaxis_title="", yaxis_title="ROI (%)")
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(odds_df[['オッズ帯', 'ベット数', '的中数', '的中率', 'ROI']], use_container_width=True)
        
        # 人気別ROI
        with col2:
            st.subheader("🏆 人気別ROI")
            
            pop_results = []
            for pop in range(1, 13):
                subset = tansho[tansho['popularity'] == pop]
                if len(subset) > 0:
                    n_bets = len(subset)
                    n_hits = subset['is_hit'].sum()
                    roi = subset['payout'].sum() / (n_bets * 100) * 100
                    pop_results.append({
                        '人気': f"{pop}番人気",
                        'ベット数': n_bets,
                        '的中数': n_hits,
                        '的中率': f"{n_hits/n_bets*100:.1f}%",
                        'ROI': f"{roi:.1f}%",
                        'roi_val': roi
                    })
            
            if pop_results:
                fig = go.Figure(data=[
                    go.Bar(
                        x=[r['人気'] for r in pop_results],
                        y=[r['roi_val'] for r in pop_results],
                        marker_color=['#e74c3c' if r['roi_val'] < 80 else '#f39c12' if r['roi_val'] < 100 else '#27ae60' for r in pop_results],
                        text=[f"{r['roi_val']:.1f}%" for r in pop_results],
                        textposition='outside'
                    )
                ])
                fig.add_hline(y=100, line_dash="dash", line_color="gray")
                fig.update_layout(title="人気別ROI", height=350,
                                xaxis_title="", yaxis_title="ROI (%)")
                st.plotly_chart(fig, use_container_width=True)
                
                pop_df = pd.DataFrame(pop_results)
                st.dataframe(pop_df[['人気', 'ベット数', '的中数', '的中率', 'ROI']], use_container_width=True)
    
    st.markdown("---")
    
    # =====================================================================
    # 5. EV（期待値）分析
    # =====================================================================
    st.header("💰 期待値（EV）分析")
    
    if '単勝' in bet_results:
        tansho = bet_results['単勝']['detail'].copy()
        
        # EVを計算（モデル確率 × オッズ）
        # ここではスコアからソフトマックスで確率を計算
        if 'score' in test_data.columns:
            score_df = test_data[['race_id', 'horse_number', 'score', 'win_odds', 'popularity', 'finish_position']].copy()
            
            # レース内でソフトマックス確率を計算
            def calc_softmax_prob(group):
                scores = group['score'].values
                exp_scores = np.exp(scores - np.max(scores))
                probs = exp_scores / exp_scores.sum()
                group['model_prob'] = probs
                return group
            
            score_df = score_df.groupby('race_id', group_keys=False).apply(calc_softmax_prob)
            score_df['ev'] = score_df['model_prob'] * score_df['win_odds']
            score_df['is_winner'] = score_df['finish_position'] == 1
            
            # Top1のみ
            score_df['rank'] = score_df.groupby('race_id')['score'].rank(ascending=False, method='first')
            top1_ev = score_df[score_df['rank'] == 1].copy()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("平均モデル確率", f"{top1_ev['model_prob'].mean()*100:.1f}%")
            with col2:
                st.metric("平均オッズ", f"{top1_ev['win_odds'].mean():.1f}倍")
            with col3:
                st.metric("平均EV", f"{top1_ev['ev'].mean():.2f}")
            
            # EV帯別分析
            st.subheader("📈 EV帯別の的中率・ROI")
            
            ev_ranges = [
                (0, 0.5, 'EV < 0.5（低EV）'),
                (0.5, 0.8, '0.5 ≤ EV < 0.8'),
                (0.8, 1.0, '0.8 ≤ EV < 1.0'),
                (1.0, 1.2, '1.0 ≤ EV < 1.2（期待値プラス）'),
                (1.2, 1.5, '1.2 ≤ EV < 1.5'),
                (1.5, 10, 'EV ≥ 1.5（高EV）')
            ]
            
            ev_results = []
            for low, high, label in ev_ranges:
                subset = top1_ev[(top1_ev['ev'] >= low) & (top1_ev['ev'] < high)]
                if len(subset) > 5:  # 最低5件
                    n_bets = len(subset)
                    n_hits = subset['is_winner'].sum()
                    avg_odds = subset[subset['is_winner']]['win_odds'].mean() if n_hits > 0 else 0
                    roi = (n_hits * avg_odds) / n_bets * 100 if n_bets > 0 else 0
                    ev_results.append({
                        'EV帯': label,
                        'ベット数': n_bets,
                        '的中数': int(n_hits),
                        '的中率': f"{n_hits/n_bets*100:.1f}%",
                        '平均オッズ': f"{subset['win_odds'].mean():.1f}倍",
                        'ROI': f"{roi:.1f}%",
                        'roi_val': roi,
                        '推奨': '✅' if roi >= 100 else ''
                    })
            
            if ev_results:
                ev_df = pd.DataFrame(ev_results)
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=[r['EV帯'] for r in ev_results],
                        y=[r['roi_val'] for r in ev_results],
                        marker_color=['#e74c3c' if r['roi_val'] < 80 else '#f39c12' if r['roi_val'] < 100 else '#27ae60' for r in ev_results],
                        text=[f"{r['roi_val']:.1f}%" for r in ev_results],
                        textposition='outside'
                    )
                ])
                fig.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="損益分岐点")
                fig.update_layout(title="EV帯別ROI", height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(ev_df[['EV帯', 'ベット数', '的中数', '的中率', '平均オッズ', 'ROI', '推奨']], use_container_width=True)
                
                # 購入判断の推奨
                profitable_evs = [r for r in ev_results if r['roi_val'] >= 100]
                if profitable_evs:
                    st.success(f"💡 **推奨購入条件**: {', '.join([r['EV帯'] for r in profitable_evs])}")
                else:
                    st.warning("⚠️ ROI 100%を超えるEV帯がありません")
    
    st.markdown("---")
    
    # =====================================================================
    # 6. 高配当的中リスト
    # =====================================================================
    st.header("🎯 高配当的中リスト")
    
    if '単勝' in bet_results:
        tansho = bet_results['単勝']['detail'].copy()
        
        # win_oddsを追加
        if 'win_odds' not in tansho.columns:
            tansho = tansho.merge(
                test_data[['race_id', 'horse_number', 'win_odds', 'popularity']].drop_duplicates(),
                on=['race_id', 'horse_number'],
                how='left'
            )
        
        # 高配当的中（1000円以上）
        high_payout = tansho[tansho['payout'] >= 1000].sort_values('payout', ascending=False).head(20)
        
        if len(high_payout) > 0:
            high_payout_display = high_payout[['race_id', 'race_date', 'win_odds', 'popularity', 'payout']].copy()
            high_payout_display['payout'] = high_payout_display['payout'].apply(lambda x: f"¥{int(x):,}")
            high_payout_display['win_odds'] = high_payout_display['win_odds'].apply(lambda x: f"{x:.1f}倍" if pd.notna(x) else "N/A")
            high_payout_display['popularity'] = high_payout_display['popularity'].apply(lambda x: f"{int(x)}番人気" if pd.notna(x) else "N/A")
            high_payout_display.columns = ['レースID', '日付', 'オッズ', '人気', '払戻']
            
            st.dataframe(high_payout_display, use_container_width=True)
            
            # 統計
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("高配当的中数", f"{len(high_payout)}件")
            with col2:
                avg_payout = high_payout['payout'].mean()
                st.metric("平均払戻", f"¥{avg_payout:,.0f}")
            with col3:
                total_payout = high_payout['payout'].sum()
                st.metric("合計払戻", f"¥{total_payout:,.0f}")
        else:
            st.info("1,000円以上の高配当的中はありません")
    
    st.markdown("---")
    
    # =====================================================================
    # 7. スキップ推奨レース分析
    # =====================================================================
    st.header("🚫 レース選別分析")
    
    st.markdown("""
    **現在の戦略**: 全レースでTop1予測馬に1点投資
    
    以下の分析で「回避すべきレース条件」を特定できます：
    """)
    
    if '単勝' in bet_results:
        tansho = bet_results['単勝']['detail'].copy()
        
        # 競馬場、距離、馬場状態などの条件別分析
        if 'venue' not in tansho.columns:
            tansho = tansho.merge(
                test_data[['race_id', 'venue', 'distance_m', 'track_surface', 'track_condition']].drop_duplicates(),
                on='race_id',
                how='left'
            )
        
        col1, col2 = st.columns(2)
        
        # 競馬場別
        with col1:
            st.subheader("🏟️ 競馬場別ROI")
            if 'venue' in tansho.columns:
                venue_results = []
                for venue in tansho['venue'].dropna().unique():
                    subset = tansho[tansho['venue'] == venue]
                    if len(subset) >= 10:
                        n_bets = len(subset)
                        n_hits = subset['is_hit'].sum()
                        roi = subset['payout'].sum() / (n_bets * 100) * 100
                        venue_results.append({
                            '競馬場': venue,
                            'レース数': n_bets,
                            '的中数': n_hits,
                            'ROI': f"{roi:.1f}%",
                            'roi_val': roi,
                            '判定': '✅' if roi >= 100 else ('⚠️' if roi >= 80 else '❌')
                        })
                
                if venue_results:
                    venue_df = pd.DataFrame(venue_results).sort_values('roi_val', ascending=False)
                    st.dataframe(venue_df[['競馬場', 'レース数', '的中数', 'ROI', '判定']], use_container_width=True)
                    
                    # 回避推奨
                    avoid = [v['競馬場'] for v in venue_results if v['roi_val'] < 80]
                    if avoid:
                        st.error(f"🚫 回避推奨: {', '.join(avoid)}")
        
        # 馬場状態別
        with col2:
            st.subheader("🏇 馬場状態別ROI")
            if 'track_condition' in tansho.columns:
                cond_results = []
                for cond in tansho['track_condition'].dropna().unique():
                    subset = tansho[tansho['track_condition'] == cond]
                    if len(subset) >= 10:
                        n_bets = len(subset)
                        n_hits = subset['is_hit'].sum()
                        roi = subset['payout'].sum() / (n_bets * 100) * 100
                        cond_results.append({
                            '馬場状態': cond,
                            'レース数': n_bets,
                            '的中数': n_hits,
                            'ROI': f"{roi:.1f}%",
                            'roi_val': roi,
                            '判定': '✅' if roi >= 100 else ('⚠️' if roi >= 80 else '❌')
                        })
                
                if cond_results:
                    cond_df = pd.DataFrame(cond_results).sort_values('roi_val', ascending=False)
                    st.dataframe(cond_df[['馬場状態', 'レース数', '的中数', 'ROI', '判定']], use_container_width=True)
                    
                    # 回避推奨
                    avoid = [c['馬場状態'] for c in cond_results if c['roi_val'] < 80]
                    if avoid:
                        st.error(f"🚫 回避推奨: {', '.join(avoid)}")
    
    st.markdown("---")
    
    # =====================================================================
    # 8. 詳細テーブル
    # =====================================================================
    st.header("📋 詳細結果")
    
    detail_bet = st.selectbox("券種を選択", list(bet_results.keys()), key="detail_bet")
    
    if detail_bet in bet_results:
        detail_df = bet_results[detail_bet]['detail'].copy()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("✅ 的中レース")
            hits_df = detail_df[detail_df['is_hit'] == True][['race_id', 'race_date', 'payout']].head(50)
            if len(hits_df) > 0:
                hits_df['payout'] = hits_df['payout'].apply(lambda x: f"¥{int(x):,}")
                st.dataframe(hits_df, use_container_width=True)
            else:
                st.info("的中レースなし")
        
        with col2:
            st.subheader("❌ 不的中レース（直近50件）")
            miss_df = detail_df[detail_df['is_hit'] == False][['race_id', 'race_date']].head(50)
            if len(miss_df) > 0:
                st.dataframe(miss_df, use_container_width=True)
            else:
                st.info("不的中レースなし")
    
    # =====================================================================
    # 9. ROI計算方法の説明
    # =====================================================================
    with st.expander("📖 ROI計算方法と用語説明", expanded=False):
        st.markdown("""
        ### ROI（投資収益率）の計算
        
        ```
        ROI = 払戻総額 ÷ 投資総額 × 100%
        ```
        
        - **100%以上**: プラス（利益）
        - **100%未満**: マイナス（損失）
        - **約80%**: JRA控除率を考慮したランダム期待値
        
        ---
        
        ### EV（期待値）の計算
        
        ```
        EV = モデル予測確率 × オッズ
        ```
        
        - **EV > 1.0**: 期待値プラス（購入推奨）
        - **EV < 1.0**: 期待値マイナス（控除率に負ける）
        
        ---
        
        ### 各券種の的中条件
        
        | 券種 | 的中条件 | JRA控除率 |
        |------|---------|----------|
        | 単勝 | 1着を的中 | 20% |
        | 複勝 | 3着以内を的中 | 20% |
        | 馬連 | 1-2着を的中（順不同） | 22.5% |
        | 馬単 | 1-2着を的中（順序あり） | 25% |
        | ワイド | Top2が3着以内 | 22.5% |
        | 三連複 | 1-3着を的中（順不同） | 25% |
        | 三連単 | 1-3着を的中（順序あり） | 27.5% |
        
        ---
        
        ### 予測モードの違い
        
        - **人気順位（ベースライン）**: JRAオッズに基づく人気順位
        - **V7モデル予測**: LeakFreeFeatureEngineerV7の33特徴量を使用したLightGBM LambdaRank
        """)
    
    st.markdown("---")
    st.caption("KeibaAI_v2 Multi-Bet ROI Dashboard | Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))


if __name__ == "__main__":
    main()

