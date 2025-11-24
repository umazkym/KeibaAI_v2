"""
モデルパフォーマンス詳細分析スクリプト

予測精度の評価、エラー分析、改善ポイントの特定を行います。
"""
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# 日本語フォント設定
plt.rcParams['font.sans-serif'] = ['MS Gothic', 'Yu Gothic', 'Meiryo']
plt.rcParams['axes.unicode_minus'] = False

def load_predictions_and_results():
    """予測データとレース結果をロード"""
    print("データをロード中...")
    
def load_predictions_and_results():
    """予測データとレース結果をロード"""
    print("データをロード中...")
    
    # シミュレーション結果（JSONファイル）
    sim_dir = Path('data/simulations')
    # v2_backtestを含むファイルを検索
    sim_files = sorted(sim_dir.glob('*v2_backtest*.json'))
    
    print(f"  シミュレーションファイル数: {len(sim_files)}")
    
    if len(sim_files) == 0:
        print("エラー: シミュレーションファイルが見つかりません")
        return None
    
    all_predictions = []
    for file in sim_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                race_id = data['race_id']
                
                # win_probsは {horse_number: prob} の辞書
                win_probs = data.get('win_probs', {})
                
                # mu/sigma/nuはここには含まれていないが、win_probがあれば十分
                
                for horse_num_str, prob in win_probs.items():
                    all_predictions.append({
                        'race_id': race_id,
                        'horse_number': int(horse_num_str),
                        'predicted_win_prob': prob
                    })
        except Exception as e:
            print(f"  警告: {file.name} の読み込みに失敗: {e}")
            continue
    
    pred_df = pd.DataFrame(all_predictions)
    print(f"  予測データ: {len(pred_df)}行, {pred_df['race_id'].nunique()}レース")
    
    # レース結果
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
    races_df = pd.read_parquet(races_path)
    races_df['race_id'] = races_df['race_id'].astype(str)
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    # 2024年のみ
    races_df = races_df[races_df['race_date'].dt.year == 2024]
    print(f"  レース結果: {len(races_df)}行")
    
    # マージ
    merged = pred_df.merge(
        races_df[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 1着フラグ
    merged = merged.dropna(subset=['finish_position'])
    merged['finish_position'] = pd.to_numeric(merged['finish_position'], errors='coerce')
    merged = merged.dropna(subset=['finish_position'])
    merged['is_winner'] = (merged['finish_position'] == 1).astype(int)
    
    print(f"  マージ後(有効データ): {len(merged)}行")
    return merged

def analyze_calibration(df):
    """予測確率のキャリブレーション分析"""
    print("\n" + "="*60)
    print("1. 予測確率のキャリブレーション分析")
    print("="*60)
    
    # 予測確率を10分位に分割
    df['prob_bin'] = pd.qcut(df['predicted_win_prob'], q=10, duplicates='drop')
    
    calibration = df.groupby('prob_bin').agg({
        'predicted_win_prob': 'mean',
        'is_winner': 'mean',
        'race_id': 'count'
    }).rename(columns={'race_id': 'count'})
    
    print("\n予測確率 vs 実際の的中率:")
    print(calibration)
    
    # 理想的には predicted_win_prob ≈ is_winner
    calibration['error'] = calibration['predicted_win_prob'] - calibration['is_winner']
    print(f"\n平均誤差: {calibration['error'].abs().mean():.4f}")
    
    # プロット
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(calibration['predicted_win_prob'], calibration['is_winner'], 
               s=calibration['count']*5, alpha=0.6)
    ax.plot([0, 1], [0, 1], 'r--', label='Perfect calibration')
    ax.set_xlabel('予測勝率')
    ax.set_ylabel('実際の勝率')
    ax.set_title('予測確率のキャリブレーション')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('calibration_plot.png', dpi=150, bbox_inches='tight')
    print("\n→ calibration_plot.png を保存しました")
    plt.close()
    
    return calibration

def analyze_by_popularity(df):
    """人気別の予測精度分析"""
    print("\n" + "="*60)
    print("2. 人気別の予測精度")
    print("="*60)
    
    # 人気を3グループに分割
    df_clean = df.dropna(subset=['popularity'])
    df_clean['pop_group'] = pd.cut(df_clean['popularity'], 
                                     bins=[0, 3, 6, 20], 
                                     labels=['人気馬(1-3)', '中団(4-6)', '人気薄(7+)'])
    
    pop_analysis = df_clean.groupby('pop_group').agg({
        'predicted_win_prob': 'mean',
        'is_winner': 'mean',
        'race_id': 'count'
    }).rename(columns={'race_id': 'count'})
    
    print("\n人気グループ別:")
    print(pop_analysis)
    
    # 人気馬の予測精度が低い場合、オーバーフィッティングの可能性
    
    return pop_analysis

def analyze_overconfidence(df):
    """過信度分析（σ/νの妥当性）"""
    print("\n" + "="*60)
    print("3. 予測の不確実性分析（σ/ν）")
    print("="*60)
    
    if 'predicted_sigma' not in df.columns or 'predicted_nu' not in df.columns:
        print("警告: predicted_sigma または predicted_nu がないため、この分析をスキップします")
        return None
    
    df_clean = df.dropna(subset=['predicted_sigma', 'predicted_nu'])
    
    # σが小さい = 自信がある予測
    df_clean['confidence_group'] = pd.qcut(df_clean['predicted_sigma'], 
                                            q=3, 
                                            labels=['高信頼(σ小)', '中信頼', '低信頼(σ大)'],
                                            duplicates='drop')
    
    conf_analysis = df_clean.groupby('confidence_group').agg({
        'predicted_win_prob': 'mean',
        'is_winner': 'mean',
        'predicted_sigma': 'mean',
        'race_id': 'count'
    }).rename(columns={'race_id': 'count'})
    
    print("\n信頼度グループ別:")
    print(conf_analysis)
    
    # 理想的には、σが小さい予測ほど的中率が高いべき
    
    return conf_analysis

def analyze_bet_thresholds(df):
    """ベット戦略の閾値最適化"""
    print("\n" + "="*60)
    print("4. ベット閾値の最適化")
    print("="*60)
    
    thresholds = np.arange(0.05, 0.40, 0.05)
    results = []
    
    for threshold in thresholds:
        # 閾値以上の予測のみベット
        bets = df[df['predicted_win_prob'] >= threshold].copy()
        
        if len(bets) == 0:
            continue
        
        # 各レースで最も高い予測確率の馬
        best_bets = bets.loc[bets.groupby('race_id')['predicted_win_prob'].idxmax()]
        
        total_races = len(best_bets)
        wins = best_bets['is_winner'].sum()
        win_rate = wins / total_races if total_races > 0 else 0
        
        # オッズがある場合のROI計算
        valid_odds = best_bets.dropna(subset=['win_odds'])
        if len(valid_odds) > 0:
            investment = len(valid_odds) * 100
            returns = (valid_odds['is_winner'] * valid_odds['win_odds'] * 100).sum()
            roi = (returns / investment) if investment > 0 else 0
        else:
            roi = 0
        
        results.append({
            'threshold': threshold,
            'races': total_races,
            'wins': wins,
            'win_rate': win_rate,
            'roi': roi
        })
    
    results_df = pd.DataFrame(results)
    print("\n閾値別の成績:")
    print(results_df.to_string(index=False))
    
    # 最適閾値
    best_threshold = results_df.loc[results_df['roi'].idxmax(), 'threshold']
    print(f"\n→ 最適閾値: {best_threshold:.2f} (ROI: {results_df['roi'].max():.2%})")
    
    # プロット
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    ax1.plot(results_df['threshold'], results_df['win_rate'], 'o-')
    ax1.set_xlabel('予測確率閾値')
    ax1.set_ylabel('的中率')
    ax1.set_title('閾値 vs 的中率')
    ax1.grid(True, alpha=0.3)
    
    ax2.plot(results_df['threshold'], results_df['roi'], 'o-', color='green')
    ax2.axhline(y=1.0, color='r', linestyle='--', label='損益分岐点')
    ax2.set_xlabel('予測確率閾値')
    ax2.set_ylabel('ROI (回収率)')
    ax2.set_title('閾値 vs ROI')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('threshold_optimization.png', dpi=150, bbox_inches='tight')
    print("→ threshold_optimization.png を保存しました")
    plt.close()
    
    return results_df

def main():
    print("モデルパフォーマンス詳細分析")
    print("="*60)
    
    # データロード
    df = load_predictions_and_results()
    
    # 各種分析
    calibration = analyze_calibration(df)
    pop_analysis = analyze_by_popularity(df)
    conf_analysis = analyze_overconfidence(df)
    threshold_results = analyze_bet_thresholds(df)
    
    # サマリー
    print("\n" + "="*60)
    print("分析完了")
    print("="*60)
    print("\n生成されたファイル:")
    print("  - calibration_plot.png")
    print("  - threshold_optimization.png")
    print("\n推奨される次のステップ:")
    print("  1. キャリブレーション誤差が大きい場合 → モデル再学習")
    print("  2. 人気馬の予測精度が低い場合 → 特徴量追加")
    print("  3. σ/νと精度の相関が弱い場合 → σ/νモデル改善")
    print("  4. 最適閾値でベット戦略を更新")

if __name__ == '__main__':
    main()
