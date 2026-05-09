"""
モデル性能比較スクリプト
keibaai/data/evaluationの全モデルを比較
"""
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# 日本語フォント設定
plt.rcParams['font.sans-serif'] = ['MS Gothic', 'Yu Gothic', 'Meiryo']
plt.rcParams['axes.unicode_minus'] = False

def load_all_evaluations():
    """evaluation結果を全て読み込み"""
    eval_dir = Path('keibaai/data/evaluation')
    
    evaluations = {}
    
    # 各CSVファイルを読み込み
    for csv_file in eval_dir.glob('evaluation_results*.csv'):
        # ファイル名からモデル名を抽出
        filename = csv_file.stem
        
        if 'mu_v2' in filename:
            model_name = 'μv2.0（1着予測）'
        elif 'complete' in filename:
            model_name = 'μ/σ/ν統合'
        elif 'mu_model' in filename or 'mu_phase' in filename:
            model_name = 'μモデル単体'
        elif filename == 'evaluation_results':
            model_name = 'ベースライン'
        else:
            continue  # スキップ
        
        try:
            df = pd.read_csv(csv_file)
            
            # dateカラムを日付型に変換
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            evaluations[model_name] = df
            print(f"✓ {model_name}: {len(df)}日分のデータ")
        except Exception as e:
            print(f"✗ {csv_file.name}: {e}")
    
    return evaluations

def compare_models(evaluations):
    """モデルを比較"""
    print("\n" + "=" * 80)
    print("モデル性能比較")
    print("=" * 80)
    
    summary = []
    
    for model_name, df in evaluations.items():
        # 集計
        total_races = df['race_count'].sum()
        avg_hit_rate = df['hit_rate'].mean()
        
        # ROI計算
        if 'total_bet' in df.columns and 'total_return' in df.columns:
            total_bet = df['total_bet'].sum()
            total_return = df['total_return'].sum()
            
            if total_bet > 0:
                roi = (total_return / total_bet)
            else:
                roi = np.nan
        else:
            roi = np.nan
        
        # RMSE/Spearman（該当モデルのみ）
        rmse = df['rmse'].mean() if 'rmse' in df.columns and df['rmse'].notna().any() else np.nan
        spearman = df['spearman_corr'].mean() if 'spearman_corr' in df.columns and df['spearman_corr'].notna().any() else np.nan
        
        summary.append({
            'モデル': model_name,
            '総レース数': int(total_races),
            '平均的中率': avg_hit_rate,
            'ROI': roi,
            'RMSE': rmse,
            'Spearman相関': spearman
        })
    
    # DataFrame化
    summary_df = pd.DataFrame(summary)
    summary_df = summary_df.sort_values('ROI', ascending=False)
    
    print(f"\n{'モデル':<20} {'総レース数':>10} {'的中率':>8} {'ROI':>8} {'RMSE':>8} {'Spearman':>10}")
    print("-" * 80)
    
    for _, row in summary_df.iterrows():
        roi_str = f"{row['ROI']:.2%}" if not np.isnan(row['ROI']) else "N/A"
        hit_str = f"{row['平均的中率']:.2%}"
        rmse_str = f"{row['RMSE']:.1f}" if not np.isnan(row['RMSE']) else "N/A"
        spearman_str = f"{row['Spearman相関']:.3f}" if not np.isnan(row['Spearman相関']) else "N/A"
        
        print(f"{row['モデル']:<20} {row['総レース数']:>10,} {hit_str:>8} {roi_str:>8} {rmse_str:>8} {spearman_str:>10}")
    
    return summary_df

def plot_comparisons(evaluations, summary_df):
    """比較グラフを作成"""
    
    # 1. ROI比較
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('モデル性能比較', fontsize=16, fontweight='bold')
    
    # ROIバーチャート
    ax1 = axes[0, 0]
    roi_data = summary_df[summary_df['ROI'].notna()].sort_values('ROI', ascending=False)
    colors = ['#FF6B6B' if roi < 1.0 else '#4ECDC4' for roi in roi_data['ROI']]
    ax1.barh(roi_data['モデル'], roi_data['ROI'], color=colors)
    ax1.axvline(x=1.0, color='black', linestyle='--', linewidth=1, label='損益分岐点')
    ax1.set_xlabel('ROI（回収率）')
    ax1.set_title('ROI比較')
    ax1.legend()
    ax1.grid(axis='x', alpha=0.3)
    
    # 的中率バーチャート
    ax2 = axes[0, 1]
    hit_data = summary_df.sort_values('平均的中率', ascending=False)
    ax2.barh(hit_data['モデル'], hit_data['平均的中率'], color='#95E1D3')
    ax2.set_xlabel('的中率')
    ax2.set_title('的中率比較')
    ax2.grid(axis='x', alpha=0.3)
    
    # 時系列ROI推移（データがあるモデルのみ）
    ax3 = axes[1, 0]
    for model_name, df in evaluations.items():
        if 'total_bet' in df.columns and 'total_return' in df.columns:
            df_clean = df.dropna(subset=['total_bet', 'total_return'])
            df_clean = df_clean[df_clean['total_bet'] > 0].copy()
            
            if len(df_clean) > 0:
                df_clean['roi'] = df_clean['total_return'] / df_clean['total_bet']
                df_clean['cumulative_roi'] = (df_clean['total_return'].cumsum() / 
                                              df_clean['total_bet'].cumsum())
                
                ax3.plot(df_clean['date'], df_clean['cumulative_roi'], 
                        marker='o', label=model_name, alpha=0.7)
    
    ax3.axhline(y=1.0, color='black', linestyle='--', linewidth=1)
    ax3.set_xlabel('日付')
    ax3.set_ylabel('累積ROI')
    ax3.set_title('累積ROI推移')
    ax3.legend()
    ax3.grid(alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    # RMSE vs Spearman散布図（該当モデルのみ）
    ax4 = axes[1, 1]
    rmse_spearman_data = summary_df[summary_df['RMSE'].notna() & summary_df['Spearman相関'].notna()]
    
    if len(rmse_spearman_data) > 0:
        ax4.scatter(rmse_spearman_data['RMSE'], rmse_spearman_data['Spearman相関'], 
                   s=100, alpha=0.6, c='#F38181')
        
        for _, row in rmse_spearman_data.iterrows():
            ax4.annotate(row['モデル'], (row['RMSE'], row['Spearman相関']),
                        xytext=(5, 5), textcoords='offset points', fontsize=9)
        
        ax4.set_xlabel('RMSE（低いほど良い）')
        ax4.set_ylabel('Spearman相関（負で絶対値が大きいほど良い）')
        ax4.set_title('RMSE vs Spearman相関')
        ax4.grid(alpha=0.3)
    else:
        ax4.text(0.5, 0.5, 'RMSEデータなし\n（μv2.0は分類モデル）', 
                ha='center', va='center', fontsize=12)
        ax4.set_title('RMSE vs Spearman相関')
    
    plt.tight_layout()
    plt.savefig('keibaai/data/evaluation/model_comparison.png', dpi=150, bbox_inches='tight')
    print(f"\n✅ グラフを保存: keibaai/data/evaluation/model_comparison.png")
    
    return fig

def main():
    print("=" * 80)
    print("KeibaAI_v2 モデル性能比較")
    print("=" * 80)
    
    # 1. データ読み込み
    evaluations = load_all_evaluations()
    
    if not evaluations:
        print("⚠️ 評価データが見つかりません")
        return
    
    # 2. 比較
    summary_df = compare_models(evaluations)
    
    # 3. グラフ化
    plot_comparisons(evaluations, summary_df)
    
    # 4. CSVで保存
    output_path = 'keibaai/data/evaluation/model_comparison_summary.csv'
    summary_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 比較結果を保存: {output_path}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
