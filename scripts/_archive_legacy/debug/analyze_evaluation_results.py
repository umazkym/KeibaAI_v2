#!/usr/bin/env python3
"""
評価結果の深層分析スクリプト
keibaai/data/evaluation内の全CSVを読み込み、各モデルの特徴とROIを比較分析する
"""

import pandas as pd
import glob
from pathlib import Path
import json

def analyze_evaluation_results():
    """全評価結果を分析"""
    
    # CSVファイルのパスを取得
    csv_files = glob.glob('keibaai/data/evaluation/evaluation_results*.csv')
    
    results_summary = []
    
    print("=" * 80)
    print("評価結果の深層分析")
    print("=" * 80)
    print()
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file)
            
            # ファイル名からモデル名を抽出
            model_name = Path(csv_file).stem.replace('evaluation_results_', '')
            
            # 基本統計（日付集計形式のCSV）
            total_races = len(df)
            
            # カラム名を確認
            if 'total_bet' in df.columns and 'total_return' in df.columns:
                # 日付集計形式
                total_bet = df['total_bet'].sum()
                total_return = df['total_return'].sum()
                roi = (total_return / total_bet) if total_bet > 0 else 0
                
                # 的中率（recovery_rateの平均から推定）
                if 'hit_rate' in df.columns:
                    hit_rate = df['hit_rate'].mean()
                else:
                    hit_rate = (df['recovery_rate'] > 0).mean() if 'recovery_rate' in df.columns else 0
                    
                # レース数合計
                if 'race_count' in df.columns:
                    total_race_count = df['race_count'].sum()
                else:
                    total_race_count = total_races
                    
            elif 'bet_amount' in df.columns and 'return_amount' in df.columns:
                # レース単位形式
                total_bet = df['bet_amount'].sum()
                total_return = df['return_amount'].sum()
                roi = (total_return / total_bet) if total_bet > 0 else 0
                
                # 的中率
                if 'is_hit' in df.columns:
                    hit_rate = df['is_hit'].mean()
                else:
                    hit_rate = 0
                    
                total_race_count = total_races
            else:
                # カラムが見つからない場合
                total_bet = 0
                total_return = 0
                roi = 0
                hit_rate = 0
                total_race_count = total_races

            
            # 予測精度（Spearman相関）- CSVには含まれていない可能性が高い
            if 'spearman_corr' in df.columns:
                corr = df['spearman_corr'].mean()
            elif 'predicted_rank' in df.columns and 'actual_rank' in df.columns:
                from scipy.stats import spearmanr
                corr, _ = spearmanr(df['predicted_rank'], df['actual_rank'])
            elif 'mu_pred' in df.columns and 'finish_time_seconds' in df.columns:
                from scipy.stats import spearmanr
                corr, _ = spearmanr(df['mu_pred'], df['finish_time_seconds'])
            else:
                corr = 0
            
            summary = {
                'model_name': model_name,
                'file': Path(csv_file).name,
                'total_dates': total_races,  # 日付数
                'total_race_count': total_race_count,  # 実際のレース数
                'total_bet': total_bet,
                'total_return': total_return,
                'roi': roi,
                'hit_rate': hit_rate,
                'spearman_corr': corr,
                'columns': list(df.columns)
            }
            
            results_summary.append(summary)
            
            # 詳細表示
            print(f"モデル: {model_name}")
            print(f"  ファイル: {Path(csv_file).name}")
            print(f"  集計日数: {total_races:,}")
            print(f"  総レース数: {total_race_count:,}")
            print(f"  総投資額: ¥{total_bet:,.0f}")
            print(f"  総回収額: ¥{total_return:,.0f}")
            print(f"  ROI: {roi:.2%}")
            print(f"  的中率: {hit_rate:.2%}")
            print(f"  Spearman相関: {corr:.3f}")
            print(f"  カラム数: {len(df.columns)}")
            print()
            
        except Exception as e:
            print(f"エラー ({csv_file}): {e}")
            print()
    
    # 結果を DataFrame に変換してソート
    if results_summary:
        summary_df = pd.DataFrame(results_summary)
        
        print("=" * 80)
        print("ROI ランキング（上位5件）")
        print("=" * 80)
        top5 = summary_df.nlargest(5, 'roi')
        for idx, row in top5.iterrows():
            print(f"{row['model_name']}: ROI={row['roi']:.2%}, Hit={row['hit_rate']:.2%}, Races={row['total_race_count']:,}, Dates={row['total_dates']}")
        print()
        
        print("=" * 80)
        print("ROI ランキング（下位5件）")
        print("=" * 80)
        bottom5 = summary_df.nsmallest(5, 'roi')
        for idx, row in bottom5.iterrows():
            print(f"{row['model_name']}: ROI={row['roi']:.2%}, Hit={row['hit_rate']:.2%}, Races={row['total_race_count']:,}, Dates={row['total_dates']}")
        print()
        
        # JSONファイルも確認
        print("=" * 80)
        print("JSONファイルの確認")
        print("=" * 80)
        json_files = glob.glob('keibaai/data/evaluation/*.json')
        for json_file in sorted(json_files):
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                print(f"ファイル: {Path(json_file).name}")
                print(f"  内容: {json.dumps(data, indent=2, ensure_ascii=False)}")
                print()
            except Exception as e:
                print(f"エラー ({json_file}): {e}")
                print()
        
        # 最適化結果も確認
        opt_files = glob.glob('keibaai/data/evaluation/optimization_results*.csv')
        if opt_files:
            print("=" * 80)
            print("最適化結果（σ・νパラメータ）")
            print("=" * 80)
            for opt_file in sorted(opt_files):
                try:
                    opt_df = pd.read_csv(opt_file)
                    print(f"ファイル: {Path(opt_file).name}")
                    print(f"  パラメータ組み合わせ数: {len(opt_df)}")
                    if 'roi' in opt_df.columns:
                        best = opt_df.loc[opt_df['roi'].idxmax()]
                        print(f"  最良ROI: {best['roi']:.2%}")
                        if 'ev_threshold' in opt_df.columns and 'nu_threshold' in opt_df.columns:
                            print(f"  最良パラメータ: EV={best['ev_threshold']}, ν={best['nu_threshold']}")
                    print(opt_df.head(10).to_string())
                    print()
                except Exception as e:
                    print(f"エラー ({opt_file}): {e}")
                    print()
        
        # 分析結果をCSVで保存
        output_path = 'keibaai/data/evaluation/analysis_summary.csv'
        summary_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"分析結果を保存: {output_path}")
        
        return summary_df
    
    return None

if __name__ == '__main__':
    analyze_evaluation_results()
