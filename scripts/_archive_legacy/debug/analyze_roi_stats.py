
import pandas as pd
import numpy as np
import sys
import io

def analyze_roi_statistics():
    """
    ev_investment_details_2024.csv を読み込み、様々な角度からROIを分析する。
    分析結果はコンソールとテキストファイルに出力する。
    """
    # --- 設定 ---
    DETAILS_CSV_PATH = "ev_investment_details_2024.csv"
    REPORT_OUTPUT_FILE = "roi_optimization_report.txt"

    # --- 準備 ---
    # stdoutのエンコーディングをUTF-8に設定
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    # 分析結果を格納する文字列バッファ
    report_buffer = io.StringIO()

    # --- データ読み込み ---
    try:
        df = pd.read_csv(DETAILS_CSV_PATH)
    except FileNotFoundError:
        print(f"エラー: 詳細ログファイル {DETAILS_CSV_PATH} が見つかりません。")
        print("先に analyze_ev_investment_2024.py を実行してください。")
        return

    report_buffer.write("--- 投資戦略 パフォーマンス分析レポート ---\n\n")
    report_buffer.write(f"分析対象の投資機会(EV>1.1): {len(df)} 件\n")
    report_buffer.write("-----------------------------------------\n\n")

    # --- 分析ヘルパー関数 ---
    def calculate_metrics(sub_df, title):
        if sub_df.empty:
            report_buffer.write(f"【{title}】\n")
            report_buffer.write("  対象データなし\n\n")
            return

        total_bets = len(sub_df)
        total_investment = sub_df['bet_amount'].sum()
        total_return = sub_df['payout'].sum()
        total_wins = len(sub_df[sub_df['payout'] > 0])
        
        roi = (total_return / total_investment) * 100 if total_investment > 0 else 0
        hit_rate = (total_wins / total_bets) * 100 if total_bets > 0 else 0
        
        avg_odds_bet = sub_df['win_odds'].mean()
        avg_odds_win = sub_df[sub_df['payout'] > 0]['win_odds'].mean() if total_wins > 0 else 0
        
        profit = total_return - total_investment

        report_buffer.write(f"【{title}】\n")
        report_buffer.write(f"  - 投資回数: {total_bets} 回\n")
        report_buffer.write(f"  - 回収率 (ROI): {roi:.2f} %\n")
        report_buffer.write(f"  - 的中率: {hit_rate:.2f} %\n")
        report_buffer.write(f"  - 純利益: {profit:,.0f} 円\n")
        report_buffer.write(f"  - 平均投資オッズ: {avg_odds_bet:.2f}\n")
        report_buffer.write(f"  - 平均的中オッズ: {avg_odds_win:.2f}\n\n")


    # --- 1. 期待値 (EV) の閾値別分析 ---
    report_buffer.write("### 1. 期待値 (EV) の閾値別パフォーマンス ###\n")
    ev_thresholds = [1.1, 1.2, 1.3, 1.4, 1.5, 1.8, 2.0, 2.5, 3.0, 4.0, 5.0]
    for ev in ev_thresholds:
        subset = df[df['expected_value'] > ev]
        calculate_metrics(subset, f"期待値 > {ev}")

    # --- 2. オッズ帯別分析 ---
    report_buffer.write("\n### 2. オッズ帯別パフォーマンス ###\n")
    odds_bins = [
        (0, 5),
        (5, 10),
        (10, 20),
        (20, 50),
        (50, 100),
        (100, 9999)
    ]
    for low, high in odds_bins:
        subset = df[(df['win_odds'] >= low) & (df['win_odds'] < high)]
        calculate_metrics(subset, f"オッズ帯: {low} - {high}")

    # --- 3. モデル予測勝率 (win_prob) 別分析 ---
    report_buffer.write("\n### 3. モデル予測勝率 (win_prob) 別パフォーマンス ###\n")
    prob_bins = [
        (0, 0.1),
        (0.1, 0.2),
        (0.2, 0.3),
        (0.3, 0.4),
        (0.4, 0.5),
        (0.5, 1.0)
    ]
    for low, high in prob_bins:
        subset = df[(df['win_prob'] >= low) & (df['win_prob'] < high)]
        calculate_metrics(subset, f"モデル予測勝率: {low:.1f} - {high:.1f}")

    # --- 4. 複合条件での分析 ---
    report_buffer.write("\n### 4. 複合条件でのパフォーマンス（参考） ###\n")
    
    # 期待値 > 1.5 and オッズ < 50
    subset_a = df[(df['expected_value'] > 1.5) & (df['win_odds'] < 50)]
    calculate_metrics(subset_a, "期待値 > 1.5 AND オッズ < 50")

    # 期待値 > 2.0 and オッズ < 50
    subset_b = df[(df['expected_value'] > 2.0) & (df['win_odds'] < 50)]
    calculate_metrics(subset_b, "期待値 > 2.0 AND オッズ < 50")
    
    # 期待値 > 2.0 and 10 < オッズ < 50
    subset_c = df[(df['expected_value'] > 2.0) & (df['win_odds'] > 10) & (df['win_odds'] < 50)]
    calculate_metrics(subset_c, "期待値 > 2.0 AND 10 < オッズ < 50")

    # --- 5. 期待値の閾値とオッズ上限のマトリクス分析 ---
    report_buffer.write("\n### 5. 期待値の閾値とオッズ上限の最適組み合わせ分析 ###\n")
    report_buffer.write("Format: (ROI %, 純利益, 投資回数)\n")
    ev_thresholds_matrix = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    odds_caps_matrix = [50, 40, 30, 20, 15, 10]

    results_matrix = []
    for ev in ev_thresholds_matrix:
        row = []
        for cap in odds_caps_matrix:
            subset = df[(df['expected_value'] > ev) & (df['win_odds'] < cap)]
            
            if subset.empty:
                row.append("( - )")
                continue

            total_investment = subset['bet_amount'].sum()
            total_return = subset['payout'].sum()
            roi = (total_return / total_investment) * 100 if total_investment > 0 else 0
            profit = total_return - total_investment
            bet_count = len(subset)
            row.append(f"({roi:.1f}%, {profit:,.0f}, {bet_count})")
        results_matrix.append(row)
    
    header = "EV Thresh\\"
    for cap in odds_caps_matrix:
        header += f"Odds<{cap}\t"
    report_buffer.write(header + "\n")
    report_buffer.write("-" * (len(header) + 5*len(odds_caps_matrix)) + "\n")

    for i, ev in enumerate(ev_thresholds_matrix):
        line = f"EV > {ev}\t"
        for res in results_matrix[i]:
            line += f"{res}\t"
        report_buffer.write(line + "\n")


    # --- レポート出力 ---
    final_report = report_buffer.getvalue()
    
    # ファイルに保存
    with open(REPORT_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(final_report)
    
    # コンソールに出力
    print(final_report)
    print(f"\nレポート全体が {REPORT_OUTPUT_FILE} に保存されました。")


if __name__ == '__main__':
    analyze_roi_statistics()
