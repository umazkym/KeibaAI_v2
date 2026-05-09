import pandas as pd

try:
    df = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_phase_f_corrected.csv')
    
    # 全体ROI（総払戻 / 総ベット）
    total_bet = df['total_bet'].sum()
    total_return = df['total_return'].sum()
    overall_roi = total_return / total_bet if total_bet > 0 else 0
    
    # Hit Rate
    mean_hit_rate = df['hit_rate'].mean()
    
    print("=== Phase F-Corrected (Sigmoid除去版) Evaluation Results ===")
    print(f"Total Races: {df['race_count'].sum()}")
    print(f"Total Bet: {total_bet} JPY")
    print(f"Total Return: {total_return} JPY")
    print(f"Overall ROI: {overall_roi:.2%}")
    print(f"Mean Hit Rate: {mean_hit_rate:.2%}")
    
except Exception as e:
    print(f"Error: {e}")
