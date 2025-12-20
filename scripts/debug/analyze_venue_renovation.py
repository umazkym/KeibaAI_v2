"""
競馬場改修前後のデータ傾向分析

データ期間（2014-2025）に関係する改修:
1. 京都: 2020年秋～2023年春（2021-2022は完全休催、2023年4月22日グランドオープン）
   → 改修前: 2014-2020、改修後: 2023-2025
2. 阪神: 2024年春～2025年2月（2025年3月1日再開）
   → 改修前: 2014-2024、改修後: 2025年3月以降（データ少）
3. 中山: 2014年前後（2014年秋頃完了）
   → データ期間開始時点で改修完了済み、比較困難

分析項目:
- 平均着順傾向
- 1番人気勝率
- 平均オッズ
- 上がり3F傾向
- 距離別勝率
- 枠番別傾向
- 逃げ/差しの成功率
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def load_data():
    """データ読み込み"""
    print("データ読み込み中...")
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    races['month'] = races['race_date'].dt.month
    races['ym'] = races['race_date'].dt.to_period('M').astype(str)
    
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    print(f"  races: {len(races):,}件")
    return races, corners, race_details

def analyze_kyoto_renovation(races, corners, race_details):
    """京都競馬場の改修前後分析"""
    print_section("京都競馬場の改修前後分析")
    print("  改修期間: 2020年秋～2023年春")
    print("  改修前: 2014-2020")
    print("  改修後: 2023年5月以降")
    
    kyoto = races[races['venue'] == '京都'].copy()
    
    # 期間設定
    kyoto['period'] = 'construction'
    kyoto.loc[kyoto['race_date'] < '2020-11-01', 'period'] = 'before'
    kyoto.loc[kyoto['race_date'] >= '2023-05-01', 'period'] = 'after'
    
    # 期間別データ量
    print(f"\n  【データ量】")
    period_counts = kyoto.groupby('period').size()
    for period in ['before', 'construction', 'after']:
        count = period_counts.get(period, 0)
        print(f"    {period}: {count:,}件")
    
    # 改修前後のみで分析
    kyoto_analysis = kyoto[kyoto['period'].isin(['before', 'after'])]
    
    # 基本統計
    print(f"\n  【基本統計比較】")
    print(f"  {'指標':<25} {'改修前':>12} {'改修後':>12} {'差分':>12}")
    print("-" * 65)
    
    before = kyoto_analysis[kyoto_analysis['period'] == 'before']
    after = kyoto_analysis[kyoto_analysis['period'] == 'after']
    
    # 1番人気勝率
    before_fav_win = (before[before['popularity'] == 1]['finish_position'] == 1).mean() * 100
    after_fav_win = (after[after['popularity'] == 1]['finish_position'] == 1).mean() * 100
    diff = after_fav_win - before_fav_win
    flag = "⚠️" if abs(diff) > 3 else ""
    print(f"  {'1番人気勝率 (%)':25} {before_fav_win:>11.1f}% {after_fav_win:>11.1f}% {diff:>+11.1f}% {flag}")
    
    # 平均配当
    before_odds = before[before['finish_position'] == 1]['win_odds'].mean()
    after_odds = after[after['finish_position'] == 1]['win_odds'].mean()
    diff = after_odds - before_odds
    flag = "⚠️" if abs(diff) > 2 else ""
    print(f"  {'1着馬平均オッズ':25} {before_odds:>12.1f} {after_odds:>12.1f} {diff:>+12.1f} {flag}")
    
    # 上がり3F
    before_3f = before['last_3f_time'].mean()
    after_3f = after['last_3f_time'].mean()
    diff = after_3f - before_3f
    flag = "⚠️" if abs(diff) > 0.3 else ""
    print(f"  {'平均上がり3F (秒)':25} {before_3f:>12.2f} {after_3f:>12.2f} {diff:>+12.2f} {flag}")
    
    # 平均タイム（距離補正が必要だが、ここでは1600m芝で比較）
    before_1600 = before[(before['distance_m'] == 1600) & (before['track_surface'] == '芝')]
    after_1600 = after[(after['distance_m'] == 1600) & (after['track_surface'] == '芝')]
    if len(before_1600) > 0 and len(after_1600) > 0:
        before_time = before_1600['finish_time_seconds'].mean()
        after_time = after_1600['finish_time_seconds'].mean()
        diff = after_time - before_time
        flag = "⚠️" if abs(diff) > 0.5 else ""
        print(f"  {'芝1600m平均タイム (秒)':25} {before_time:>12.2f} {after_time:>12.2f} {diff:>+12.2f} {flag}")
    
    # 馬場別・距離別分析
    print(f"\n  【馬場別・距離別 1番人気勝率】")
    for surface in ['芝', 'ダート']:
        print(f"\n  {surface}:")
        for dist in [1200, 1400, 1600, 1800, 2000, 2200, 2400]:
            before_d = before[(before['track_surface'] == surface) & (before['distance_m'] == dist)]
            after_d = after[(after['track_surface'] == surface) & (after['distance_m'] == dist)]
            
            if len(before_d) >= 50 and len(after_d) >= 50:
                before_w = (before_d[before_d['popularity'] == 1]['finish_position'] == 1).mean() * 100
                after_w = (after_d[after_d['popularity'] == 1]['finish_position'] == 1).mean() * 100
                diff = after_w - before_w
                flag = "⚠️" if abs(diff) > 5 else ""
                print(f"    {dist}m: {before_w:.1f}% → {after_w:.1f}% ({diff:+.1f}%) n={len(before_d)}/{len(after_d)} {flag}")
    
    # 枠番別分析
    print(f"\n  【枠番別 1着率】")
    print(f"  {'枠番':<6} {'改修前':>10} {'改修後':>10} {'差分':>10}")
    print("-" * 40)
    for bracket in range(1, 9):
        before_b = before[before['bracket_number'] == bracket]
        after_b = after[after['bracket_number'] == bracket]
        
        if len(before_b) > 0 and len(after_b) > 0:
            before_w = (before_b['finish_position'] == 1).mean() * 100
            after_w = (after_b['finish_position'] == 1).mean() * 100
            diff = after_w - before_w
            flag = "⚠️" if abs(diff) > 2 else ""
            print(f"  {bracket}枠      {before_w:>9.1f}% {after_w:>9.1f}% {diff:>+9.1f}% {flag}")
    
    # コーナー通過位置分析（逃げ/差し傾向）
    print(f"\n  【脚質別成功率（4コーナー位置）】")
    
    # cornersデータをマージ
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_pos']
    
    kyoto_c4 = kyoto_analysis.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # 脚質分類
    kyoto_c4['running_style'] = pd.cut(
        kyoto_c4['c4_pos'],
        bins=[0, 2, 5, 10, 99],
        labels=['逃げ先行(1-2)', '好位(3-5)', '中団(6-10)', '後方(11-)']
    )
    
    print(f"  {'脚質':<15} {'改修前勝率':>12} {'改修後勝率':>12} {'差分':>10}")
    print("-" * 55)
    
    for style in ['逃げ先行(1-2)', '好位(3-5)', '中団(6-10)', '後方(11-)']:
        before_s = kyoto_c4[(kyoto_c4['period'] == 'before') & (kyoto_c4['running_style'] == style)]
        after_s = kyoto_c4[(kyoto_c4['period'] == 'after') & (kyoto_c4['running_style'] == style)]
        
        if len(before_s) > 0 and len(after_s) > 0:
            before_w = (before_s['finish_position'] == 1).mean() * 100
            after_w = (after_s['finish_position'] == 1).mean() * 100
            diff = after_w - before_w
            flag = "⚠️" if abs(diff) > 3 else ""
            print(f"  {style:<15} {before_w:>11.1f}% {after_w:>11.1f}% {diff:>+9.1f}% {flag}")
    
    return kyoto_analysis

def analyze_hanshin_renovation(races, corners, race_details):
    """阪神競馬場の改修前後分析"""
    print_section("阪神競馬場の改修前後分析")
    print("  改修期間: 2024年春～2025年2月")
    print("  改修前: 2014-2024年春")
    print("  改修後: 2025年3月以降（データ限定的）")
    
    hanshin = races[races['venue'] == '阪神'].copy()
    
    # 期間設定
    hanshin['period'] = 'construction'
    hanshin.loc[hanshin['race_date'] < '2024-04-01', 'period'] = 'before'
    hanshin.loc[hanshin['race_date'] >= '2025-03-01', 'period'] = 'after'
    
    # 期間別データ量
    print(f"\n  【データ量】")
    period_counts = hanshin.groupby('period').size()
    for period in ['before', 'construction', 'after']:
        count = period_counts.get(period, 0)
        print(f"    {period}: {count:,}件")
    
    # 改修後データが少ない場合の警告
    after_count = period_counts.get('after', 0)
    if after_count < 1000:
        print(f"\n  ⚠️ 改修後データが{after_count}件と少ないため、統計的に信頼性が低い")
        print(f"     分析は参考程度にしてください")
    
    # 改修前後で分析可能な場合
    if after_count > 0:
        before = hanshin[hanshin['period'] == 'before']
        after = hanshin[hanshin['period'] == 'after']
        
        print(f"\n  【基本統計比較（参考）】")
        before_fav = (before[before['popularity'] == 1]['finish_position'] == 1).mean() * 100
        after_fav = (after[after['popularity'] == 1]['finish_position'] == 1).mean() * 100
        print(f"    1番人気勝率: {before_fav:.1f}% → {after_fav:.1f}%")
    
    return hanshin

def analyze_nakayama(races, corners, race_details):
    """中山競馬場の分析"""
    print_section("中山競馬場の分析")
    print("  改修完了: 2014年秋頃")
    print("  ※データ期間(2014-)開始時点で改修済みのため、改修前データなし")
    
    nakayama = races[races['venue'] == '中山'].copy()
    
    print(f"\n  【データ量】")
    print(f"    総レコード数: {len(nakayama):,}")
    print(f"    ユニークレース数: {nakayama['race_id'].nunique():,}")
    
    # 年別傾向（改修直後の2014-2015と安定期の2019-2024で比較）
    print(f"\n  【年別1番人気勝率推移】")
    for year in sorted(nakayama['year'].unique()):
        year_data = nakayama[nakayama['year'] == year]
        fav_win = (year_data[year_data['popularity'] == 1]['finish_position'] == 1).mean() * 100
        print(f"    {year}: {fav_win:.1f}%")
    
    return nakayama

def compare_all_venues(races):
    """全競馬場の一貫性比較"""
    print_section("全競馬場の一貫性比較（変動幅分析）")
    
    print(f"\n  各競馬場の年別1番人気勝率の変動幅（標準偏差）")
    print(f"  {'競馬場':<10} {'サンプル数':>12} {'平均勝率':>10} {'標準偏差':>10} {'変動評価':>12}")
    print("-" * 60)
    
    results = []
    for venue in races['venue'].unique():
        venue_data = races[races['venue'] == venue]
        yearly_rates = []
        for year in venue_data['year'].unique():
            year_data = venue_data[venue_data['year'] == year]
            fav = year_data[year_data['popularity'] == 1]
            if len(fav) >= 100:
                rate = (fav['finish_position'] == 1).mean() * 100
                yearly_rates.append(rate)
        
        if len(yearly_rates) >= 5:
            mean_rate = np.mean(yearly_rates)
            std_rate = np.std(yearly_rates)
            n_samples = len(venue_data)
            
            # 変動評価
            if std_rate < 2:
                evaluation = "安定"
            elif std_rate < 4:
                evaluation = "やや変動"
            else:
                evaluation = "変動大 ⚠️"
            
            results.append({
                'venue': venue,
                'n': n_samples,
                'mean': mean_rate,
                'std': std_rate,
                'eval': evaluation
            })
    
    results = sorted(results, key=lambda x: x['std'], reverse=True)
    for r in results:
        print(f"  {r['venue']:<10} {r['n']:>12,} {r['mean']:>9.1f}% {r['std']:>9.2f}% {r['eval']:>12}")

def recommend_venue_separation(races, corners, race_details):
    """競馬場分離の推奨判断"""
    print_section("競馬場分離の推奨判断")
    
    # 京都の改修前後差を再計算
    kyoto = races[races['venue'] == '京都'].copy()
    before = kyoto[kyoto['race_date'] < '2020-11-01']
    after = kyoto[kyoto['race_date'] >= '2023-05-01']
    
    print(f"\n  【京都競馬場の改修前後比較サマリー】")
    print(f"    改修前データ: {len(before):,}件 (2014-2020)")
    print(f"    改修後データ: {len(after):,}件 (2023.5-2025)")
    
    # 統計的な差異テスト（簡易版）
    metrics = []
    
    # 1番人気勝率
    before_fav = (before[before['popularity'] == 1]['finish_position'] == 1).mean() * 100
    after_fav = (after[after['popularity'] == 1]['finish_position'] == 1).mean() * 100
    diff_fav = after_fav - before_fav
    metrics.append(('1番人気勝率', diff_fav, 3.0))
    
    # 上がり3F
    before_3f = before['last_3f_time'].mean()
    after_3f = after['last_3f_time'].mean()
    diff_3f = after_3f - before_3f
    metrics.append(('上がり3F', diff_3f, 0.3))
    
    # 枠番偏り（内枠1-4 vs 外枠5-8）
    before_inner = (before[before['bracket_number'] <= 4]['finish_position'] == 1).mean() * 100
    before_outer = (before[before['bracket_number'] >= 5]['finish_position'] == 1).mean() * 100
    before_bias = before_inner - before_outer
    
    after_inner = (after[after['bracket_number'] <= 4]['finish_position'] == 1).mean() * 100
    after_outer = (after[after['bracket_number'] >= 5]['finish_position'] == 1).mean() * 100
    after_bias = after_inner - after_outer
    
    diff_bias = after_bias - before_bias
    metrics.append(('内外枠バイアス差', diff_bias, 2.0))
    
    print(f"\n  【差異評価】")
    significant_diffs = 0
    for name, diff, threshold in metrics:
        is_sig = abs(diff) > threshold
        if is_sig:
            significant_diffs += 1
        flag = "⚠️ 有意" if is_sig else "  許容範囲"
        print(f"    {name}: {diff:+.2f} (閾値: ±{threshold}) {flag}")
    
    # 推奨判断
    print(f"\n  【推奨判断】")
    
    if significant_diffs >= 2:
        print(f"""
    ⚠️ 京都競馬場の改修前後で有意な差異が{significant_diffs}項目確認されました。
    
    【分離を推奨】
    - 「京都_旧」(2014-2020) と「京都_新」(2023-) として別競馬場扱い
    
    【実装方法案】
    ```python
    if venue == '京都':
        if race_date < '2020-11-01':
            venue = '京都_旧'
        elif race_date >= '2023-05-01':
            venue = '京都_新'
        else:
            # 工事期間(2020.11-2023.4)はデータなし
            venue = '京都_工事'  # または除外
    ```
    
    【サンプルサイズへの影響】
    - 京都_旧: {len(before):,}件
    - 京都_新: {len(after):,}件
    - 両方とも十分なサンプルサイズを維持
""")
    else:
        print(f"""
    ✓ 京都競馬場の改修前後で有意な差異は{significant_diffs}項目のみです。
    
    【分離は不要と判断】
    - 改修前後を同一競馬場として扱っても大きな問題はない
    - ただし、将来的なモニタリングは継続すべき
""")
    
    # 阪神についての判断
    print(f"\n  【阪神競馬場について】")
    print(f"""
    ⏳ 改修後データが2025年3月以降のため、現時点では判断保留
    
    【対応方針】
    - 2025年末までデータを蓄積してから再評価
    - 現時点では分離不要
""")

def analyze_data_volume_impact():
    """データ量への影響シミュレーション"""
    print_section("データ量への影響シミュレーション")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    original_count = len(races)
    
    # 現状のvenueユニーク数
    original_venues = races['venue'].nunique()
    
    # 京都を分離した場合
    races_sim = races.copy()
    kyoto_mask = races_sim['venue'] == '京都'
    races_sim.loc[kyoto_mask & (races_sim['race_date'] < '2020-11-01'), 'venue'] = '京都_旧'
    races_sim.loc[kyoto_mask & (races_sim['race_date'] >= '2023-05-01'), 'venue'] = '京都_新'
    
    new_venues = races_sim['venue'].nunique()
    
    print(f"\n  【venue数の変化】")
    print(f"    現状: {original_venues}競馬場")
    print(f"    分離後: {new_venues}競馬場（+{new_venues - original_venues}）")
    
    # 各venueのデータ量
    print(f"\n  【分離後のデータ量】")
    venue_counts = races_sim.groupby('venue').size().sort_values(ascending=False)
    for venue, count in venue_counts.items():
        pct = count / original_count * 100
        print(f"    {venue}: {count:,}件 ({pct:.1f}%)")
    
    # 最小サンプル数
    min_venue = venue_counts.min()
    min_venue_name = venue_counts.idxmin()
    
    print(f"\n  【最小サンプル競馬場】")
    print(f"    {min_venue_name}: {min_venue:,}件")
    
    if min_venue < 10000:
        print(f"    ⚠️ 10,000件未満のため、特徴量エンジニアリングに影響の可能性")
    else:
        print(f"    ✓ 十分なサンプルサイズを維持")

def main():
    print("=" * 80)
    print("  競馬場改修前後のデータ傾向分析")
    print("=" * 80)
    
    races, corners, race_details = load_data()
    
    # 京都競馬場の詳細分析
    analyze_kyoto_renovation(races, corners, race_details)
    
    # 阪神競馬場の分析
    analyze_hanshin_renovation(races, corners, race_details)
    
    # 中山競馬場の分析
    analyze_nakayama(races, corners, race_details)
    
    # 全競馬場の一貫性比較
    compare_all_venues(races)
    
    # 分離推奨判断
    recommend_venue_separation(races, corners, race_details)
    
    # データ量への影響
    analyze_data_volume_impact()
    
    print("\n" + "=" * 80)
    print("  分析完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
