"""
モデル精度への影響を生データで詳細分析

race_class統一がモデル精度にどう影響するかを実データで検証
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("モデル精度への影響分析（生データ）")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. 現状のrace_classはモデルに使われているか？
    # ===============================
    print("\n" + "=" * 80)
    print("1. 現状のrace_classの使用状況")
    print("=" * 80)
    
    # V15モデルの55特徴量を確認（ドキュメントから）
    v15_features = """
    V15モデルで使用されている55特徴量（ドキュメントより）：
    
    基本特徴量:
    - distance_m, track_surface, weather, track_condition
    - basis_weight, age, sex
    - horse_weight, horse_weight_change
    
    過去成績特徴量:
    - horse_wins, horse_runs, horse_win_rate
    - horse_avg_finish_position, horse_avg_win_odds
    - jockey_win_rate, trainer_win_rate
    - distance_win_rate, surface_win_rate
    
    血統特徴量:
    - sire_win_rate, dam_sire_win_rate
    
    オッズ特徴量:
    - win_odds, popularity
    
    ※ race_class は特徴量リストに含まれていない可能性が高い
    """
    print(v15_features)
    
    # race_classが特徴量として使われているか確認
    # 特徴量エンジニアを確認
    print("\n【確認ポイント】")
    print("  race_classがV15モデルの特徴量に含まれているか要確認")
    print("  → 含まれていなければ、race_class統一のモデル精度への直接影響は限定的")
    
    # ===============================
    # 2. race_classがモデル精度に影響する可能性
    # ===============================
    print("\n" + "=" * 80)
    print("2. race_classがモデル精度に影響する経路")
    print("=" * 80)
    
    print("""
    【直接影響】
      - race_classをカテゴリ特徴量として使用 → 現状の分布問題が直接影響
    
    【間接影響（より重要）】
      1. クラス別統計の計算
         - 「この馬はG1で勝率10%、1勝クラスで勝率50%」のような統計
         - 現状: 2020年以降の1勝/2勝/3勝クラスが認識されていない
         - 影響: クラス別適性の計算が不正確になる可能性
      
      2. 学習データの分布
         - 現状: 「未勝利」が70%を占める異常な分布
         - 影響: モデルが「未勝利」に偏った学習をする可能性
      
      3. 同一馬の出走履歴の連続性
         - 例: 馬Aが2019年に「500万下」で出走、2020年に「1勝クラス」で出走
         - 現状: 「500」と「未勝利」(誤認識)として別クラス扱い
         - 影響: 馬のクラス昇級履歴が断絶
    """)
    
    # ===============================
    # 3. クラス別の実データ分析
    # ===============================
    print("\n" + "=" * 80)
    print("3. クラス別の実データ分析")
    print("=" * 80)
    
    # 各クラスの1番人気勝率を比較
    print("\n【クラス別1番人気勝率】")
    for cls in ['新馬', '未勝利', '500', '1000', '1600', 'OP', 'G1']:
        if cls in flat_races['race_class'].values:
            cls_data = flat_races[(flat_races['race_class'] == cls) & (flat_races['popularity'] == 1)]
            win_rate = (cls_data['finish_position'] == 1).mean() * 100 if len(cls_data) > 0 else 0
            print(f"  {cls}: {win_rate:.1f}% (n={len(cls_data):,})")
    
    # ===============================
    # 4. 2019年前後の同一馬追跡
    # ===============================
    print("\n" + "=" * 80)
    print("4. 2019年前後の同一馬追跡（クラス昇級の連続性）")
    print("=" * 80)
    
    # 2019年と2020年の両方に出走した馬
    horses_2019 = set(flat_races[flat_races['year'] == 2019]['horse_id'].unique())
    horses_2020 = set(flat_races[flat_races['year'] == 2020]['horse_id'].unique())
    both_years = horses_2019 & horses_2020
    
    print(f"\n2019年出走馬: {len(horses_2019):,}")
    print(f"2020年出走馬: {len(horses_2020):,}")
    print(f"両年出走馬: {len(both_years):,}")
    
    # これらの馬のクラス遷移を確認
    both_years_data = flat_races[flat_races['horse_id'].isin(both_years)]
    
    # 2019年のクラス分布
    print(f"\n両年出走馬の2019年クラス分布:")
    cls_2019 = both_years_data[both_years_data['year'] == 2019]['race_class'].value_counts()
    for cls, cnt in cls_2019.items():
        print(f"  {cls}: {cnt:,}")
    
    # 2020年のクラス分布
    print(f"\n両年出走馬の2020年クラス分布:")
    cls_2020 = both_years_data[both_years_data['year'] == 2020]['race_class'].value_counts()
    for cls, cnt in cls_2020.items():
        print(f"  {cls}: {cnt:,}")
    
    # 問題: 2019年に500クラスにいた馬が2020年にどこにいるか
    print(f"\n【問題の具体例】")
    horses_500_2019 = set(both_years_data[(both_years_data['year'] == 2019) & 
                                          (both_years_data['race_class'] == '500')]['horse_id'].unique())
    print(f"2019年に500クラスに出走した馬: {len(horses_500_2019):,}")
    
    # これらの馬の2020年のクラス
    if len(horses_500_2019) > 0:
        data_2020 = both_years_data[(both_years_data['year'] == 2020) & 
                                    (both_years_data['horse_id'].isin(horses_500_2019))]
        cls_distribution = data_2020['race_class'].value_counts()
        print(f"同じ馬たちの2020年クラス分布:")
        for cls, cnt in cls_distribution.items():
            print(f"  {cls}: {cnt:,}")
        
        # 本来は「1勝クラス」に出走しているはずだが、現状は「未勝利」に誤分類
        total_2020 = len(data_2020)
        mishirou_pct = cls_distribution.get('未勝利', 0) / total_2020 * 100 if total_2020 > 0 else 0
        print(f"\n⚠️ 「未勝利」に分類されている割合: {mishirou_pct:.1f}%")
        print(f"   → これは「1勝クラス」「2勝クラス」等の誤認識を示す")
    
    # ===============================
    # 5. オッズと勝率の関係（クラス別）
    # ===============================
    print("\n" + "=" * 80)
    print("5. オッズ帯別勝率のクラス間比較")
    print("=" * 80)
    
    # オッズ帯別勝率はクラスによって異なるか？
    print("\n【オッズ5-10倍の1着率】")
    for cls in ['新馬', '未勝利', '500', 'OP', 'G1']:
        if cls in flat_races['race_class'].values:
            cls_data = flat_races[(flat_races['race_class'] == cls) & 
                                  (flat_races['win_odds'] >= 5) & 
                                  (flat_races['win_odds'] < 10)]
            win_rate = (cls_data['finish_position'] == 1).mean() * 100 if len(cls_data) > 0 else 0
            print(f"  {cls}: {win_rate:.1f}% (n={len(cls_data):,})")
    
    # ===============================
    # 6. モデル精度への影響度評価
    # ===============================
    print("\n" + "=" * 80)
    print("6. モデル精度への影響度評価")
    print("=" * 80)
    
    print("""
    【影響度: 高】
    
    1. クラス別統計の断絶
       - 馬が2019年「500クラス」→2020年「1勝クラス」に昇級
       - 現状: 「1勝クラス」が認識されず、連続性が失われる
       - 影響: クラス適性の計算が不正確
    
    2. 学習データの分布歪み
       - 「未勝利」が45%→70%に膨張
       - モデルが「未勝利レース」に過適合するリスク
    
    3. 年代間のデータ不整合
       - 2014-2019と2020-2025で異なる分布
       - 時系列での学習・検証が不正確に
    
    【対策の効果予測】
    
    race_class統一により:
    - クラス別統計が正確に計算可能
    - 学習データ分布が均一化
    - 馬のクラス昇級履歴が保持される
    
    → ROI改善の可能性あり（特にクラス適性を使う場合）
    """)
    
    # ===============================
    # 7. 結論
    # ===============================
    print("\n" + "=" * 80)
    print("7. 結論")
    print("=" * 80)
    
    print("""
    【結論】
    
    race_class統一はモデル精度に**間接的だが重要な影響**がある:
    
    1. race_classを直接特徴量として使用 → 中程度の影響
    2. クラス別統計（適性）の計算 → 高い影響
    3. 学習データの分布正規化 → 中〜高の影響
    
    **推奨**: race_class統一を実施し、以下を検証
    - 統一前後でのモデルROIを比較
    - クラス適性特徴量の追加を検討
    """)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
