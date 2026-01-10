# -*- coding: utf-8 -*-
"""
EVCalculatorのテストスクリプト

実際のオッズデータと仮の予測確率を使用して、
EV計算の動作を検証する。
"""
import json
import numpy as np
import sys
sys.path.insert(0, 'c:/Users/zk-ht/Keiba/Keiba_AI_v2')

from keibaai.src.models.ev_calculator import (
    EVCalculator,
    harville_probability,
    calculate_quinella_probability,
    calculate_trio_probability
)


def test_harville_probability():
    """ハービル法の確率計算テスト"""
    print("=== ハービル法テスト ===")
    
    # 5頭立て、勝率が均等の場合
    probs = np.array([0.2, 0.2, 0.2, 0.2, 0.2])
    
    # 各馬が1着になる確率（均等なので各0.2）
    for i in range(5):
        p = harville_probability(probs, [i])
        print(f"  馬番{i+1}が1着: {p:.4f}")
    
    # 馬番1が1着、馬番2が2着の確率
    p_12 = harville_probability(probs, [0, 1])
    print(f"  馬番1→馬番2の確率: {p_12:.4f}")
    
    # 馬連1-2の確率（1→2 + 2→1）
    p_quinella = calculate_quinella_probability(probs, 0, 1)
    print(f"  馬連1-2の確率: {p_quinella:.4f}")
    
    # 理論値確認: 5頭で馬連の組み合わせ数 = 5C2 = 10
    # 均等なら各組み合わせ確率 = 1/10 = 0.1
    print(f"  → 理論値（均等5頭）: 0.1000")
    
    # 不均等な場合
    print("\n--- 不均等な勝率の場合 ---")
    probs2 = np.array([0.4, 0.3, 0.15, 0.1, 0.05])  # 合計=1.0
    
    for i in range(5):
        p = harville_probability(probs2, [i])
        print(f"  馬番{i+1}が1着: {p:.4f}")
    
    p_12 = calculate_quinella_probability(probs2, 0, 1)
    p_34 = calculate_quinella_probability(probs2, 2, 3)
    print(f"  馬連1-2の確率: {p_12:.4f}")
    print(f"  馬連3-4の確率: {p_34:.4f}")


def test_trio_probability():
    """三連複確率のテスト"""
    print("\n=== 三連複確率テスト ===")
    
    # 6頭立て
    probs = np.array([0.3, 0.25, 0.2, 0.15, 0.07, 0.03])
    
    # 全組み合わせの確率合計が1.0になることを確認
    total = 0.0
    from itertools import combinations
    for combo in combinations(range(6), 3):
        p = calculate_trio_probability(probs, combo[0], combo[1], combo[2])
        total += p
    
    print(f"  全20組み合わせの確率合計: {total:.6f}")
    print(f"  → 理論値: 1.0000")
    
    # 上位3頭の三連複確率
    p_123 = calculate_trio_probability(probs, 0, 1, 2)
    print(f"  三連複1-2-3の確率: {p_123:.4f}")


def test_ev_calculation():
    """EV計算のテスト"""
    print("\n=== EV計算テスト ===")
    
    # 実際のオッズデータを読み込み
    odds_path = 'c:/Users/zk-ht/Keiba/Keiba_AI_v2/results/odds_poc_v2/202506050810_odds.json'
    
    try:
        with open(odds_path, 'r', encoding='utf-8') as f:
            odds_json = json.load(f)
        print(f"オッズデータ読み込み成功: {odds_path}")
    except FileNotFoundError:
        print(f"オッズデータが見つかりません: {odds_path}")
        return
    
    # 16頭立ての仮の勝率（1番人気が高い）
    n_horses = 16
    # 人気順に確率を設定（1番人気=0.2, 2番人気=0.15, ...）
    raw_probs = np.array([0.20, 0.15, 0.12, 0.10, 0.08, 0.07, 0.06, 0.05,
                          0.04, 0.03, 0.025, 0.02, 0.015, 0.01, 0.008, 0.002])
    # 正規化
    win_probs = raw_probs / raw_probs.sum()
    horse_numbers = list(range(1, n_horses + 1))
    
    print(f"馬数: {n_horses}")
    print(f"勝率合計: {win_probs.sum():.4f}")
    
    # EVCalculatorのインスタンス化
    calculator = EVCalculator()
    
    # 単勝EV計算
    print("\n--- 単勝EV ---")
    win_ev = calculator.calculate_win_ev(win_probs, odds_json['win_place']['data'], horse_numbers)
    if len(win_ev) > 0:
        win_ev_sorted = win_ev.sort_values('ev', ascending=False).head(5)
        print(win_ev_sorted[['horse_number', 'win_prob', 'odds', 'ev']].to_string(index=False))
    
    # 馬連EV計算（上位10件）
    print("\n--- 馬連EV (上位10件) ---")
    quinella_ev = calculator.calculate_quinella_ev(win_probs, odds_json['quinella']['data'], horse_numbers)
    if len(quinella_ev) > 0:
        quinella_ev_sorted = quinella_ev.sort_values('ev', ascending=False).head(10)
        print(quinella_ev_sorted[['horse1', 'horse2', 'prob', 'odds', 'ev']].to_string(index=False))
    
    # 三連複EV計算（上位10件）
    print("\n--- 三連複EV (上位10件) ---")
    trio_ev = calculator.calculate_trio_ev(win_probs, odds_json['trio']['data'], horse_numbers)
    if len(trio_ev) > 0:
        trio_ev_sorted = trio_ev.sort_values('ev', ascending=False).head(10)
        print(trio_ev_sorted[['horse1', 'horse2', 'horse3', 'prob', 'odds', 'ev']].to_string(index=False))
    
    # EV >= 1.0の購入候補を抽出
    print("\n--- EV >= 1.0 の購入候補 ---")
    all_ev = calculator.calculate_all_ev(win_probs, odds_json, horse_numbers, ev_threshold=1.0)
    
    for bet_type, df in all_ev.items():
        if bet_type == 'summary':
            continue
        if isinstance(df, dict):
            continue
        if len(df) > 0:
            print(f"  {bet_type}: {len(df)}件")


if __name__ == "__main__":
    test_harville_probability()
    test_trio_probability()
    test_ev_calculation()
