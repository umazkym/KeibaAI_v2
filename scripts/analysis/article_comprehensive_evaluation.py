#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
記事提案の網羅的評価スクリプト

記事の全提案について、KeibaAI_v2での実装状況と
今回の検証結果を整理する。

【第1部: 基本戦略】
1. セグメント分割（芝/ダート、距離、年齢）
2. タイム指数化
3. 順位正規化
4. 騎手/血統の簡略化
5. 穴馬モデル
6. 第8-12レース絞り込み

【第2部: 改善案】
1. Entity Embeddings
2. Learning to Rank
3. LSTM/Transformer
4. GNN
5. オッズ分離
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")
OUTPUT_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\outputs\analysis")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

def calc_roi(df, win_col='is_win', odds_col='win_odds'):
    if len(df) == 0:
        return 0, 0, 0
    win_rate = df[win_col].mean()
    winners = df[df[win_col] == 1]
    total_return = (winners[odds_col] * 100).sum()
    total_bet = len(df) * 100
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    return win_rate, roi, len(df)

def main():
    results = []
    
    def log(msg):
        results.append(msg)
        print(msg)
    
    log("=" * 100)
    log("記事提案の網羅的評価レポート")
    log("=" * 100)
    log("")
    
    # データ読み込み
    log("データ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['is_win'] = (races['finish_position'] == 1).astype(int)
    races = races.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    races['year'] = races['race_date'].dt.year
    
    # race_numberを抽出（race_idから）
    races['race_number'] = races['race_id'].astype(str).str[-2:].astype(int)
    
    log(f"総レコード数: {len(races):,}件")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【第1部: 基本戦略】")
    log("=" * 100)
    # ========================================================================
    
    # 1. セグメント分割
    log("\n--- 1. セグメント分割（芝/ダート、距離、年齢）---")
    log("■ 検証結果: ROI差が小さく、分割のメリットなし")
    log("■ KeibaAI_v2での状態: 特徴量として使用（分割はしない）")
    log("■ 採用状況: 特徴量として使用 ✅（モデル分割は非採用 ❌）")
    
    # 2. タイム指数化
    log("\n--- 2. タイム指数化（基準タイムとの差、上がり3F指数）---")
    log("■ KeibaAI_v2での確認:")
    
    # 上がり3Fの存在確認
    if 'last_3f_time' in races.columns:
        valid_3f = races['last_3f_time'].notna().sum()
        log(f"  last_3f_time: {valid_3f:,}件のデータあり ({valid_3f/len(races)*100:.1f}%)")
        
        # 上がり3Fと勝率の関係（年度別）
        log("\n  上がり3F時間と勝率の関係（年度安定性）:")
        for year in [2022, 2023, 2024]:
            year_data = races[(races['year'] == year) & races['last_3f_time'].notna()]
            if len(year_data) > 100:
                fast = year_data[year_data['last_3f_time'] <= year_data['last_3f_time'].quantile(0.2)]
                slow = year_data[year_data['last_3f_time'] >= year_data['last_3f_time'].quantile(0.8)]
                fast_wr = fast['is_win'].mean() * 100
                slow_wr = slow['is_win'].mean() * 100
                log(f"  {year}年: 上がりTop20% {fast_wr:.1f}% vs Bottom20% {slow_wr:.1f}% (差: {fast_wr-slow_wr:.1f}pt)")
    else:
        log("  last_3f_time: カラムなし")
    
    log("■ 注意: 当該レースの上がり3Fは「結果情報」→ リーク ❌")
    log("■ 正しい使い方: 過去レースの上がり3Fを累積統計として使用")
    log("■ 採用状況: 過去上がり3F統計として検討価値あり △")
    
    # 3. 順位正規化（レース内での相対順位）
    log("\n--- 3. 順位正規化（0-1範囲）---")
    log("■ KeibaAI_v2での状態: LambdaRankで相対順位を学習済み")
    log("■ 採用状況: 既に実装済み ✅")
    
    # 4. 騎手/血統の簡略化
    log("\n--- 4. 騎手/血統の簡略化（リーディング順位など）---")
    log("■ 検証結果: 累積勝率が4/5年安定 (+5.2pt)")
    log("■ 採用状況: 累積勝率として採用 ✅")
    
    # 5. 穴馬モデル
    log("\n--- 5. 穴馬モデル（単勝10倍以上で好走する確率）---")
    log("■ KeibaAI_v2での状態: V4.4 Residualが「穴馬発見」役を担当")
    log("■ 検証: オッズ10-20倍帯のROI安定性:")
    
    for year in [2020, 2021, 2022, 2023, 2024]:
        df = races[(races['year'] == year) & (races['win_odds'] >= 10) & (races['win_odds'] < 20)]
        _, roi, n = calc_roi(df)
        log(f"  {year}年: ROI {roi:.1f}% ({n:,}件)")
    
    log("■ 採用状況: V4.4 Residualで既に実装済み ✅")
    
    # 6. 第8-12レース絞り込み
    log("\n--- 6. 第8〜12レースに絞る ---")
    log("■ 検証: レース番号別ROI（年度安定性）:")
    
    log(f"{'年度':>6} | {'R1-7':>10} | {'R8-12':>10} | {'差':>10}")
    log("-" * 50)
    
    yearly_diffs = []
    for year in [2020, 2021, 2022, 2023, 2024]:
        year_data = races[races['year'] == year]
        early = year_data[year_data['race_number'] <= 7]
        late = year_data[(year_data['race_number'] >= 8) & (year_data['race_number'] <= 12)]
        
        early_roi = calc_roi(early)[1]
        late_roi = calc_roi(late)[1]
        diff = late_roi - early_roi
        yearly_diffs.append(diff)
        
        log(f"{year:>6} | ROI {early_roi:>5.1f}% | ROI {late_roi:>5.1f}% | {diff:>+6.1f}pt")
    
    log(f"\n平均差: {np.mean(yearly_diffs):+.1f}pt, 標準偏差: {np.std(yearly_diffs):.1f}pt")
    log(f"後半レースが優位な年: {sum(1 for d in yearly_diffs if d > 0)}/{len(yearly_diffs)}年")
    log("■ 採用状況: 効果不安定のため見送り △")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【第2部: 改善案】")
    log("=" * 100)
    # ========================================================================
    
    # 1. Entity Embeddings
    log("\n--- 1. Entity Embeddings（騎手/種牡馬/調教師）---")
    log("■ 検証結果: 種牡馬×距離は時系列分割で+4.4pt差に縮小（期待外れ）")
    log("■ 代替案: 累積勝率を使用（シンプルで効果安定）")
    log("■ 採用状況: 累積勝率で代替 ✅（Embeddingsは見送り △）")
    
    # 2. Learning to Rank
    log("\n--- 2. Learning to Rank（LambdaRank）---")
    log("■ KeibaAI_v2での状態: V4.4で実装済み")
    log("■ 採用状況: 既に実装済み ✅")
    
    # 3. LSTM/Transformer
    log("\n--- 3. LSTM/Transformer（時系列シーケンス）---")
    log("■ 検証結果: 前走着順×休養期間の単純特徴量で4/5年安定")
    log("■ LSTM/Transformerの追加価値: 限定的")
    log("■ 採用状況: 単純特徴量で代替 ✅（LSTM/Transformerは見送り △）")
    
    # 4. GNN
    log("\n--- 4. GNN（展開予測）---")
    log("■ 4コーナー位置: 「結果情報」であり使用不可 ❌")
    log("■ 過去脚質からの推定: 検証価値あり")
    log("■ 採用状況: 過去脚質からのハイペース確率を検討 △")
    
    # 5. オッズ分離
    log("\n--- 5. オッズ分離---")
    log("■ KeibaAI_v2での状態: 既に実装済み（モデルにオッズ入力なし）")
    log("■ EVフィルタリング: 予測後に適用")
    log("■ 採用状況: 既に実装済み ✅")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【未検証項目の追加検証】")
    log("=" * 100)
    # ========================================================================
    
    # クラスレベル（対戦相手の強さ）
    log("\n--- クラスレベル特徴量（記事の改善案B）---")
    log("■ 提案: 過去1年間のレースレベル（対戦相手の平均レーティング）")
    
    # race_classの分布確認
    if 'race_class' in races.columns:
        log("\n  レースクラス分布:")
        class_dist = races['race_class'].value_counts()
        for cls, count in class_dist.head(10).items():
            log(f"    {cls}: {count:,}件")
        
        # クラス別勝率（年度安定性）
        log("\n  クラス別ROI（主要クラス）:")
        for cls in ['新馬', '未勝利', '1勝', '2勝', '3勝', 'OP', 'G3', 'G2', 'G1']:
            cls_data = races[races['race_class'] == cls]
            if len(cls_data) > 1000:
                _, roi, n = calc_roi(cls_data)
                log(f"    {cls}: ROI {roi:.1f}% ({n:,}件)")
    
    log("\n■ 検証結果: クラス情報は既にrace_classとして存在")
    log("■ 採用状況: 特徴量として使用可能 ✅")
    
    # ========================================================================
    log("\n" + "=" * 100)
    log("【総合まとめ: 記事提案の採用状況】")
    log("=" * 100)
    # ========================================================================
    
    log("""
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        記事提案の採用状況一覧                                     │
├──────────────────────────────────────────────────────────────────────────────────┤
│                                                                                   │
│ 【第1部: 基本戦略】                                                              │
│ ✅ 既存 | セグメント分割 → 特徴量として使用（モデル分割はしない）               │
│ △ 検討 | タイム指数化 → 過去上がり3F統計として検討価値あり                      │
│ ✅ 既存 | 順位正規化 → LambdaRankで相対順位学習済み                              │
│ ✅ 採用 | 騎手/血統の簡略化 → 累積勝率として採用（+5.2pt安定効果）               │
│ ✅ 既存 | 穴馬モデル → V4.4 Residualが担当                                       │
│ △ 見送 | 第8-12レース絞り込み → 効果不安定（2/5年のみプラス）                    │
│                                                                                   │
│ 【第2部: 改善案】                                                                │
│ △ 見送 | Entity Embeddings → 時系列分割で効果縮小、累積勝率で代替               │
│ ✅ 既存 | Learning to Rank → V4.4 LambdaRank実装済み                             │
│ △ 見送 | LSTM/Transformer → 単純特徴量で同等効果                                │
│ △ 検討 | GNN/展開予測 → 過去脚質からのハイペース確率を検討                       │
│ ✅ 既存 | オッズ分離 → 既に実装済み                                              │
│                                                                                   │
│ 【新規検討項目】                                                                 │
│ △ 検討 | 過去上がり3F統計 → 安定性検証が必要                                    │
│ △ 検討 | クラスレベル特徴量 → 対戦相手の強さを数値化                            │
│ △ 検討 | 過去脚質からのハイペース確率                                           │
│                                                                                   │
└──────────────────────────────────────────────────────────────────────────────────┘

【結論】
記事の主要な提案は、KeibaAI_v2で既に実装済みか、今回の検証で採用/見送りを判断済み。

新たに検討価値のある項目:
1. 過去上がり3F統計（過去レースの上がり3Fを累積）
2. 過去脚質からのハイペース確率

これらは追加検証の価値あり。
""")
    
    # 結果をファイルに保存
    output_file = OUTPUT_PATH / "article_comprehensive_evaluation.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(results))
    
    log(f"\n結果を {output_file} に保存しました。")

if __name__ == "__main__":
    main()
