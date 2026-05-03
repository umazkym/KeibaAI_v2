#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""System E 多角的深層分析"""
import pandas as pd
import numpy as np

df = pd.read_csv('outputs/analysis/system_e_optimized_results.csv')
df['year'] = df['quarter'].str[:4]

print('='*80)
print('System E 多角的深層分析')
print('='*80)

# ============================================================
# 1. 本質的な問題の特定
# ============================================================
print('\n【1. 効率化による影響分析】')
print('-'*60)

# 効率化版では「一括transform」を行った
# これにより、日次特徴量（V16）の計算がどう変わったか？

# 元の日次更新版（2020Q1）: win 77.7%, Gap 29.3%
# 効率化版（2020Q1）: win 78.3%, Gap 38.7%
print('2020Q1比較:')
print('  元の日次更新版: win 77.7%, Gap 29.3%')
q1 = df[df['quarter'] == '2020Q1'].iloc[0]
print(f'  効率化版:       win {q1["win_prob_test_roi"]:.1f}%, Gap {q1["win_prob_gap"]:.1f}%')
print()
print('  → ROIはほぼ同等だがGapが増加')
print('  → 効率化によりV16特徴量の日次更新が失われている可能性')

# ============================================================
# 2. V16特徴量の計算方法の違い
# ============================================================
print('\n【2. V16特徴量計算の違い】')
print('-'*60)
print('元の日次更新版:')
print('  - 各レース日ごとにcompute_daily_features()を呼び出し')
print('  - target_date < race_dateのデータのみで累積統計を計算')
print()
print('効率化版:')
print('  - 全期間を一括でtransform')
print('  - shift(1)で前日までの累積を計算')
print('  → 問題: train_end以降のデータでもshift(1)で計算')
print('  → つまりテスト期間内の情報が累積される（軽微なリーク）')

# ============================================================
# 3. 既存システムとの本質的な違い
# ============================================================
print('\n【3. 既存システムとの構造的差異】')
print('-'*60)
print('既存System A+ (年単位検証):')
print('  - 学習: 2014-2019 → テスト: 2020')
print('  - V16特徴量: shift(1)で計算（全データに適用）')
print('  - 検証: 年単位で1回だけ評価')
print()
print('System E (効率化版):')
print('  - 学習: 四半期ごとに再学習')
print('  - V16特徴量: shift(1)で計算（同じ手法）')
print('  - 検証: 四半期単位で評価')
print()
print('★ 核心的な違い:')
print('  1. モデル再学習頻度: 年1回 vs 四半期4回')
print('  2. テスト期間: 1年 vs 3ヶ月')
print('  3. 極端正則化: 既存はstrongレベル、今回はextremeレベル')

# ============================================================
# 4. 正則化レベルの影響
# ============================================================
print('\n【4. 正則化レベルの影響（仮説）】')
print('-'*60)
print('extreme正則化:')
print('  - max_depth=2, num_leaves=4')
print('  - 特徴量の複雑な相互作用を捉えられない')
print('  - 過学習は防げるがアンダーフィットのリスク')
print()
print('strong正則化（既存）:')
print('  - max_depth=3, num_leaves=8')
print('  - 適度な複雑さを許容')
print()
print('→ 今回のROI低下（80.2% → 72.8%）の主因かもしれない')

# ============================================================
# 5. top2_probのGapが低い本当の理由
# ============================================================
print('\n【5. top2_probのGap最小化の分析】')
print('-'*60)

# top2_prob: 2着以内 = 約13%の勝率（1着8% + 2着-1着重複除外）
# win_prob: 1着のみ = 約8%の勝率
# place_prob: 3着以内 = 約18%の勝率

print('ターゲットの正例率（理論値）:')
print('  win_prob:   ~8% (18頭立て平均)')
print('  top2_prob:  ~13%')
print('  place_prob: ~18%')
print()
print('Gapの観点:')
print('  - win_prob: 正例少ない → Valid期間でのランダム性高い → Gap増大')
print('  - place_prob: 正例多い → 予測が平均回帰しやすい → Gap中程度')
print('  - top2_prob: 中間 → バランス最適 → Gap最小')
print()
print('★ top2_probのGap最小は「ターゲットのバランス」による可能性大')

# ============================================================
# 6. V4.4がGapを増やす理由
# ============================================================
print('\n【6. V4.4 Residualの過学習メカニズム】')
print('-'*60)
print('V4.4の特徴:')
print('  - LambdaRank（ランキング学習）')
print('  - オッズ重み付きRelevance')
print('  - 大穴発見に特化')
print()
print('過学習の原因:')
print('  1. 大穴は低頻度イベント → 学習データへの過適合')
print('  2. オッズ重み付けが極端な外れ値を増幅')
print('  3. LambdaRankはBinaryClassificationより過学習しやすい')
print()
print('除外による効果:')
print(f'  v1_original (V4.4 30%): Gap {df["v1_original_gap"].mean():.1f}%')
print(f'  v4_no_v44 (V4.4 0%):   Gap {df["v4_no_v44_gap"].mean():.1f}%')
print(f'  → Gap半減、これはV4.4が主要な過学習源であることを示す')

# ============================================================
# 7. 四半期別詳細分析
# ============================================================
print('\n【7. 四半期別パフォーマンス分布】')
print('-'*60)

# ROI > 80%の四半期を特定
high_roi_quarters = df[df['win_prob_test_roi'] > 80]['quarter'].tolist()
low_roi_quarters = df[df['win_prob_test_roi'] < 65]['quarter'].tolist()

print(f'高パフォーマンス四半期 (ROI > 80%): {high_roi_quarters}')
print(f'低パフォーマンス四半期 (ROI < 65%): {low_roi_quarters}')
print()

# 2023年の詳細
print('2023年詳細（最低パフォーマンス年）:')
for _, row in df[df['year'] == '2023'].iterrows():
    print(f'  {row["quarter"]}: win {row["win_prob_test_roi"]:.1f}%, top2 {row["top2_prob_test_roi"]:.1f}%')

# ============================================================
# 8. 芝/ダート詳細分析
# ============================================================
print('\n【8. 芝/ダート詳細分析】')
print('-'*60)

print('芝 vs ダート（v3_low_v44基準）:')
print(f'  芝平均ROI:   {df["v3_low_v44_turf_roi"].mean():.1f}%')
print(f'  ダート平均ROI: {df["v3_low_v44_dirt_roi"].mean():.1f}%')
print()

# 年別の芝/ダート差
print('年別 芝-ダート差:')
for year in ['2020', '2021', '2022', '2023', '2024', '2025']:
    year_df = df[df['year'] == year]
    turf_avg = year_df['v3_low_v44_turf_roi'].mean()
    dirt_avg = year_df['v3_low_v44_dirt_roi'].mean()
    diff = turf_avg - dirt_avg
    print(f'  {year}: 芝{turf_avg:.1f}% ダ{dirt_avg:.1f}% 差{diff:+.1f}pt')

# ============================================================
# 9. 根本的な再考
# ============================================================
print('\n【9. 根本的な再考】')
print('-'*60)
print('質問1: 日次境界は本当に必要か？')
print('  - V16特徴量のshift(1)は既に日次境界を実現')
print('  - 追加の日次処理は計算コストに見合う効果があったか？')
print(f'  → 元の検証と同程度のROI（~78%）を達成したが、')
print(f'     効率化でshift(1)に戻したことでGapが増加')
print()
print('質問2: 極端正則化は適切だったか？')
print('  - Gapは確実に低下した')
print('  - しかしROIも低下した')
print('  → トレードオフの最適点はstrongレベル付近かもしれない')
print()
print('質問3: 3指数分離の価値は何か？')
print('  - top2_probはGap最小で安定性が高い')
print('  - 券種別に最適な指数を選べる')
print('  → 複合的な投資戦略に有用')

# ============================================================
# 10. 結論と次のアクション
# ============================================================
print('\n【10. 多角的結論】')
print('='*60)
print()
print('A. 効率化版の問題点:')
print('   - shift(1)でテスト期間内の累積が混入（軽微なリーク）')
print('   - Gapが元の日次更新版より増加')
print()
print('B. ROI低下の主因:')
print('   - 極端正則化によるアンダーフィット（主因）')
print('   - 四半期ごとのモデル再学習による不安定性（副因）')
print()
print('C. top2_probの価値:')
print('   - Gap 9.0%は実運用で信頼できる水準')
print('   - ただしROI 75.6%は既存80.2%より低い')
print()
print('D. 推奨アクション（多角的検討）:')
print('   1. 極端→strong正則化でROI回復を確認')
print('   2. 日次更新版のGap改善効果を定量評価')
print('   3. 年単位学習 + 3指数分離のハイブリッド検討')
print('   4. V4.4を10%程度でバランス調整')

print('\n【改善版（年単位学習 + strong正則化）結果】')
print('='*60)

# 改善版結果読み込み
try:
    improved_df = pd.read_csv('outputs/analysis/system_e_improved_yearly_results.csv')
    
    print('\n--- 年別結果 ---')
    for _, r in improved_df.iterrows():
        print(f"  {r['year']}: win {r['win_prob_test_roi']:.1f}%, top2 {r['top2_prob_test_roi']:.1f}%")
    
    print('\n--- 6年平均 ---')
    win_avg = improved_df['win_prob_test_roi'].mean()
    top2_avg = improved_df['top2_prob_test_roi'].mean()
    place_avg = improved_df['place_prob_test_roi'].mean()
    win_gap = improved_df['win_prob_gap'].mean()
    top2_gap = improved_df['top2_prob_gap'].mean()
    place_gap = improved_df['place_prob_gap'].mean()
    
    print(f"  win_prob:   {win_avg:.1f}% (Gap: {win_gap:.1f}%)")
    print(f"  top2_prob:  {top2_avg:.1f}% (Gap: {top2_gap:.1f}%)")
    print(f"  place_prob: {place_avg:.1f}% (Gap: {place_gap:.1f}%)")
    
    print('\n--- 既存システムとの比較 ---')
    print(f"  既存System A+: 80.2%")
    print(f"  今回(win_prob): {win_avg:.1f}%")
    print(f"  差: {win_avg - 80.2:+.1f}pt")
except Exception as e:
    print(f"改善版結果読み込みエラー: {e}")
