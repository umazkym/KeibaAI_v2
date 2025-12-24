import pandas as pd
import numpy as np
from typing import Dict, Optional, List

def calculate_daily_bias(
    past_results: pd.DataFrame,
    current_race_num: int,
    target_surface: str = None
) -> Dict[str, float]:
    """
    同日の過去レース結果からトラックバイアスを計算する。
    
    Args:
        past_results: 同日の過去レース結果DataFrame
        current_race_num: 現在のレース番号（これ未満のレースを使用）
        target_surface: 現在のレースの馬場種別 ('芝', 'ダート', '障害' など)
                        指定された場合、同じ馬場種別のレースのみを集計対象とする。
    
    Returns:
        bias_stats: バイアス指標の辞書
            - front_runner_win_rate: 先行馬（4角通過順位率 < 0.3）の勝率
            - inner_bracket_win_rate: 内枠（枠番 1-3）の勝率
            - bias_sample_count: 集計に使用したレース数
    """
    # デフォルト値（バイアスなし）
    default_bias = {
        'front_runner_win_rate': 0.3, # 一般的な先行馬勝率（仮）
        'inner_bracket_win_rate': 0.3, # 一般的な内枠勝率（仮）
        'bias_sample_count': 0
    }
    
    if past_results.empty:
        return default_bias
        
    # 現在のレースより前のレースにフィルタリング
    # (past_resultsは既にフィルタリングされている前提だが、念のため)
    if 'race_num' in past_results.columns:
        past_races = past_results[past_results['race_num'] < current_race_num].copy()
    else:
        past_races = past_results.copy()
        
    # 馬場種別によるフィルタリング（重要：芝・ダート分離）
    if target_surface and 'track_surface' in past_races.columns:
        # '芝', 'ダート' などの文字列が含まれているかで判定
        # データによっては '芝左' 'ダート右' のようになっている場合があるため部分一致推奨だが、
        # ここでは正規化されていることを期待して完全一致または包含で判定
        # features.yamlのone-hot encoding前の生データが必要
        
        # track_surfaceがone-hotされている場合は復元が難しいので、
        # 呼び出し元でフィルタリング済みのpast_resultsを渡す設計の方が良いかもしれないが、
        # ここではdataframeに 'track_surface' カラム（生）が残っていると仮定する。
        
        # もし 'track_surface' がない場合は、フィルタリングできないので警告を出して全データ使うか、何もしない。
        # ここでは単純に文字列比較を行う。
        is_target_dirt = 'ダート' in target_surface
        is_target_turf = '芝' in target_surface
        
        if is_target_dirt:
            past_races = past_races[past_races['track_surface'].str.contains('ダート', na=False)]
        elif is_target_turf:
            past_races = past_races[past_races['track_surface'].str.contains('芝', na=False)]
            
    if len(past_races) == 0:
        return default_bias
        
    # 勝った馬（1着）のデータのみ抽出
    winners = past_races[past_races['finish_position'] == 1]
    
    if len(winners) == 0:
        return default_bias
        
    # 1. 先行馬バイアス (Front Runner Bias)
    # 4コーナー通過順位率 (passing_order_4 / n_horses) が 0.3 未満の馬が勝った割合
    # ここでは正規化済み 'passing_order_4' (0.0-1.0) があると仮定
    # もし正規化されていない場合は、passing_order_4 / n_horses を計算する必要がある
    
    if 'passing_order_4' in winners.columns:
        # 既に正規化されていると仮定 (0.0 - 1.0)
        # normalize_passing_order されている場合、先頭は0.0
        front_runners_wins = winners[winners['passing_order_4'] <= 0.3]
        front_runner_win_rate = len(front_runners_wins) / len(winners)
    else:
        front_runner_win_rate = default_bias['front_runner_win_rate']
        
    # 2. 内枠バイアス (Inner Bracket Bias)
    # 枠番が1, 2, 3の馬が勝った割合
    if 'bracket_number' in winners.columns:
        inner_wins = winners[winners['bracket_number'].isin([1, 2, 3])]
        inner_bracket_win_rate = len(inner_wins) / len(winners)
    else:
        inner_bracket_win_rate = default_bias['inner_bracket_win_rate']
        
    return {
        'front_runner_win_rate': front_runner_win_rate,
        'inner_bracket_win_rate': inner_bracket_win_rate,
        'bias_sample_count': len(winners)
    }

def get_bias_fit_score(row: pd.Series, bias_stats: Dict[str, float]) -> float:
    """
    馬の特性とトラックバイアスの適合度スコアを計算する。
    
    Args:
        row: 馬の特徴量を含むSeries
        bias_stats: calculate_daily_biasで計算されたバイアス指標
        
    Returns:
        fit_score: 適合度スコア (0.0 - 1.0, 0.5がニュートラル)
    """
    score = 0.5
    weight = 0.0
    
    # サンプル数が少ない場合はバイアスを信用しない
    if bias_stats['bias_sample_count'] < 2:
        return 0.5
        
    # 1. 脚質適合度
    # 馬の過去平均位置取り (past_avg_passing_order_4) があると仮定
    if 'past_avg_passing_order_4' in row.index:
        horse_pos = row['past_avg_passing_order_4']
        bias_front = bias_stats['front_runner_win_rate']
        
        # バイアスが先行有利 (例えば > 0.5) かつ 馬が先行 (<= 0.3) -> プラス
        # バイアスが差し有利 (< 0.3) かつ 馬が差し (> 0.6) -> プラス
        
        # シンプルなロジック:
        # バイアスが高い(先行有利)なら、位置取りが前(値が小さい)ほど良い
        # bias_front: 0.0(追込有利) ~ 1.0(逃げ有利)
        # horse_pos: 0.0(逃げ) ~ 1.0(追込)
        
        # 適合度 = 1 - |bias_front - (1 - horse_pos)| ... ちょっと複雑
        # 逃げ有利(1.0) & 逃げ馬(0.0) -> マッチ
        # 追込有利(0.0) & 追込馬(1.0) -> マッチ
        
        # 先行有利度(0~1) と 馬の先行度(1-pos) の積？
        # ここでは簡易的に:
        # 先行有利バイアスがある場合
        if bias_front > 0.4: # 平均より先行が勝っている
            if horse_pos <= 0.4: # この馬も先行
                score += 0.1
            elif horse_pos >= 0.7: # この馬は後方
                score -= 0.1
        elif bias_front < 0.2: # 差し有利
            if horse_pos >= 0.6: # この馬は差し
                score += 0.1
            elif horse_pos <= 0.3: # この馬は先行
                score -= 0.1
                
    # 2. 枠順適合度
    if 'bracket_number' in row.index:
        bracket = row['bracket_number']
        bias_inner = bias_stats['inner_bracket_win_rate']
        
        # 内枠有利 (3枠以内の勝率が高い)
        if bias_inner > 0.4: # 平均(3/8=0.375)より高い
            if bracket <= 3:
                score += 0.1
            elif bracket >= 7:
                score -= 0.1
        elif bias_inner < 0.2: # 外枠有利
            if bracket >= 6:
                score += 0.1
            elif bracket <= 2:
                score -= 0.1
                
    return np.clip(score, 0.0, 1.0)
