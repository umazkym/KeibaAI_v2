#!/usr/bin/env python3
# src/sim/simulator.py
"""
Monte Carlo シミュレーションモジュール
仕様書 8.2章 に基づく実装
Numba最適化版
"""

import logging
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
from numba import jit, prange
import json
from pathlib import Path
from datetime import datetime, timezone

class RaceSimulator:
    """
    レースシミュレータ
    仕様書 8.2
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: シミュレーション設定辞書 (configs/default.yaml や optimization.yaml)
        """
        self.config = config if config else {}
        self.K = self.config.get('K', 1000)
        self.seed = self.config.get('seed', 42)
        
        # ログパスの設定（configから取得、なければデフォルト）
        # logs_pathキーがない場合のエラーを防ぐ
        self.logs_dir = Path(self.config.get('logs_path', 'data/logs'))
        self.logs_dir.mkdir(parents=True, exist_ok=True)
    
    def simulate_race(
        self,
        mu: np.ndarray,
        sigma: np.ndarray,
        nu: float,
        horse_numbers: np.ndarray,
        K: int = 1000,
        seed: int = 42
    ) -> Dict:
        """
        1レースのシミュレーション
        仕様書 8.2
        
        Args:
            mu: 各馬のμ値（性能スコア）
            sigma: 各馬のσ値（残差分散）
            nu: レース荒れ度
            horse_numbers: 馬番の配列
            K: シミュレーション回数
            seed: 乱数シード
        
        Returns:
            シミュレーション結果辞書
        """
        logging.info(f"シミュレーション開始: K={K}, 馬数={len(mu)}")
        
        n_horses = len(mu)
        
        if n_horses == 0:
            logging.warning("シミュレーション対象の馬がいません。")
            return {}
            
        # Numba最適化関数を呼び出し
        rankings = simulate_plackett_luce_numba(
            mu=mu,
            sigma=sigma,
            nu=nu,
            n_horses=n_horses,
            K=K,
            seed=seed
        )
        
        # 結果を集計
        results = self._aggregate_results(
            rankings=rankings,
            horse_numbers=horse_numbers,
            K=K
        )
        
        logging.info("シミュレーション完了")
        
        return results
    
    def _aggregate_results(
        self,
        rankings: np.ndarray,
        horse_numbers: np.ndarray,
        K: int
    ) -> Dict:
        """
        シミュレーション結果を集計
        仕様書 8.2
        
        Args:
            rankings: ランキング配列 (K, n_horses) (0-indexedのインデックス)
            horse_numbers: 馬番配列 (インデックスに対応する馬番)
            K: シミュレーション回数
        
        Returns:
            集計結果辞書
        """
        n_horses = len(horse_numbers)
        
        # 1. 単勝確率（1着確率）
        win_probs = {}
        # (np.bincount を使って高速化)
        # rankings[:, 0] は1着のインデックス
        first_place_indices = rankings[:, 0]
        win_counts = np.bincount(first_place_indices, minlength=n_horses)
        
        # デバッグ出力
        logging.debug(f"n_horses: {n_horses}")
        logging.debug(f"rankings min/max: {first_place_indices.min()}/{first_place_indices.max()}")
        logging.debug(f"win_counts sum: {win_counts.sum()}")
        logging.debug(f"win_counts len: {len(win_counts)}")
        logging.debug(f"horse_numbers: {horse_numbers}")
        
        if len(set(horse_numbers)) != len(horse_numbers):
            logging.error(f"Duplicate horse_numbers detected! {horse_numbers}")
        
        for i, horse_num in enumerate(horse_numbers):
            # win_countsのインデックスiが範囲外でないか確認
            if i < len(win_counts):
                # キーが既に存在する場合は加算する（重複馬番対策）
                key = str(horse_num)
                if key in win_probs:
                    logging.warning(f"Overwriting/Adding to existing key {key} in win_probs")
                    win_probs[key] += float(win_counts[i] / K)
                else:
                    win_probs[key] = float(win_counts[i] / K)
            else:
                logging.error(f"Index {i} is out of bounds for win_counts (len={len(win_counts)})")
                # キーがなければ0で初期化
                if str(horse_num) not in win_probs:
                    win_probs[str(horse_num)] = 0.0
        
        # 2. 複勝確率（3着以内確率）
        place_probs = {}
        # (3着までのランキングをフラットにしてbincount)
        top3_indices = rankings[:, :3].ravel()
        place_counts = np.bincount(top3_indices, minlength=n_horses)
        for i, horse_num in enumerate(horse_numbers):
            place_probs[str(horse_num)] = float(place_counts[i] / K)
             
        # 3. 馬連確率（上位2頭の組み合わせ）
        exacta_probs = {}
        # (インデックス -> 馬番への変換)
        top2_horse_numbers = np.take(horse_numbers, rankings[:, :2])
        # (ソートして文字列キーを作成 '馬番1-馬番2')
        top2_sorted = np.sort(top2_horse_numbers, axis=1)
        
        # 高速な集計
        keys = [f"{r[0]}-{r[1]}" for r in top2_sorted]
        unique_keys, counts = np.unique(keys, return_counts=True)
        exacta_probs = {key: float(count / K) for key, count in zip(unique_keys, counts)}

        # 4. 3連単確率（上位3頭の順序付き組み合わせ）
        trifecta_probs = {}
        # (インデックス -> 馬番への変換)
        top3_horse_numbers = np.take(horse_numbers, rankings[:, :3])
        
        # 高速な集計
        keys_trifecta = [f"{r[0]}-{r[1]}-{r[2]}" for r in top3_horse_numbers]
        unique_keys_t, counts_t = np.unique(keys_trifecta, return_counts=True)
        
        # TopNのみ保持（メモリ節約のため）
        trifecta_all = {key: float(count / K) for key, count in zip(unique_keys_t, counts_t)}
        trifecta_probs_top = dict(
            sorted(trifecta_all.items(), key=lambda x: x[1], reverse=True)[:20] # Top 20
        )
        
        return {
            'win_probs': win_probs,
            'place_probs': place_probs,
            'exacta_probs': exacta_probs, # 馬連 (仕様書 では exacta だが、ソートしているので馬連 (quinella))
            'trifecta_probs': trifecta_probs_top,
            'K': K
            # 'rankings': rankings, # 生データは巨大なので通常は保存しない
        }
    
    def save_simulation(
        self,
        race_id: str,
        model_id: str,
        simulation_results: Dict,
        output_dir: str = 'data/simulations'
    ):
        """
        シミュレーション結果を保存
        仕様書 8.2
        
        Args:
            race_id: レースID
            model_id: モデルID
            simulation_results: シミュレーション結果
            output_dir: 出力ディレクトリ
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # sim_id生成
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        sim_id = f"{timestamp}_{model_id}_{race_id}"
        
        # 保存データ
        save_data = {
            'sim_id': sim_id,
            'race_id': race_id,
            'model_id': model_id,
            'created_ts': datetime.now(timezone.utc).isoformat(),
            'K': simulation_results['K'],
            'win_probs': simulation_results['win_probs'],
            'place_probs': simulation_results['place_probs'],
            'exacta_probs': simulation_results['exacta_probs'],
            'trifecta_probs': simulation_results['trifecta_probs']
        }
        
        # JSON保存 (atomic_write を使った方が安全だが、仕様書に合わせて簡易化)
        output_file = output_path / f"{sim_id}.json"
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            logging.info(f"シミュレーション結果保存: {output_file}")
        except Exception as e:
            logging.error(f"シミュレーション結果の保存に失敗: {e}")


@jit(nopython=True, parallel=True)
def simulate_plackett_luce_numba(
    mu: np.ndarray,
    sigma: np.ndarray,
    nu: float,
    n_horses: int,
    K: int,
    seed: int
) -> np.ndarray:
    """
    Plackett-Luceモデルによるランキング生成（Numba最適化）
    仕様書 8.2
    
    Args:
        mu: 各馬のμ値
        sigma: 各馬のσ値
        nu: レース荒れ度
        n_horses: 馬数
        K: シミュレーション回数
        seed: 乱数シード
    
    Returns:
        ランキング配列 (K, n_horses)
        各行は馬のインデックス（0-indexed）の順位
    """
    np.random.seed(seed)
    
    rankings = np.zeros((K, n_horses), dtype=np.int32)
    
    # K回のシミュレーション (並列実行)
    for k in prange(K):
        # 各馬の性能スコアをサンプリング
        # θ_i ~ N(μ_i, σ_i^2 + ν^2)
        theta = np.zeros(n_horses)
        for i in range(n_horses):
            # (σ^2 + ν^2) の平方根
            total_std = np.sqrt(sigma[i]**2 + nu**2)
            theta[i] = np.random.normal(mu[i], total_std)
        
        # Plackett-Luceサンプリング
        # (Numbaは np.arange(n_horses).tolist() のような動的リスト操作が遅い)
        # (代わりに、インデックス配列とマスクを使用する)
        
        remaining_indices = np.arange(n_horses)
        
        for pos in range(n_horses):
            # 残りの馬の数
            remaining_count = n_horses - pos
            
            # 残りの馬の性能スコア (θ)
            remaining_theta = np.zeros(remaining_count)
            for j in range(remaining_count):
                remaining_theta[j] = theta[remaining_indices[j]]

            # 指数変換（数値安定性のため正規化）
            exp_theta = np.exp(remaining_theta - np.max(remaining_theta))
            
            # 確率計算
            sum_exp_theta = np.sum(exp_theta)
            if sum_exp_theta == 0: # 全員 0 の場合
                 probs = np.ones(remaining_count) / remaining_count
            else:
                 probs = exp_theta / sum_exp_theta
            
            # サンプリング (Gumbel-Max-Trick の方が速いが、仕様書 に合わせる)
            cumsum_probs = np.cumsum(probs)
            rand_val = np.random.random()
            
            selected_idx_in_remaining = 0
            for j in range(remaining_count):
                if rand_val <= cumsum_probs[j]:
                    selected_idx_in_remaining = j
                    break
            
            # 選択された馬の「元のインデックス」
            selected_original_idx = remaining_indices[selected_idx_in_remaining]
            
            # 順位を記録
            rankings[k, pos] = selected_original_idx
            
            # 残りから削除 (選択された要素を末尾と交換して末尾を捨てる)
            last_idx_in_remaining = remaining_count - 1
            remaining_indices[selected_idx_in_remaining] = remaining_indices[last_idx_in_remaining]
            # (配列のサイズは変えずに、次のループで見る範囲を1つ減らす)
            
    return rankings