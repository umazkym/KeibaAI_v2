# src/modules/monitoring/model_analyzer.py
"""
モデル分析・デバッグツール

モデルの性能を詳細に分析し、問題点を特定して改善提案を行う。
"""

import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from collections import defaultdict
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class ModelAnalysisReport:
    """モデル分析レポート"""
    timestamp: str
    model_path: str
    overall_metrics: Dict[str, float]
    segment_analysis: Dict[str, Dict[str, Any]]
    error_analysis: Dict[str, Any]
    feature_importance: Dict[str, float]
    recommendations: List[str]


class ModelAnalyzer:
    """モデル分析・デバッグツール"""

    def __init__(self, model_path: Path, data_path: Path):
        """
        Args:
            model_path: モデルディレクトリのパス
            data_path: データディレクトリのパス
        """
        self.model_path = Path(model_path)
        self.data_path = Path(data_path)
        self.predictions_df: Optional[pd.DataFrame] = None
        self.actuals_df: Optional[pd.DataFrame] = None

    def analyze(self, predictions_df: pd.DataFrame,
               actuals_df: pd.DataFrame) -> ModelAnalysisReport:
        """
        包括的なモデル分析を実行

        Args:
            predictions_df: 予測結果のDataFrame (race_id, horse_id, predicted_score)
            actuals_df: 実績データのDataFrame (race_id, horse_id, finish_position, finish_time_seconds)

        Returns:
            ModelAnalysisReport
        """
        self.predictions_df = predictions_df
        self.actuals_df = actuals_df

        logger.info("=" * 80)
        logger.info("モデル分析開始")
        logger.info("=" * 80)

        # データのマージ
        merged_df = self._merge_predictions_and_actuals()

        # 1. 全体的な評価指標の計算
        logger.info("\n[1] 全体的な評価指標の計算")
        overall_metrics = self._calculate_overall_metrics(merged_df)

        # 2. セグメント別分析
        logger.info("\n[2] セグメント別分析")
        segment_analysis = self._analyze_by_segments(merged_df)

        # 3. エラー分析
        logger.info("\n[3] エラー分析")
        error_analysis = self._analyze_errors(merged_df)

        # 4. 特徴量重要度（モデルがある場合）
        logger.info("\n[4] 特徴量重要度分析")
        feature_importance = self._analyze_feature_importance()

        # 5. 改善提案の生成
        logger.info("\n[5] 改善提案の生成")
        recommendations = self._generate_recommendations(
            overall_metrics, segment_analysis, error_analysis
        )

        # レポートの作成
        report = ModelAnalysisReport(
            timestamp=datetime.now().isoformat(),
            model_path=str(self.model_path),
            overall_metrics=overall_metrics,
            segment_analysis=segment_analysis,
            error_analysis=error_analysis,
            feature_importance=feature_importance,
            recommendations=recommendations
        )

        logger.info("=" * 80)
        logger.info("モデル分析完了")
        logger.info("=" * 80)

        return report

    def _merge_predictions_and_actuals(self) -> pd.DataFrame:
        """予測と実績をマージ"""
        merge_keys = ['race_id', 'horse_id']

        # キーの正規化
        for df in [self.predictions_df, self.actuals_df]:
            for key in merge_keys:
                if key in df.columns:
                    df[key] = df[key].astype(str).str.strip()

        # マージ
        merged_df = pd.merge(
            self.predictions_df,
            self.actuals_df,
            on=merge_keys,
            how='inner'
        )

        logger.info(f"マージ後のデータ件数: {len(merged_df)} 件")

        return merged_df

    def _calculate_overall_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """全体的な評価指標を計算"""
        metrics = {}

        # 1. スピアマン順位相関係数
        correlations = []
        for race_id, group in df.groupby('race_id'):
            if len(group) > 1:
                from scipy.stats import spearmanr
                corr, _ = spearmanr(group['predicted_score'], group['finish_position'])
                if not np.isnan(corr):
                    correlations.append(corr)

        if correlations:
            metrics['spearman_mean'] = np.mean(correlations)
            metrics['spearman_median'] = np.median(correlations)
            metrics['spearman_std'] = np.std(correlations)
            logger.info(f"スピアマン順位相関係数 (平均): {metrics['spearman_mean']:.4f}")

        # 2. Brier Score（確率予測がある場合）
        if 'predicted_win_prob' in df.columns and 'is_winner' in df.columns:
            brier_score = np.mean((df['predicted_win_prob'] - df['is_winner']) ** 2)
            metrics['brier_score'] = brier_score
            logger.info(f"Brier Score: {brier_score:.4f}")

        # 3. Expected Calibration Error (ECE)
        if 'predicted_win_prob' in df.columns and 'is_winner' in df.columns:
            ece = self._calculate_ece(df['predicted_win_prob'], df['is_winner'])
            metrics['ece'] = ece
            logger.info(f"Expected Calibration Error: {ece:.4f}")

        # 4. Top-K Accuracy（上位K頭に入る確率）
        for k in [1, 3, 5]:
            top_k_acc = self._calculate_top_k_accuracy(df, k)
            metrics[f'top_{k}_accuracy'] = top_k_acc
            logger.info(f"Top-{k} Accuracy: {top_k_acc:.4f}")

        # 5. RMSE（タイム予測がある場合）
        if 'predicted_time' in df.columns and 'finish_time_seconds' in df.columns:
            valid_mask = df['finish_time_seconds'].notna() & df['predicted_time'].notna()
            if valid_mask.any():
                rmse = np.sqrt(np.mean((df.loc[valid_mask, 'predicted_time'] -
                                       df.loc[valid_mask, 'finish_time_seconds']) ** 2))
                metrics['rmse'] = rmse
                logger.info(f"RMSE (タイム予測): {rmse:.4f} 秒")

        # 6. ROI（模擬投資収益率）
        if 'predicted_score' in df.columns and 'win_odds' in df.columns:
            roi = self._calculate_roi(df)
            metrics['roi'] = roi
            logger.info(f"ROI (模擬投資): {roi:.2f}%")

        return metrics

    def _calculate_ece(self, predicted_probs: pd.Series,
                      actual_outcomes: pd.Series, n_bins: int = 10) -> float:
        """Expected Calibration Error (ECE) を計算"""
        bin_edges = np.linspace(0, 1, n_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

        ece = 0.0
        n_total = len(predicted_probs)

        for i in range(n_bins):
            bin_mask = (predicted_probs >= bin_edges[i]) & (predicted_probs < bin_edges[i + 1])
            if i == n_bins - 1:  # 最後のビンは右端を含む
                bin_mask = (predicted_probs >= bin_edges[i]) & (predicted_probs <= bin_edges[i + 1])

            n_bin = bin_mask.sum()
            if n_bin == 0:
                continue

            bin_predicted = predicted_probs[bin_mask].mean()
            bin_actual = actual_outcomes[bin_mask].mean()

            ece += (n_bin / n_total) * abs(bin_predicted - bin_actual)

        return ece

    def _calculate_top_k_accuracy(self, df: pd.DataFrame, k: int) -> float:
        """Top-K Accuracy を計算"""
        correct_count = 0
        total_races = 0

        for race_id, group in df.groupby('race_id'):
            if len(group) < k:
                continue

            # 予測スコアの上位K頭を取得
            top_k_predicted = group.nsmallest(k, 'predicted_score')

            # 実際の上位K頭を取得
            top_k_actual = group.nsmallest(k, 'finish_position')

            # 重複数を計算
            overlap = len(set(top_k_predicted['horse_id']) & set(top_k_actual['horse_id']))

            if overlap > 0:
                correct_count += 1

            total_races += 1

        return correct_count / total_races if total_races > 0 else 0.0

    def _calculate_roi(self, df: pd.DataFrame, bet_threshold: float = 0.2) -> float:
        """
        ROI（Return on Investment）を計算

        予測スコアが上位bet_threshold（デフォルト20%）の馬に均等に投資した場合の収益率
        """
        total_investment = 0
        total_return = 0

        for race_id, group in df.groupby('race_id'):
            n_horses = len(group)
            n_bet = max(1, int(n_horses * bet_threshold))

            # 予測スコア上位n_bet頭に投資
            top_horses = group.nsmallest(n_bet, 'predicted_score')

            for _, horse in top_horses.iterrows():
                total_investment += 1  # 1単位投資

                # 1着なら配当を受け取る
                if horse['finish_position'] == 1 and 'win_odds' in horse:
                    total_return += horse['win_odds']

        roi = ((total_return - total_investment) / total_investment * 100
               if total_investment > 0 else 0.0)

        return roi

    def _analyze_by_segments(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """セグメント別に性能を分析"""
        segment_analysis = {}

        # セグメント定義
        segments = {
            'distance': self._create_distance_segments(df),
            'track_surface': df.get('track_surface', pd.Series(dtype=str)),
            'race_class': df.get('race_class', pd.Series(dtype=str)),
            'venue': df.get('venue', pd.Series(dtype=str)),
            'weather': df.get('weather', pd.Series(dtype=str)),
        }

        for segment_name, segment_series in segments.items():
            if segment_series.empty or segment_series.isna().all():
                continue

            df_with_segment = df.copy()
            df_with_segment['segment'] = segment_series

            segment_results = {}

            for segment_value, group in df_with_segment.groupby('segment'):
                if len(group) < 10:  # 最低10件以上
                    continue

                # スピアマン相関
                correlations = []
                for race_id, race_group in group.groupby('race_id'):
                    if len(race_group) > 1:
                        from scipy.stats import spearmanr
                        corr, _ = spearmanr(race_group['predicted_score'],
                                          race_group['finish_position'])
                        if not np.isnan(corr):
                            correlations.append(corr)

                if correlations:
                    segment_results[str(segment_value)] = {
                        'count': len(group),
                        'spearman_mean': np.mean(correlations),
                        'spearman_std': np.std(correlations),
                    }

            segment_analysis[segment_name] = segment_results

            # ログ出力
            logger.info(f"\nセグメント: {segment_name}")
            for seg_val, stats in segment_results.items():
                logger.info(f"  {seg_val}: 件数={stats['count']}, "
                          f"相関={stats['spearman_mean']:.4f}")

        return segment_analysis

    def _create_distance_segments(self, df: pd.DataFrame) -> pd.Series:
        """距離別のセグメントを作成"""
        if 'distance_m' not in df.columns:
            return pd.Series(dtype=str)

        def categorize_distance(dist):
            if pd.isna(dist):
                return 'unknown'
            if dist < 1400:
                return 'sprint'
            elif dist < 1800:
                return 'mile'
            elif dist < 2200:
                return 'intermediate'
            elif dist < 2800:
                return 'long'
            else:
                return 'extended'

        return df['distance_m'].apply(categorize_distance)

    def _analyze_errors(self, df: pd.DataFrame) -> Dict[str, Any]:
        """エラー分析"""
        error_analysis = {}

        # 1. 大きなエラーを持つレースを特定
        race_errors = []
        for race_id, group in df.groupby('race_id'):
            if len(group) > 1:
                from scipy.stats import spearmanr
                corr, _ = spearmanr(group['predicted_score'], group['finish_position'])

                if np.isnan(corr):
                    continue

                # 相関が低い（予測が悪い）レースを記録
                if corr > -0.3:  # 負の相関が弱い = 予測が悪い
                    race_errors.append({
                        'race_id': race_id,
                        'correlation': corr,
                        'horse_count': len(group),
                        'distance_m': group['distance_m'].iloc[0] if 'distance_m' in group else None,
                        'track_surface': group['track_surface'].iloc[0] if 'track_surface' in group else None,
                        'venue': group['venue'].iloc[0] if 'venue' in group else None,
                    })

        # エラーの多いレースをソート
        race_errors.sort(key=lambda x: x['correlation'], reverse=True)
        error_analysis['worst_races'] = race_errors[:20]  # 上位20件

        logger.info(f"\n予測エラーの大きいレース（上位5件）:")
        for i, race_error in enumerate(race_errors[:5], 1):
            logger.info(f"  {i}. race_id={race_error['race_id']}, "
                       f"相関={race_error['correlation']:.4f}, "
                       f"距離={race_error.get('distance_m', 'N/A')}m, "
                       f"馬場={race_error.get('track_surface', 'N/A')}")

        # 2. エラーの共通パターンを分析
        if race_errors:
            error_df = pd.DataFrame(race_errors)

            # 距離別のエラー率
            if 'distance_m' in error_df.columns and error_df['distance_m'].notna().any():
                distance_error_rate = error_df.groupby(
                    pd.cut(error_df['distance_m'], bins=[0, 1400, 1800, 2200, 2800, 5000],
                          labels=['sprint', 'mile', 'intermediate', 'long', 'extended'])
                ).size().to_dict()
                error_analysis['distance_error_distribution'] = distance_error_rate

            # 馬場別のエラー率
            if 'track_surface' in error_df.columns:
                surface_error_rate = error_df['track_surface'].value_counts().to_dict()
                error_analysis['surface_error_distribution'] = surface_error_rate

        return error_analysis

    def _analyze_feature_importance(self) -> Dict[str, float]:
        """特徴量重要度を分析"""
        feature_importance = {}

        # モデルファイルから特徴量重要度を読み込む
        importance_file = self.model_path / 'feature_importance.json'

        if importance_file.exists():
            with open(importance_file, 'r', encoding='utf-8') as f:
                feature_importance = json.load(f)

            # 上位10件をログ出力
            sorted_features = sorted(feature_importance.items(),
                                    key=lambda x: x[1], reverse=True)[:10]

            logger.info("\n特徴量重要度（上位10件）:")
            for i, (feature, importance) in enumerate(sorted_features, 1):
                logger.info(f"  {i}. {feature}: {importance:.4f}")
        else:
            logger.warning("特徴量重要度ファイルが見つかりません")

        return feature_importance

    def _generate_recommendations(self, overall_metrics: Dict[str, float],
                                 segment_analysis: Dict[str, Dict[str, Any]],
                                 error_analysis: Dict[str, Any]) -> List[str]:
        """改善提案を生成"""
        recommendations = []

        # 1. 全体的な性能に基づく提案
        if 'spearman_mean' in overall_metrics:
            corr = overall_metrics['spearman_mean']
            if corr > -0.3:
                recommendations.append(
                    f"⚠ 全体的な予測精度が低い（相関={corr:.4f}）。"
                    "特徴量の見直しやモデルアーキテクチャの変更を検討してください。"
                )
            elif corr > -0.5:
                recommendations.append(
                    f"⚠ 予測精度は中程度（相関={corr:.4f}）。"
                    "特徴量エンジニアリングの改善余地があります。"
                )
            else:
                recommendations.append(
                    f"✓ 予測精度は良好（相関={corr:.4f}）。"
                )

        # 2. セグメント別の性能差に基づく提案
        for segment_name, segment_results in segment_analysis.items():
            if not segment_results:
                continue

            # 性能の良いセグメントと悪いセグメントを特定
            sorted_segments = sorted(segment_results.items(),
                                    key=lambda x: x[1]['spearman_mean'])

            if len(sorted_segments) >= 2:
                worst = sorted_segments[0]
                best = sorted_segments[-1]

                if worst[1]['spearman_mean'] > -0.3:
                    recommendations.append(
                        f"⚠ {segment_name}={worst[0]} で性能が低い（相関={worst[1]['spearman_mean']:.4f}）。"
                        f"このセグメント向けの特徴量追加やデータ拡充を検討してください。"
                    )

        # 3. エラー分析に基づく提案
        if 'worst_races' in error_analysis and error_analysis['worst_races']:
            worst_count = len(error_analysis['worst_races'])
            recommendations.append(
                f"⚠ {worst_count} 件のレースで予測精度が特に低い。"
                "これらのレースに共通する特徴を分析し、モデル改善に活用してください。"
            )

        # 4. Brier Scoreに基づく提案
        if 'brier_score' in overall_metrics:
            brier = overall_metrics['brier_score']
            if brier > 0.25:
                recommendations.append(
                    f"⚠ Brier Score が高い（{brier:.4f}）。"
                    "確率キャリブレーションの改善が必要です。Temperature Scalingなどを検討してください。"
                )

        # 5. ECEに基づく提案
        if 'ece' in overall_metrics:
            ece = overall_metrics['ece']
            if ece > 0.1:
                recommendations.append(
                    f"⚠ Expected Calibration Error が高い（{ece:.4f}）。"
                    "確率予測がキャリブレートされていません。Platt Scalingなどを検討してください。"
                )

        # 6. ROIに基づく提案
        if 'roi' in overall_metrics:
            roi = overall_metrics['roi']
            if roi < 0:
                recommendations.append(
                    f"⚠ ROI がマイナス（{roi:.2f}%）。"
                    "投資戦略の見直しや、より保守的なベット閾値の設定を検討してください。"
                )
            elif roi > 10:
                recommendations.append(
                    f"✓ ROI が良好（{roi:.2f}%）。"
                    "現在の戦略は有効です。"
                )

        # 提案をログ出力
        logger.info("\n改善提案:")
        for i, rec in enumerate(recommendations, 1):
            logger.info(f"  {i}. {rec}")

        return recommendations

    def save_report(self, report: ModelAnalysisReport, output_path: Path):
        """レポートを保存"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # JSON形式で保存
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2)

        logger.info(f"モデル分析レポートを保存: {output_path}")

        # Markdown形式でも保存
        md_path = output_path.with_suffix('.md')
        self._save_markdown_report(report, md_path)

    def _save_markdown_report(self, report: ModelAnalysisReport, output_path: Path):
        """Markdown形式でレポートを保存"""
        lines = [
            "# モデル分析レポート",
            "",
            f"**生成日時**: {report.timestamp}",
            f"**モデルパス**: {report.model_path}",
            "",
            "## 全体的な評価指標",
            "",
        ]

        for metric, value in report.overall_metrics.items():
            lines.append(f"- **{metric}**: {value:.4f}")

        lines.append("")
        lines.append("## セグメント別分析")
        lines.append("")

        for segment_name, segment_results in report.segment_analysis.items():
            lines.append(f"### {segment_name}")
            lines.append("")
            lines.append("| セグメント | 件数 | スピアマン相関 | 標準偏差 |")
            lines.append("|-----------|------|--------------|---------|")

            for seg_val, stats in segment_results.items():
                lines.append(f"| {seg_val} | {stats['count']} | "
                           f"{stats['spearman_mean']:.4f} | "
                           f"{stats['spearman_std']:.4f} |")

            lines.append("")

        lines.append("## 改善提案")
        lines.append("")

        for i, rec in enumerate(report.recommendations, 1):
            lines.append(f"{i}. {rec}")

        lines.append("")

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        logger.info(f"Markdownレポートを保存: {output_path}")
