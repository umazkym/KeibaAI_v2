# results/ — モデル検証・バックテスト結果

**目的**: モデルの検証結果やバックテスト出力を格納するディレクトリ

## 主なサブディレクトリ

| ディレクトリ | 内容 |
|-------------|------|
| `walkforward_validation/` | Walk-forward検証結果 |
| `calibration/` | 確率キャリブレーション結果 |
| `edge_analysis/` | エッジ（期待値の優位性）分析 |
| `fukusho/` | 複勝戦略のバックテスト |
| `umaren_box/` | 馬連ボックス戦略のバックテスト |
| `odds_poc/` | オッズ分析PoC |
| `pace_*` | ペース関連分析 |
| `multi_bet_analysis/` | 複合馬券分析 |
| `final_strategy/` | 最終戦略の結果 |

## outputs/ との違い

- **`results/`**: モデル検証・バックテスト結果（再現性が重要、バージョン管理対象）
- **`outputs/`**: 一回限りの分析・レポート出力（再現性は不要）
