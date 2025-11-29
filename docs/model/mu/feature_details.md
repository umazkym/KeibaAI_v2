# KeibaAI μモデル v2.7 全特徴量リスト

本ドキュメントでは、μモデル v2.7 で使用されているすべての特徴量を網羅的に列挙し、解説します。
v2.7では、v2.6の特徴量に加え、**ROI最大化のための7つの新特徴量**が追加されました。

## 1. 新規追加特徴量 (v2.7 ROI Maximization)
v2.7で導入された、市場の歪みや隠れた勝因を捉えるための特徴量です。

| 特徴量名 | カテゴリ | 説明 | 狙い |
|---|---|---|---|
| `avg_pci_last5` | Time & Pace | 過去5走の平均PCI (Pace Change Index) | スロー/ハイペースへの適性と、自身の脚質の安定性を評価。 |
| `nishida_trend_score` | Time & Pace | 西田式スピード指数の近3走トレンド | 絶対的なタイムではなく、自身の調子が上向きか下向きかを評価。 |
| `corner_loss_proxy` | Bias | 過去の「外枠かつ先行」による距離ロス経験 | 過去の敗因が「不利」によるものかを判定し、実力を再評価。 |
| `sire_wet_track_boost` | Pedigree | 父の「重・不良」勝率 - 「良」勝率 | 血統的な道悪適性を数値化し、雨天時の穴馬を見つける。 |
| `pedigree_course_compatibility` | Pedigree | 父の「今回のコース条件」での複勝率 | 特定のコースに強い血統（コース巧者）を評価。 |
| `prev_race_level_index` | Field Level | 前走のレースクラス（格） | 前走の相手関係が強かったか弱かったかを評価（昇級初戦の過小評価対策）。 |
| `under_valued_score_avg` | Market | 過去5走の「人気 - 着順」平均 | 「人気より走る（過小評価されがち）」という馬の特性を数値化。 |

## 2. 基本属性 (Basic Attributes)
競走馬、レース、騎手などの基本的な属性情報です。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `sex` | 性別 | 牡/牝/セン (One-Hot Encoding) |
| `track_surface` | 馬場種別 | 芝/ダート (One-Hot Encoding) |
| `bracket_number` | 枠番カテゴリ | 内枠(1-3)/中枠(4-6)/外枠(7-8) |
| `distance_category` | 距離カテゴリ | Sprint/Mile/Intermediate/Long/Extreme |
| `basis_weight` | 斤量 | ハンデキャップや減量を含む |
| `horse_weight` | 馬体重 | 当日の馬体重（欠損時は中央値補完） |
| `age` | 馬齢 | |

## 3. 馬のキャリア・実績 (Career Stats)
馬の過去の通算成績です。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `career_starts` | 通算出走回数 | 経験値の指標 |
| `career_wins` | 通算勝利数 | |
| `prize_total` | 通算獲得賞金 | 競走能力の総合的な指標 |

## 4. 過去走パフォーマンストレンド (Performance Trend)
直近のレース成績から、現在の調子や安定性を分析します。
※ `N` = 3, 5, 10走前

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `avg_finish_last{N}` | 直近N走の平均着順 | 基本的な能力指標 |
| `win_rate_last{N}` | 直近N走の勝率 | |
| `place_rate_last{N}` | 直近N走の複勝率（3着内率） | 安定感の指標 |
| `finish_std_last{N}` | 直近N走の着順標準偏差 | 成績のばらつき（ムラっ気） |
| `finish_cv_last{N}` | 直近N走の着順変動係数 | 標準偏差 / 平均値（相対的な安定性） |
| `margin_seconds_mean_last{N}` | 直近N走の平均着差（秒） | 着順以上に実力を反映 |
| `last_3f_time_mean_last{N}` | 直近N走の平均上がり3Fタイム | 末脚の鋭さ |
| `passing_order_1_mean_last{N}` | 直近N走の平均通過順（第1コーナー） | 先行力の指標 |
| `passing_order_4_mean_last{N}` | 直近N走の平均通過順（第4コーナー） | 展開への対応力 |

## 5. コース適性 (Course Affinity)
今回のレース条件（競馬場、距離、馬場）に対する適性を評価します。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `venue_avg_finish` | 同一競馬場での平均着順 | コース巧者かどうか |
| `venue_races` | 同一競馬場での出走回数 | 経験値 |
| `venue_avg_odds` | 同一競馬場での平均オッズ | 期待値の指標 |
| `dist_avg_finish` | 同一距離カテゴリでの平均着順 | 距離適性 |
| `dist_avg_time` | 同一距離カテゴリでの平均走破タイム | 時計の裏付け |
| `surface_avg_finish` | 同一馬場（芝/ダート）での平均着順 | 馬場適性 |
| `surface_avg_last3f` | 同一馬場での平均上がり3F | |

## 6. 血統分析 (Bloodline Analytics)
血統背景から潜在的な適性を推測します。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `sire_avg_prize` | 父産駒の平均獲得賞金 | 種牡馬の格 |
| `damsire_avg_prize` | 母父産駒の平均獲得賞金 | 母系の格 |
| `sire_avg_finish` | 父産駒の平均着順 | |
| `sire_std_finish` | 父産駒の着順標準偏差 | 産駒の安定性 |
| `sire_avg_distance` | 父産駒の平均距離 | スタミナ/スピードタイプ判定 |
| `bms_avg_finish` | 母父産駒の平均着順 | BMS (Broodmare Sire) 効果 |
| `nicks_avg_finish` | ニックス（父×母父）の平均着順 | 配合の相性 |
| `sire_course_avg_finish` | 父産駒の「今回の条件」での平均着順 | 血統的なコース適性 |

## 7. 人馬の相性・関係者 (Synergy & Connections)
騎手、調教師、およびその組み合わせの成績です。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `jockey_win_rate` | 騎手の通算勝率 | 騎手の手腕 |
| `trainer_win_rate` | 調教師の通算勝率 | 厩舎力 |
| `combo_win_rate` | 騎手×調教師のコンビ勝率 | ゴールデンタッグの検知 |
| `combo_avg_finish` | 騎手×調教師のコンビ平均着順 | |
| `combo_overperform` | コンビ成績と人気（期待値）の乖離 | 人気以上に走らせるコンビか |
| `jockey_venue_win_rate` | 騎手の「今回の競馬場」での勝率 | コース得意騎手 |
| `jockey_distance_win_rate` | 騎手の「今回の距離」での勝率 | |
| `trainer_venue_win_rate` | 調教師の「今回の競馬場」での勝率 | 地元開催での勝負気配など |

## 8. 条件変化・ローテーション (Condition Changes & Rotation)
前走からの変化や間隔に着目した特徴量です。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `days_since_last_race` | 前走からの日数 | レース間隔 |
| `is_rest_return` | 長期休養明けフラグ | 90日以上の間隔 |
| `distance_change` | 距離変動量（m） | 延長/短縮の幅 |
| `is_distance_shortened` | 距離短縮フラグ | 今回距離 < 前走距離 |
| `is_distance_lengthened` | 距離延長フラグ | 今回距離 > 前走距離 |
| `surface_change` | 馬場替わりフラグ | 芝⇔ダートの変更 |
| `venue_change` | 競馬場替わりフラグ | 遠征など |

## 9. レース内相対指標 (Relative Metrics)
そのレースのメンバー内での相対的な立ち位置を示します。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `weight_diff_from_avg` | 斤量 - レース平均斤量 | ハンデの有利不利 |
| `horse_weight_diff_from_avg` | 馬体重 - レース平均馬体重 | 体格の優位性 |

## 10. バイアス・環境要因 (Bias & Environment)
レース当日の傾向や季節性を捉えます。

| 特徴量名 | 説明 | 備考 |
|---|---|---|
| `bias_seasonal_score` | 季節性バイアススコア | 同時期・同条件での過去の枠順/脚質傾向 |
| `dynamic_avg_finish` | 当日の枠順平均着順 | その日のトラックバイアス（内有利など） |
| `race_season` | 季節 | 春/夏/秋/冬 |
| `field_size_category` | 出走頭数カテゴリ | 少頭数/多頭数 |
| `race_importance` | レース重要度 | 賞金に基づくグレード推定 |

---
**合計特徴量数**: 約 120個
**除外しているデータ**:
- 確定オッズ (`win_odds`)、確定人気 (`popularity`) ※学習時のみ除外（リーク防止）
- レース後のタイム、着差など（未来の情報）
