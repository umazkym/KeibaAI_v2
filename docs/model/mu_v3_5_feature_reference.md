# μモデル v3.5 特徴量リファレンス

## 概要
- **モデルバージョン**: v3.5
- **Test ROI**: 81.84%
- **特徴量数**: 165個
- **作成日**: 2024-12-04

---

## 特徴量重要度ランキング（全165特徴量）

| 順位 | 特徴量名 | 重要度 | カテゴリ | 解説 |
|------|----------|--------|----------|------|
| 1 | `gap_ability_popularity` | 252 | Gap Feature | **最重要特徴量**。過去5走平均着順から算出した能力ランクと人気順位の差。正の値は「能力に対して過小評価」を示し、ROI向上に最も寄与 |
| 2 | `horse_number` | 45 | 基本情報 | 馬番（1〜18）。内枠・外枠の有利不利、スタート位置の影響を反映 |
| 3 | `horse_weight_zscore` | 36 | 馬体重 | 馬体重の標準化スコア。レース条件（距離・馬場）に対する適正体重からの乖離度 |
| 4 | `sire_win_rate` | 34 | 血統 | 父馬の産駒全体の勝率。血統による基本的な勝利能力の指標 |
| 5 | `gap_jockey_popularity` | 33 | Gap Feature | 騎手勝率ランクと人気順位の差。騎手の実力に対する市場評価のギャップを検出 |
| 6 | `gap_pedigree_popularity` | 32 | Gap Feature | 血統（父馬勝率）ランクと人気順位の差。血統能力の過小評価を検出 |
| 7 | `sire_avg_finish` | 27 | 血統 | 父馬産駒の平均着順。低いほど優秀な血統 |
| 8 | `gap_course_fit_popularity` | 21 | Gap Feature | コース適性ランクと人気順位の差。コース相性の過小評価を検出 |
| 9 | `past_5_last_3f_time_std` | 20 | 過去戦績 | 過去5走の上がり3Fタイムの標準偏差。パフォーマンスの安定性を示す |
| 10 | `combo_avg_finish` | 17 | シナジー | 現在の騎手と馬の組み合わせでの過去平均着順。相性の良し悪しを数値化 |
| 11 | `is_jockey_id_changed` | 17 | 騎手 | 前走から騎手変更があったかどうか（0/1）。乗り替わりの影響を評価 |
| 12 | `past_1_passing_order_1_mean` | 17 | 脚質 | 前走の1コーナー通過順位。先行力・脚質の指標 |
| 13 | `sire_course_avg_finish` | 16 | 血統×コース | 父馬産駒の当該コースでの平均着順。血統のコース適性 |
| 14 | `leader_ratio` | 15 | ペース予想 | レース内の先行馬（通過順位3位以内）の比率。ハイペース/スローペースの予測に使用 |
| 15 | `popularity` | 15 | オッズ | 単勝人気順位（1〜18）。レース5分前に確定する市場評価 |
| 16 | `morning_odds` | 15 | オッズ | 前日オッズ。最終オッズとの変動を分析するためのベースライン |
| 17 | `jockey_芝_win_rate` | 15 | 騎手×馬場 | 騎手の芝コースでの勝率。馬場別の騎手能力 |
| 18 | `basis_weight` | 14 | 斤量 | 負担重量（kg）。ハンデ戦での斤量差の影響 |
| 19 | `past_3_last_3f_time_min` | 14 | 過去戦績 | 過去3走での上がり3F最速タイム。末脚の最大能力 |
| 20 | `form_rank` | 14 | 調子 | 直近の調子を数値化したランク。好調/不調の指標 |
| 21 | `race_class_overbet_risk` | 14 | クラス | 当該クラスでの過大評価リスク。昇級馬などのリスク評価 |
| 22 | `trainer_win_rate` | 14 | 調教師 | 調教師の全体勝率。厩舎の基本能力 |
| 23 | `bracket_avg_finish` | 14 | 枠番 | 当該枠番での過去平均着順。枠による有利不利 |
| 24 | `jockey_long_win_rate` | 13 | 騎手×距離 | 騎手の長距離（2200m以上）での勝率 |
| 25 | `time_deviation_score_avg_5` | 13 | タイム指数 | 過去5走のタイム偏差スコア平均。レース水準に対する相対的なタイム能力 |
| 26 | `form_rank` | 14 | 調子 | 馬の調子ランク |
| 27 | `race_class_overbet_risk` | 14 | クラス | クラスでの過大評価リスク |
| 28 | `trainer_win_rate` | 14 | 調教師 | 調教師の勝率 |
| 29 | `bracket_avg_finish` | 14 | 枠番 | 枠番別平均着順 |
| 30 | `jockey_long_win_rate` | 13 | 騎手×距離 | 騎手の長距離勝率 |
| 31 | `time_deviation_score_avg_5` | 13 | タイム指数 | 過去5走のタイム偏差スコア平均 |
| 32 | `pace_fit_score` | 13 | ペース | ペース適合スコア。予想ペースと脚質の相性 |
| 33 | `finish_cv_last3` | 12 | 過去戦績 | 過去3走の着順変動係数。安定性の指標 |
| 34 | `trainer_阪神_win_rate` | 12 | 調教師×競馬場 | 調教師の阪神競馬場での勝率 |
| 35 | `jockey_ダート_win_rate` | 12 | 騎手×馬場 | 騎手のダートコースでの勝率 |
| 36 | `jockey_mile_win_rate` | 12 | 騎手×距離 | 騎手のマイル（1400-1800m）での勝率 |
| 37 | `surface_avg_last3f` | 11 | コース適性 | 当該馬場での上がり3F平均タイム |
| 38 | `basis_weight_zscore` | 11 | 斤量 | 斤量の標準化スコア |
| 39 | `bms_avg_finish` | 11 | 血統 | 母父（BMS）産駒の平均着順 |
| 40 | `gap_pace_popularity` | 11 | Gap Feature | ペース適性ランクと人気の差 |
| 41 | `past_5_margin_seconds_mean` | 11 | 過去戦績 | 過去5走の着差（秒）平均。勝者との差 |
| 42 | `combo_overperform` | 11 | シナジー | 騎手×馬の組み合わせでの期待値超過度 |
| 43 | `past_3_passing_order_4_mean` | 11 | 脚質 | 過去3走の4コーナー通過順位平均 |
| 44 | `bms_win_rate` | 11 | 血統 | 母父（BMS）産駒の勝率 |
| 45 | `sire_course_win_rate` | 10 | 血統×コース | 父馬産駒の当該コース勝率 |
| 46 | `jockey_福島_win_rate` | 10 | 騎手×競馬場 | 騎手の福島競馬場での勝率 |
| 47 | `past_3_margin_seconds_std` | 10 | 過去戦績 | 過去3走の着差標準偏差 |
| 48 | `jockey_新潟_win_rate` | 10 | 騎手×競馬場 | 騎手の新潟競馬場での勝率 |
| 49 | `past_3_passing_order_1_std` | 10 | 脚質 | 過去3走の1コーナー通過順位標準偏差 |
| 50 | `combo_races` | 10 | シナジー | 騎手×馬の組み合わせでの出走回数 |
| 51 | `past_10_finish_position_mean` | 10 | 過去戦績 | 過去10走の平均着順 |
| 52 | `gap_trainer_popularity` | 9 | Gap Feature | 調教師ランクと人気の差 |
| 53 | `sire_wet_boost` | 9 | 血統×馬場 | 父馬産駒の重馬場でのブースト効果 |
| 54 | `jockey_小倉_win_rate` | 9 | 騎手×競馬場 | 騎手の小倉競馬場での勝率 |
| 55 | `past_3_last_3f_time_std` | 9 | 過去戦績 | 過去3走の上がり3F標準偏差 |
| 56 | `jockey_中京_win_rate` | 9 | 騎手×競馬場 | 騎手の中京競馬場での勝率 |
| 57 | `past_3_last_3f_time_max` | 9 | 過去戦績 | 過去3走の上がり3F最遅タイム |
| 58 | `nicks_avg_finish` | 9 | 血統相性 | ニックス（血統相性）での平均着順 |
| 59 | `past_5_passing_order_1_mean` | 8 | 脚質 | 過去5走の1コーナー通過順位平均 |
| 60 | `dist_avg_time` | 8 | 距離適性 | 当該距離での平均タイム |
| 61 | `sire_course_place_rate` | 8 | 血統×コース | 父馬産駒の当該コース複勝率 |
| 62 | `jockey_東京_win_rate` | 8 | 騎手×競馬場 | 騎手の東京競馬場での勝率 |
| 63 | `past_3_finish_position_std` | 8 | 過去戦績 | 過去3走の着順標準偏差 |
| 64 | `heavy_track_score_avg` | 8 | 馬場適性 | 重馬場でのスコア平均 |
| 65 | `trainer_函館_win_rate` | 8 | 調教師×競馬場 | 調教師の函館競馬場での勝率 |
| 66 | `past_10_passing_order_1_std` | 8 | 脚質 | 過去10走の1コーナー通過順位標準偏差 |
| 67 | `jockey_札幌_win_rate` | 8 | 騎手×競馬場 | 騎手の札幌競馬場での勝率 |
| 68 | `past_10_finish_position_std` | 7 | 過去戦績 | 過去10走の着順標準偏差 |
| 69 | `jockey_win_rate` | 7 | 騎手 | 騎手の全体勝率 |
| 70 | `jockey_sprint_win_rate` | 7 | 騎手×距離 | 騎手のスプリント（〜1400m）勝率 |
| 71 | `past_10_last_3f_time_std` | 7 | 過去戦績 | 過去10走の上がり3F標準偏差 |
| 72 | `avg_finish_last10` | 7 | 過去戦績 | 過去10走の平均着順（別計算） |
| 73 | `finish_cv_last5` | 7 | 過去戦績 | 過去5走の着順変動係数 |
| 74 | `jockey_intermediate_win_rate` | 7 | 騎手×距離 | 騎手の中距離（1800-2200m）勝率 |
| 75 | `past_5_last_3f_time_mean` | 6 | 過去戦績 | 過去5走の上がり3F平均タイム |
| 76 | `past_5_finish_position_mean` | 6 | 過去戦績 | 過去5走の平均着順 |
| 77 | `trainer_京都_win_rate` | 6 | 調教師×競馬場 | 調教師の京都競馬場での勝率 |
| 78 | `l3f_deviation_score_avg_5` | 6 | タイム指数 | 過去5走の上がり3F偏差スコア平均 |
| 79 | `past_5_passing_order_4_std` | 6 | 脚質 | 過去5走の4コーナー通過順位標準偏差 |
| 80 | `past_3_passing_order_4_std` | 6 | 脚質 | 過去3走の4コーナー通過順位標準偏差 |
| 81 | `jockey_中山_win_rate` | 6 | 騎手×競馬場 | 騎手の中山競馬場での勝率 |
| 82 | `past_5_margin_seconds_std` | 6 | 過去戦績 | 過去5走の着差標準偏差 |
| 83 | `trainer_東京_win_rate` | 6 | 調教師×競馬場 | 調教師の東京競馬場での勝率 |
| 84 | `jockey_阪神_win_rate` | 6 | 騎手×競馬場 | 騎手の阪神競馬場での勝率 |
| 85 | `past_10_passing_order_4_std` | 6 | 脚質 | 過去10走の4コーナー通過順位標準偏差 |
| 86 | `bias_seasonal_score` | 6 | バイアス | 季節による成績バイアススコア |
| 87 | `past_10_passing_order_1_mean` | 6 | 脚質 | 過去10走の1コーナー通過順位平均 |
| 88 | `past_10_last_3f_time_mean` | 6 | 過去戦績 | 過去10走の上がり3F平均タイム |
| 89 | `nicks_win_rate` | 6 | 血統相性 | ニックス（血統相性）での勝率 |
| 90 | `dist_avg_finish` | 6 | 距離適性 | 当該距離での平均着順 |
| 91 | `finish_std_last10` | 6 | 過去戦績 | 過去10走の着順標準偏差 |
| 92 | `horse_weight_change` | 5 | 馬体重 | 前走からの馬体重変動（kg） |
| 93 | `venue_avg_finish` | 5 | 競馬場適性 | 当該競馬場での平均着順 |
| 94 | `past_10_last_3f_time_median` | 5 | 過去戦績 | 過去10走の上がり3F中央値 |
| 95 | `past_5_passing_order_1_median` | 5 | 脚質 | 過去5走の1コーナー通過順位中央値 |
| 96 | `past_5_finish_position_max` | 5 | 過去戦績 | 過去5走の最悪着順 |
| 97 | `finish_std_last3` | 5 | 過去戦績 | 過去3走の着順標準偏差 |
| 98 | `past_3_margin_seconds_mean` | 5 | 過去戦績 | 過去3走の着差平均 |
| 99 | `past_1_last_3f_time_max` | 4 | 過去戦績 | 前走の上がり3Fタイム |
| 100 | `past_10_last_3f_time_max` | 4 | 過去戦績 | 過去10走の上がり3F最遅タイム |
| 101 | `days_since_last_race` | 4 | ローテーション | 前走からの日数。休養明けの影響 |
| 102 | `combo_win_rate` | 4 | シナジー | 騎手×馬の組み合わせ勝率 |
| 103 | `past_10_margin_seconds_mean` | 4 | 過去戦績 | 過去10走の着差平均 |
| 104 | `past_10_margin_seconds_median` | 4 | 過去戦績 | 過去10走の着差中央値 |
| 105 | `bracket_is_outer` | 4 | 枠番 | 外枠フラグ（6-8枠） |
| 106 | `avg_finish_last5` | 4 | 過去戦績 | 過去5走の平均着順（別計算） |
| 107 | `finish_std_last5` | 4 | 過去戦績 | 過去5走の着順標準偏差 |
| 108 | `past_3_margin_seconds_median` | 4 | 過去戦績 | 過去3走の着差中央値 |
| 109 | `place_rate_last10` | 4 | 過去戦績 | 過去10走の複勝率 |
| 110 | `past_5_finish_position_std` | 4 | 過去戦績 | 過去5走の着順標準偏差 |
| 111 | `past_3_margin_seconds_max` | 4 | 過去戦績 | 過去3走の最大着差 |
| 112 | `past_5_last_3f_time_median` | 4 | 過去戦績 | 過去5走の上がり3F中央値 |
| 113 | `past_5_passing_order_4_mean` | 4 | 脚質 | 過去5走の4コーナー通過順位平均 |
| 114 | `past_5_last_3f_time_max` | 4 | 過去戦績 | 過去5走の上がり3F最遅タイム |
| 115 | `prev_distance_m` | 4 | 距離 | 前走の距離（m） |
| 116 | `past_3_last_3f_time_median` | 4 | 過去戦績 | 過去3走の上がり3F中央値 |
| 117 | `past_3_passing_order_1_mean` | 4 | 脚質 | 過去3走の1コーナー通過順位平均 |
| 118 | `past_1_passing_order_1_max` | 4 | 脚質 | 前走の1コーナー通過順位 |
| 119 | `past_3_passing_order_1_median` | 3 | 脚質 | 過去3走の1コーナー通過順位中央値 |
| 120 | `sex_牡` | 3 | 性別 | 牡馬フラグ |
| 121 | `past_5_margin_seconds_median` | 3 | 過去戦績 | 過去5走の着差中央値 |
| 122 | `distance_change` | 3 | 距離 | 前走からの距離変更（m） |
| 123 | `dist_races` | 3 | 距離適性 | 当該距離での出走回数 |
| 124 | `past_3_finish_position_mean` | 3 | 過去戦績 | 過去3走の平均着順 |
| 125 | `n_leaders` | 3 | ペース予想 | レース内の先行馬頭数 |
| 126 | `jockey_unknown_win_rate` | 3 | 騎手 | 騎手の不明条件勝率 |
| 127 | `surface_avg_finish` | 3 | 馬場適性 | 当該馬場（芝/ダート）での平均着順 |
| 128 | `past_10_finish_position_median` | 3 | 過去戦績 | 過去10走の着順中央値 |
| 129 | `fast_track_score_avg` | 3 | 馬場適性 | 良馬場でのスコア平均 |
| 130 | `past_3_passing_order_1_max` | 3 | 脚質 | 過去3走の1コーナー通過順位最大 |
| 131 | `is_overvalued` | 3 | オッズ | 過大評価フラグ |
| 132 | `surface_races` | 3 | 馬場適性 | 当該馬場での出走回数 |
| 133 | `finish_cv_last10` | 3 | 過去戦績 | 過去10走の着順変動係数 |
| 134 | `avg_finish_last3` | 3 | 過去戦績 | 過去3走の平均着順 |
| 135 | `past_10_passing_order_1_median` | 3 | 脚質 | 過去10走の1コーナー通過順位中央値 |
| 136 | `past_10_passing_order_4_max` | 3 | 脚質 | 過去10走の4コーナー通過順位最大 |
| 137 | `day_of_meeting` | 2 | 開催情報 | 開催日（1-8日目） |
| 138 | `past_1_margin_seconds_max` | 2 | 過去戦績 | 前走の着差 |
| 139 | `jockey_京都_win_rate` | 2 | 騎手×競馬場 | 騎手の京都競馬場での勝率 |
| 140 | `jockey_函館_win_rate` | 2 | 騎手×競馬場 | 騎手の函館競馬場での勝率 |
| 141 | `past_5_passing_order_1_std` | 2 | 脚質 | 過去5走の1コーナー通過順位標準偏差 |
| 142 | `past_1_last_3f_time_mean` | 2 | 過去戦績 | 前走の上がり3Fタイム |
| 143 | `past_3_finish_position_max` | 2 | 過去戦績 | 過去3走の最悪着順 |
| 144 | `past_1_passing_order_4_max` | 2 | 脚質 | 前走の4コーナー通過順位 |
| 145 | `past_10_margin_seconds_max` | 2 | 過去戦績 | 過去10走の最大着差 |
| 146 | `race_month` | 2 | 開催情報 | レース月（1-12） |
| 147 | `past_10_passing_order_4_mean` | 2 | 脚質 | 過去10走の4コーナー通過順位平均 |
| 148 | `round_of_year` | 2 | 開催情報 | 年間開催回次 |
| 149 | `win_rate_last5` | 2 | 過去戦績 | 過去5走の勝率 |
| 150 | `age` | 1 | 基本情報 | 馬齢 |
| 151 | `venue_races` | 1 | 競馬場適性 | 当該競馬場での出走回数 |
| 152 | `past_5_finish_position_median` | 1 | 過去戦績 | 過去5走の着順中央値 |
| 153 | `past_3_passing_order_4_max` | 1 | 脚質 | 過去3走の4コーナー通過順位最大 |
| 154 | `past_3_last_3f_time_mean` | 1 | 過去戦績 | 過去3走の上がり3F平均タイム |
| 155 | `past_10_passing_order_1_max` | 1 | 脚質 | 過去10走の1コーナー通過順位最大 |
| 156 | `past_10_finish_position_max` | 1 | 過去戦績 | 過去10走の最悪着順 |
| 157 | `bracket_is_middle` | 1 | 枠番 | 中枠フラグ（3-5枠） |
| 158 | `jockey_marathon_win_rate` | 0 | 騎手×距離 | 騎手のマラソン（3000m以上）勝率 |
| 159 | `past_5_passing_order_4_median` | 0 | 脚質 | 過去5走の4コーナー通過順位中央値 |
| 160 | `past_5_margin_seconds_max` | 0 | 過去戦績 | 過去5走の最大着差 |
| 161 | `past_3_finish_position_median` | 0 | 過去戦績 | 過去3走の着順中央値 |
| 162 | `past_10_passing_order_4_median` | 0 | 脚質 | 過去10走の4コーナー通過順位中央値 |
| 163 | `distance_m` | 0 | 距離 | レース距離（m） |
| 164 | `bracket_is_inner` | 0 | 枠番 | 内枠フラグ（1-2枠） |
| 165 | `past_3_passing_order_4_median` | 0 | 脚質 | 過去3走の4コーナー通過順位中央値 |

---

## カテゴリ別サマリー

### Gap Features（5個）
市場評価と実力のギャップを検出する最重要カテゴリ。

| 特徴量 | 重要度 | 解説 |
|--------|--------|------|
| `gap_ability_popularity` | 252 | 能力ランク vs 人気 |
| `gap_jockey_popularity` | 33 | 騎手ランク vs 人気 |
| `gap_pedigree_popularity` | 32 | 血統ランク vs 人気 |
| `gap_course_fit_popularity` | 21 | コース適性 vs 人気 |
| `gap_pace_popularity` | 11 | ペース適性 vs 人気 |
| `gap_trainer_popularity` | 9 | 調教師ランク vs 人気 |

### 血統系（約15個）
父馬・母父の産駒傾向から能力を推定。

| 特徴量 | 重要度 | 解説 |
|--------|--------|------|
| `sire_win_rate` | 34 | 父馬産駒勝率 |
| `sire_avg_finish` | 27 | 父馬産駒平均着順 |
| `sire_course_avg_finish` | 16 | 父馬産駒のコース別成績 |
| `bms_avg_finish` | 11 | 母父産駒平均着順 |
| `bms_win_rate` | 11 | 母父産駒勝率 |
| `sire_course_win_rate` | 10 | 父馬産駒のコース勝率 |
| `sire_wet_boost` | 9 | 父馬産駒の重馬場適性 |
| `nicks_avg_finish` | 9 | 血統相性（ニックス）成績 |

### 騎手系（約20個）
騎手の能力・適性を多角的に評価。

| 特徴量 | 重要度 | 解説 |
|--------|--------|------|
| `is_jockey_id_changed` | 17 | 騎手変更フラグ |
| `jockey_芝_win_rate` | 15 | 芝コース勝率 |
| `jockey_long_win_rate` | 13 | 長距離勝率 |
| `jockey_ダート_win_rate` | 12 | ダートコース勝率 |
| `jockey_mile_win_rate` | 12 | マイル勝率 |
| `jockey_*_win_rate` | 2-10 | 各競馬場別勝率 |

### 過去戦績系（約40個）
直近レースのパフォーマンスを詳細に分析。

| カテゴリ | 特徴量例 | 解説 |
|----------|----------|------|
| 着順系 | `past_*_finish_position_*` | 平均/標準偏差/中央値/最大 |
| 上がり3F | `past_*_last_3f_time_*` | 末脚の能力と安定性 |
| 通過順位 | `past_*_passing_order_*` | 脚質の分析 |
| 着差 | `past_*_margin_seconds_*` | 勝者との差 |

---

## 重要な発見

### 1. Gap Featuresの圧倒的な重要性
`gap_ability_popularity`（重要度252）が他を大きく上回っています。これは「能力に対して市場が過小評価している馬」を見つけることがROI向上の最大の鍵であることを示しています。

### 2. 血統情報の有効性
父馬・母父の産駒傾向は、初出走馬や条件変更時に特に有効です。

### 3. 重要度0の特徴量
以下の7特徴量は削除してもモデルに影響がありません：
- `jockey_marathon_win_rate`
- `past_5_passing_order_4_median`
- `past_5_margin_seconds_max`
- `past_3_finish_position_median`
- `past_10_passing_order_4_median`
- `distance_m`
- `bracket_is_inner`
- `past_3_passing_order_4_median`

---

## 改善の方向性

現在のモデルは**Gap Features**に強く依存しています。ROIをさらに向上させるには：

1. **新しいGap Featureの開発** - 調教師×条件、血統×馬場など
2. **時系列の詳細化** - 直近1走の重み付け強化
3. **外部データの追加** - 調教データ、パドック情報など

---

*作成: 2024-12-04 | μモデル v3.5*
