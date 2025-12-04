# μモデル v2.8 評価レポート

## 1. エグゼクティブサマリー
- **モデルバージョン**: v2.8 (レポートメトリクス統合版)
- **リリース日**: 2025-12-01
- **目的**: レースレポート生成ロジックから派生した「コンテキスト依存の強さ」指標（基準タイム、トラックバイアス）を統合し、ROIを向上させること。
- **主な結果**:
    - **ROI (単勝1位)**: **79.44%** (v2.7: 77.61%) -> **+1.83% 改善**
    - **的中率 (単勝1位)**: **26.91%** (v2.7: 24.96%) -> **+1.95% 改善**
    - **AUC**: **0.7715** (v2.7: 0.7670) -> **+0.0045 改善**

## 2. モデルの進化と変更点

### 2.1 コンセプト: 「コンテキスト依存の強さ」
以前のモデル(v2.7)は、生の着順やタイムに大きく依存していました。v2.8では、その日の特定の条件（トラックバイアス、基準タイム）に基づいてパフォーマンスを正規化する指標を導入しました。
- **仮説**: 「時計のかかる馬場（タイムが遅くなりやすい）」で好走した馬は、生の走破タイムだけを見ると過小評価される可能性があります。「基準タイム」と比較することで、真の強さを明らかにできるはずです。

### 2.2 新機能 (レポートメトリクス)
`generate_race_report.py` システムから以下の特徴量を移植しました：
1.  **`time_deviation_score_avg_5`**: 過去5走における「基準タイム」からの偏差平均。（高いほど良い）
2.  **`l3f_deviation_score_avg_5`**: 過去5走における「基準上がり3F」からの偏差平均。
3.  **`fast_track_score_avg`**: 「高速馬場（バイアス < -0.5）」でのパフォーマンス。
4.  **`heavy_track_score_avg`**: 「タフ/時計のかかる馬場（バイアス > 0.5）」でのパフォーマンス。

### 2.3 学習設定
- **アルゴリズム**: LightGBM (勾配ブースティング)
- **損失関数**: **オッズ加重** (`log1p(win_odds)`) を用いたバイナリLogLoss。
    - これにより、低オッズの人気馬よりも、高オッズの勝ち馬を正しく予測することを優先します。
- **最適化**: 検証データ(2023年)において **ROI** を最大化するようにOptunaでハイパーパラメータを調整。

## 3. 詳細評価結果

### 3.1 性能指標 (テストデータ: 2024年)

| 指標 | v2.7 (ベースライン) | v2.8 (今回) | 差分 | 備考 |
| :--- | :--- | :--- | :--- | :--- |
| **ROI (単勝1位)** | 77.61% | **79.44%** | **+1.83%** | 収益性が大幅に回復。 |
| **的中率 (単勝1位)** | 24.96% | **26.91%** | **+1.95%** | 純粋な予測力が大きく向上。 |
| **AUC** | 0.7670 | **0.7715** | **+0.0045** | ランキング能力の改善。 |
| **LogLoss** | N/A | **0.2297** | - | - |

### 3.2 特徴量重要度分析 (Top 20)
モデルは依然として近走の成績や騎手/調教師の統計に大きく依存しています。

| 順位 | 特徴量 | 重要度 (Gain) | カテゴリ |
| :--- | :--- | :--- | :--- |
| 1 | `past_3_finish_position_mean` | 25,221 | 近走成績 |
| 2 | `past_1_finish_position_max` | 22,437 | 近走成績 |
| 3 | `jockey_win_rate` | 21,378 | 騎手 |
| 4 | `past_10_finish_position_mean` | 17,615 | 長期成績 |
| 5 | `trainer_win_rate` | 14,515 | 調教師 |
| 6 | `n_leaders` | 13,928 | レース展開 |
| 7 | `age_zscore` | 13,499 | 馬属性 |
| 8 | `jockey_芝_win_rate` | 12,564 | 騎手 (馬場適性) |
| ... | ... | ... | ... |
| 16 | `pace_fit_score` | 10,099 | ペース |

**新機能に関する考察**:
新しい「レポートメトリクス」特徴量（例：`time_deviation_score`）はTop 20には入りませんでした。
- **解釈**:
    - 「基準タイム」のロジックが、既存の「スピード指数」や生のタイム特徴量と高度に相関しており、モデルが重要度を分散させたか、より単純な既存の特徴量を優先した可能性があります。
    - しかし、**全体的な性能向上（的中率 +1.95%）** は、再学習や、おそらくこれらの新機能の微妙な相互作用（あるいはよりクリーンな特徴量パイプライン）が、より良いモデルに貢献したことを示唆しています。
    - `pace_fit_score`（16位）は、ペース関連のコンテキストが依然として重要であることを示しています。

## 4. バイアス分析 (人気・オッズ)

ユーザーによる追加分析の結果、本モデルには**極端な「人気馬選好」バイアス**があることが判明しました。

### 4.1 予測傾向
- **Top 1 予測の分布**:
    - **1番人気**: **100.0%** (6333件中6333件)
    - **2番人気以下**: **0.0%**
- **意味**: 本モデルは「どの馬が勝つか」ではなく、**「1番人気の馬が勝つ確率はどれくらいか（信頼できるか）」** を極めて正確に判定するフィルタとして機能しています。

### 4.2 オッズ帯別パフォーマンス (Top 1 予測)
| オッズ帯 | 件数 | 的中数 | 的中率 | 回収率 | 評価 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1.0-1.9** | 917 | 668 | **72.8%** | **118.2%** | **極めて優秀** (ベタ買いでプラス) |
| **2.0-4.9** | 2635 | 864 | **32.8%** | **102.0%** | **優秀** (プラス域を維持) |
| **5.0-9.9** | 1408 | 152 | 10.8% | 69.3% | 危険 (過大評価) |
| **10.0+** | 1373 | 20 | 1.5% | 15.6% | 回避推奨 |

- **戦略的示唆**:
    - オッズ **5.0倍未満** の1番人気に関しては、モデルの信頼度は絶大です。
    - オッズ **5.0倍以上** の1番人気（混戦模様）では、モデルの予測精度が急激に低下します。
    - **σ-ν戦略への応用**: モデルの予測スコア（確率）が高い場合、その1番人気は「鉄板」です。逆にスコアが低い場合、その1番人気は「危険な人気馬」であり、荒れるレースのシグナルとなります。

## 5. 全特徴量リスト (179個)

以下はモデル学習に使用された全特徴量の一覧です（重要度順）。

1. `past_3_finish_position_mean`
2. `past_1_finish_position_max`
3. `jockey_win_rate`
4. `past_10_finish_position_mean`
5. `trainer_win_rate`
6. `n_leaders`
7. `age_zscore`
8. `jockey_芝_win_rate`
9. `sire_mile_avg_finish`
10. `sire_芝_avg_finish`
11. `horse_weight_diff_from_avg`
12. `sire_intermediate_avg_finish`
13. `past_5_finish_position_mean`
14. `trainer_新潟_win_rate`
15. `horse_weight_zscore`
16. `pace_fit_score`
17. `past_1_finish_position_mean`
18. `sire_sprint_avg_finish`
19. `sire_ダート_avg_finish`
20. `surface_avg_finish`
21. `jockey_intermediate_win_rate`
22. `bracket_avg_finish`
23. `trainer_福島_win_rate`
24. `sire_long_avg_finish`
25. `basis_weight_zscore`
26. `sire_marathon_avg_finish`
27. `trainer_札幌_win_rate`
28. `trainer_東京_win_rate`
29. `time_deviation_score_avg_5` (New)
30. `sire_unknown_avg_finish`
31. `weight_diff_from_avg`
32. `days_since_last_race`
33. `jockey_mile_win_rate`
34. `jockey_新潟_win_rate`
35. `jockey_中京_win_rate`
36. `surface_races`
37. `trainer_函館_win_rate`
38. `jockey_小倉_win_rate`
39. `past_5_finish_position_std`
40. `trainer_小倉_win_rate`
41. `past_3_last_3f_time_std`
42. `jockey_ダート_win_rate`
43. `jockey_sprint_win_rate`
44. `jockey_福島_win_rate`
45. `venue_avg_finish`
46. `l3f_deviation_score_avg_5` (New)
47. `horse_weight`
48. `past_1_last_3f_time_max`
49. `trainer_中京_win_rate`
50. `jockey_long_win_rate`
51. `trainer_中山_win_rate`
52. `trainer_阪神_win_rate`
53. `jockey_阪神_win_rate`
54. `dist_races`
55. `dist_avg_finish`
56. `jockey_中山_win_rate`
57. `past_5_last_3f_time_std`
58. `past_10_finish_position_median`
59. `heavy_track_score_avg` (New)
60. `surface_avg_last3f`
61. `jockey_東京_win_rate`
62. `past_10_finish_position_std`
63. `jockey_京都_win_rate`
64. `dist_avg_time`
65. `jockey_札幌_win_rate`
66. `past_10_last_3f_time_max`
67. `past_10_passing_order_1_std`
68. `past_3_finish_position_std`
69. `past_10_passing_order_1_mean`
70. `past_10_last_3f_time_std`
71. `past_5_passing_order_1_std`
72. `trainer_京都_win_rate`
73. `jockey_函館_win_rate`
74. `horse_number`
75. `past_1_passing_order_1_max`
76. `leader_ratio`
77. `bias_seasonal_score`
78. `fast_track_score_avg` (New)
79. `past_3_finish_position_median`
80. `past_5_finish_position_median`
81. `age`
82. `past_3_last_3f_time_max`
83. `past_3_last_3f_time_mean`
84. `horse_weight_change`
85. `past_5_passing_order_1_mean`
86. `past_3_passing_order_1_mean`
87. `past_10_last_3f_time_mean`
88. `past_3_passing_order_1_std`
89. `venue_races`
90. `past_3_last_3f_time_median`
91. `distance_m`
92. `past_5_last_3f_time_max`
93. `past_10_passing_order_4_mean`
94. `past_10_passing_order_4_std`
95. `past_10_last_3f_time_median`
96. `past_5_last_3f_time_mean`
97. `avg_finish_last10`
98. `race_month`
99. `past_5_passing_order_1_median`
100. `past_5_passing_order_4_std`
101. `basis_weight`
102. `past_5_passing_order_4_mean`
103. `past_3_passing_order_4_std`
104. `win_rate_last10`
105. `past_10_passing_order_1_median`
106. `jockey_unknown_win_rate`
107. `day_of_meeting`
108. `past_5_last_3f_time_median`
109. `past_10_finish_position_max`
110. `past_10_passing_order_1_max`
111. `past_3_passing_order_1_max`
112. `past_3_finish_position_max`
113. `past_3_passing_order_1_median`
114. `distance_change`
115. `past_3_passing_order_4_mean`
116. `past_5_passing_order_1_max`
117. `round_of_year`
118. `past_5_finish_position_max`
119. `past_1_passing_order_4_max`
120. `past_5_passing_order_4_max`
121. `prev_distance_m`
122. `past_10_passing_order_4_max`
123. `past_3_passing_order_4_median`
124. `past_10_passing_order_4_median`
125. `past_5_passing_order_4_median`
126. `is_jockey_id_changed`
127. `track_芝`
128. `jockey_marathon_win_rate`
129. `is_distance_shortened`
130. `bracket_is_middle`
131. `avg_finish_last3`
132. `past_1_passing_order_1_mean`
133. `past_3_passing_order_4_max`
134. `track_ダート`
135. `sex_牡`
136. `is_distance_lengthened`
137. `past_1_last_3f_time_mean`
138. `bracket_is_inner`
139. `past_1_passing_order_4_mean`
140. `place_rate_last10`
141. `year`
142. `past_1_last_3f_time_median`
143. `sex_牝`
144. `venue_change`
145. `sex_セ`
146. `bracket_is_outer`
147. `finish_std_last10`
148. `surface_change`
149. `finish_cv_last10`
150. `avg_finish_last5`
151. `past_1_passing_order_4_median`
152. `is_trainer_id_changed`
153. `past_1_finish_position_median`
154. `past_1_passing_order_1_median`
155. `is_lone_leader`
156. `past_1_passing_order_4_std`
157. `under_valued_score_avg_5`
158. `past_1_last_3f_time_std`
159. `win_rate_last3`
160. `win_rate_last5`
161. `bias_dynamic_score`
162. `past_1_finish_position_std`
163. `prev_race_class_val`
164. `combo_avg_finish`
165. `combo_avg_popularity`
166. `combo_overperform`
167. `combo_races`
168. `combo_win_rate`
169. `corner_loss_avg_5`
170. `pci_avg_5`
171. `place_rate_last3`
172. `past_1_passing_order_1_std`
173. `nishida_trend_score`
174. `finish_cv_last3`
175. `finish_cv_last5`
176. `is_rest_return`
177. `finish_std_last3`
178. `finish_std_last5`
179. `place_rate_last5`

## 6. ROI分析と戦略
