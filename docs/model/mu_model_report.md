# μモデル (Win Probability Model) レポート

## モデル概要 (Model Overview)

*   **バージョン**: v2.3 (2025-11-27 Update)
*   **目的**: 各出走馬が1着になる確率（勝率）を予測する
*   **アルゴリズム**: LightGBM (Gradient Boosting Decision Tree)
*   **評価指標**: AUC (Area Under the ROC Curve), LogLoss, ROI (回収率)

## パフォーマンス評価 (Performance Evaluation)

2024年のテストデータ（約6万レース）における評価結果です。

| 指標 | 値 | 評価 |
|------|----|------|
| **Test AUC** | **0.7787** | 非常に高い識別精度を示しています。(Previous v2.2: 0.7746) |
| **ROI (Top1)** | **77.47%** | 単勝回収率 (490,600/633,300)。Top1的中率は **27.36%**。 |
| **LogLoss** | **0.2256** | 予測確率の正確性を示す指標（低いほど良い）。 |

> [!NOTE]
> ROIが100%を下回っていますが、これは**データリーク（カンニング）を完全に排除した正当な結果**です。
> 単勝控除率（約20%）を考慮すると、ベースラインとして非常に健全な数値であり、ここからオッズの歪みを突く戦略（σ・νモデル）を組み合わせることで100%超えを目指します。

## 特徴量 (Features)

現在、モデルは以下のカテゴリに分類される特徴量を使用しています。

### 1. 基本情報 (Basic Info)
レースや馬の基本的な属性情報です。
*   `horse_number`: 馬番 (枠順の有利不利を反映)
*   `age`: 馬齢 (成長曲線や衰えを反映)
*   `sex_牡`, `sex_牝`, `sex_セ`: 性別 (牡馬/牝馬/セン馬の特性)
*   `basis_weight`: 斤量 (負担重量の影響)
*   `horse_weight`: 馬体重 (馬格)
*   `horse_weight_change`: 馬体重増減 (当日のコンディション)
*   `distance_m`: 距離 (m)
*   `direction_右`, `direction_左`, `direction_直`: コース周回方向
*   `weather_晴`, `weather_曇`, `weather_雨`, `weather_小雨`, `weather_雪`, `weather_小雪`: 天候
*   `track_condition_良`, `track_condition_稍`, `track_condition_重`, `track_condition_不`: 馬場状態
*   `track_surface`: 芝/ダート

### 2. 過去のパフォーマンス (Past Performance)
馬の過去の競走成績を集約した指標です。近走の勢いや安定感を示します。
*   **着順 (Finish Position)**
    *   `past_1_finish_position_mean/max/min/std`: 前走
    *   `past_3_finish_position_mean/max/min/std`: 近3走
    *   `past_5_finish_position_mean/max/min/std`: 近5走
    *   `past_10_finish_position_mean/max/min/std`: 近10走
*   **上がり3F (Last 3F Time)**
    *   `past_1_last_3f_time_mean/max/min/std` ... `past_10_...`
    *   末脚の切れ味や持続力を示します。
*   **通過順位 (Passing Order)**
    *   `past_1_passing_order_1_mean` ...: 第1コーナー通過順（先行力）
    *   `past_1_passing_order_4_mean` ...: 第4コーナー通過順（位置取り）
*   **賞金 (Prize)**
    *   `prize_total`: 獲得賞金総額（馬の格）
*   **経験 (Experience)**
    *   `career_starts`: 通算出走数
    *   `career_wins`: 通算勝利数
    *   `days_since_last_race`: 前走からの間隔（ローテーション）

### 3. 関係者実績 (Connections)
騎手や調教師の実績データです。
*   **騎手 (Jockey)**
    *   `jockey_win_rate`: 通算勝率
    *   `jockey_rank_avg`: 平均着順
    *   `jockey_races`: 出走回数（経験値）
*   **調教師 (Trainer)**
    *   `trainer_win_rate`: 通算勝率
    *   `trainer_rank_avg`: 平均着順
    *   `trainer_races`: 出走回数

### 4. 血統 (Pedigree)
*   `sire_win_rate`: 父馬（種牡馬）の産駒勝率
*   `sire_rank_avg`: 父馬の産駒平均着順
*   `sire_races`: 父馬の産駒出走回数

### 5. 変化フラグ (Change Flags)
今回と前走の変化を捉えるフラグです。
*   `is_jockey_id_changed`: 騎手乗り替わり（勝負気配や相性）
*   `is_trainer_id_changed`: 転厩

### 6. 交互作用 (Interactions)
特定の条件下での強さを示す組み合わせ特徴量です。
*   **騎手×条件**
    *   `jockey_芝_win_rate`, `jockey_ダート_win_rate`: 馬場適性
    *   `jockey_sprint_win_rate` (~1400m), `jockey_mile_win_rate` (1400-1800m), `jockey_intermediate_win_rate` (1800-2200m), `jockey_long_win_rate` (2200-2800m), `jockey_marathon_win_rate` (2800m+): 距離適性
    *   `jockey_札幌_win_rate`, `jockey_東京_win_rate`, ...: 競馬場適性
*   **調教師×条件**
    *   `trainer_芝_win_rate`, `trainer_ダート_win_rate`
    *   `trainer_sprint_win_rate` ... `trainer_marathon_win_rate`
    *   `trainer_札幌_win_rate` ... `trainer_東京_win_rate`
*   **種牡馬×条件**
    *   `sire_芝_win_rate`, `sire_ダート_win_rate`
    *   `sire_sprint_win_rate` ... `sire_marathon_win_rate`
    *   `sire_札幌_win_rate` ... `sire_東京_win_rate`

### 7. 高度な特徴量 (Advanced Features - v2.3 New)
v2.3で導入された、コンテキストと相対性を重視した特徴量です。

*   **騎手×競馬場 相性 (Jockey-Venue Affinity)**
    *   `jockey_venue_avg_finish`: その競馬場における騎手の平均着順。
    *   `jockey_venue_win_rate`: その競馬場における騎手の勝率。
*   **レース内相対指標 (Relative Metrics)**
    *   `horse_weight_diff_from_avg`: レースメンバー平均馬体重との差。
    *   `age_zscore`: レース内での馬齢偏差値。
*   **展開適合スコア (Pace Fit Score)**
    *   `pace_fit_score`: レースのペース傾向と馬の脚質の適合度。
*   **バイアス (Bias)**
    *   `bias_seasonal_score`: 季節・コース・枠順による有利不利スコア。

## 特徴量重要度 (Feature Importance Analysis)

v2.3における主要な特徴量は以下の通りです。

| Rank | Feature | Importance | Description |
| :--- | :--- | :--- | :--- |
| 1 | `past_1_finish_position_max` | 32,244 | 前走の着順（最も直近のパフォーマンス） |
| 2 | `jockey_win_rate` | 16,530 | 騎手の通算勝率 |
| 3 | `past_3_finish_position_mean` | 14,164 | 近3走の平均着順（安定感） |
| ... | ... | ... | ... |
| **13** | **`jockey_venue_avg_finish`** | **5,119** | **[New] 騎手×競馬場の相性** |
| **15** | **`horse_weight_diff_from_avg`** | **4,455** | **[New] 馬体重の相対差** |
| **23** | **`pace_fit_score`** | **2,792** | **[New] 展開適合度** |

## データリーク対策 (Leakage Prevention)

v2.3では、以下のデータリーク対策を徹底しています。

1.  **未来の情報の排除**: 特徴量生成時、対象レース以降のデータ（結果、オッズ、上がりタイムなど）は一切使用しません。
2.  **オッズの扱い**: `win_odds` (単勝オッズ) は予測時の入力特徴量としては使用せず、レース後のROI計算（評価）のみに使用します。
3.  **再学習プロセス**: 定期的な再学習において、常に過去のデータのみから特徴量を生成するパイプラインを構築しています。
