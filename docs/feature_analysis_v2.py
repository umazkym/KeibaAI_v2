"""
KeibaAI_v2 特徴量拡張分析フレームワーク
=========================================
目的: 市場の非効率性を捉える新たな特徴量の発見
原則: Point-in-time設計、リーク防止、過学習防止
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum

class DataSource(Enum):
    RACES = "races.parquet"
    PEDIGREES = "pedigrees.parquet"
    RACE_DETAILS = "race_details.parquet"
    CORNERS = "corner_positions.parquet"
    SHUTUBA = "shutuba.parquet"
    HORSES = "horses.parquet"
    RETURNS = "returns.parquet"

class LeakRisk(Enum):
    NONE = "なし"
    LOW = "低"
    MEDIUM = "中"
    HIGH = "高"

class MarketBlindSpot(Enum):
    """市場が見落としやすいポイント"""
    PEDIGREE_DEPTH = "血統の深層分析"
    PACE_MISMATCH = "ペース不適合"
    HIDDEN_FORM = "隠れた好調"
    CONDITION_SPECIALIST = "条件適性の専門性"
    MOMENTUM = "勢い・トレンド"
    TACTICAL_ADVANTAGE = "戦術的優位性"
    ODDS_INFORMATION = "オッズ情報"
    PHYSICAL_CONDITION = "身体コンディション"
    ENVIRONMENTAL_FIT = "環境適合"

@dataclass
class FeatureProposal:
    name: str
    description: str
    data_sources: List[DataSource]
    calculation_logic: str
    leak_risk: LeakRisk
    leak_prevention: str
    market_blind_spot: MarketBlindSpot
    expected_impact: str
    implementation_complexity: str  # 低/中/高

# ============================================================================
# カテゴリ1: 血統深層分析（Pedigree Deep Analysis）
# ============================================================================
pedigree_features = [
    FeatureProposal(
        name="broodmare_sire_win_rate",
        description="母父（BMSire = generation2の母方）の産駒勝率",
        data_sources=[DataSource.PEDIGREES, DataSource.RACES],
        calculation_logic="""
        1. pedigrees.parquetからgeneration=2の母方祖先(母父)を特定
        2. 母父の産駒の過去勝率を計算（shift(1)で当該レース除外）
        3. 特に長距離・重馬場での母父影響は大きい
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="累積統計にshift(1)適用、当該レース以前のデータのみ使用",
        market_blind_spot=MarketBlindSpot.PEDIGREE_DEPTH,
        expected_impact="中〜高：母父の影響は3歳以降・長距離で顕在化しやすく市場が軽視",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="sire_distance_specialization",
        description="父の産駒の距離別勝率（短距離/中距離/長距離）",
        data_sources=[DataSource.PEDIGREES, DataSource.RACES],
        calculation_logic="""
        1. 父産駒の過去成績を距離カテゴリ別に集計
        2. 当該レース距離カテゴリでの父産駒勝率を特徴量化
        3. 市場は「サンデーサイレンス系」など大括りで評価しがち
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="shift(1)適用、固定値として事前計算も可",
        market_blind_spot=MarketBlindSpot.PEDIGREE_DEPTH,
        expected_impact="中：距離適性の血統バイアスを定量化",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="sire_track_condition_affinity",
        description="父産駒の馬場状態別勝率（良/稍重/重/不良）",
        data_sources=[DataSource.PEDIGREES, DataSource.RACES],
        calculation_logic="""
        1. 父産駒の馬場状態別成績を集計
        2. 重馬場巧者血統など、条件適性を捕捉
        3. 例：ハーツクライ産駒の重馬場成績 vs ディープインパクト産駒
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="固定統計値として事前計算、または累積にshift(1)",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="中：重馬場・道悪での血統適性は市場が過小評価",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="inbreeding_coefficient",
        description="近親交配係数（特定祖先の重複出現）",
        data_sources=[DataSource.PEDIGREES],
        calculation_logic="""
        1. generation 1-3の祖先IDの重複をチェック
        2. 例：サンデーサイレンス3×4（3代目と4代目に同一祖先）
        3. 適度なインブリードは能力固定、過度は虚弱
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="血統データは出生時に確定、将来情報なし",
        market_blind_spot=MarketBlindSpot.PEDIGREE_DEPTH,
        expected_impact="低〜中：専門的すぎて市場反映されにくい",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="producing_area_win_rate",
        description="生産地別の勝率統計",
        data_sources=[DataSource.HORSES, DataSource.RACES],
        calculation_logic="""
        1. 産地別（早来町、浦河町、日高町など）の勝率を計算
        2. 特に社台系（早来町）vs その他の差異
        3. 生産者規模・設備の影響を間接的に捕捉
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="固定統計として事前計算",
        market_blind_spot=MarketBlindSpot.ENVIRONMENTAL_FIT,
        expected_impact="低〜中：馬主・生産者の影響を間接的に捕捉",
        implementation_complexity="低"
    ),
]

# ============================================================================
# カテゴリ2: ラップタイム・ペース分析（Pace & Lap Analysis）
# ============================================================================
pace_features = [
    FeatureProposal(
        name="race_pace_category",
        description="レースのペースカテゴリ（ハイペース/ミドル/スロー）",
        data_sources=[DataSource.RACE_DETAILS],
        calculation_logic="""
        1. first_half / second_half の比率を計算
        2. 比率 > 1.05: ハイペース（前傾）
        3. 比率 < 0.95: スローペース（後傾）
        4. 0.95-1.05: ミドルペース
        
        重要：これは「過去レース」の情報として使用
        """,
        leak_risk=LeakRisk.HIGH,
        leak_prevention="""
        【警告】当該レースのペースは使用不可（結果情報）
        代わりに：
        - 過去の同コース・同条件レースのペース傾向
        - 出走馬の脚質構成から「予測ペース」を算出
        """,
        market_blind_spot=MarketBlindSpot.PACE_MISMATCH,
        expected_impact="高：ペース予測は市場の最大の盲点の一つ",
        implementation_complexity="高"
    ),
    
    FeatureProposal(
        name="expected_pace_from_running_styles",
        description="出走馬の脚質構成から予測されるペース",
        data_sources=[DataSource.RACES, DataSource.CORNERS],
        calculation_logic="""
        1. 各出走馬の過去脚質（逃げ率、先行率）を集計
        2. 当該レースの逃げ馬数、先行馬数を予測
        3. 逃げ馬複数 → ハイペース予測
        4. 逃げ馬不在 → スローペース予測
        
        これは「予測」なのでリークなし
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="出走馬の過去データのみ使用、当該レース結果は未使用",
        market_blind_spot=MarketBlindSpot.TACTICAL_ADVANTAGE,
        expected_impact="高：展開予測は競馬予想の核心",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="horse_pace_preference",
        description="馬の好むペース（過去成績のペース別勝率）",
        data_sources=[DataSource.RACES, DataSource.RACE_DETAILS],
        calculation_logic="""
        1. 馬の過去レースをペースカテゴリ別に分類
        2. ハイペース/スロー時の成績差を計算
        3. 「スローペース巧者」「ハイペース巧者」を識別
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースのペースと成績のみ使用、shift(1)適用",
        market_blind_spot=MarketBlindSpot.PACE_MISMATCH,
        expected_impact="中〜高：展開適性は市場が見落としやすい",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="lap_time_variance",
        description="過去レースのラップタイム分散（レースの荒れやすさ指標）",
        data_sources=[DataSource.RACE_DETAILS],
        calculation_logic="""
        1. lap_timesリストの標準偏差を計算
        2. 分散大 = 不均一なペース = 荒れやすいレース
        3. 荒れるレースでは人気馬が崩れやすい
        
        用途：同条件過去レースの荒れ傾向を特徴量化
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去の同条件レースの統計を使用",
        market_blind_spot=MarketBlindSpot.PACE_MISMATCH,
        expected_impact="中：荒れやすいレースでの穴馬発見",
        implementation_complexity="中"
    ),
]

# ============================================================================
# カテゴリ3: コーナー通過詳細分析（Corner Position Deep Analysis）
# ============================================================================
corner_features = [
    FeatureProposal(
        name="corner_position_improvement",
        description="コーナー間の順位上昇量（追い込み力の定量化）",
        data_sources=[DataSource.CORNERS],
        calculation_logic="""
        1. corner_positions.parquetから各コーナーの順位を取得
        2. position_change = corner4_pos - corner1_pos
        3. 負の値 = 追い込み馬（後方から前に上がる）
        4. 正の値 = 逃げ・先行馬（前から下がる）
        
        既存のposition_change_X_Yより詳細な分析
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用、shift(1)適用",
        market_blind_spot=MarketBlindSpot.TACTICAL_ADVANTAGE,
        expected_impact="中：脚質の細分化で展開適性をより正確に",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="gap_from_leader_pattern",
        description="先頭との馬身差パターン（各コーナーでの位置取り戦術）",
        data_sources=[DataSource.CORNERS],
        calculation_logic="""
        1. gap_from_leaderの推移パターンを分析
        2. パターン分類：
           - 徐々に詰める型（スタミナ型追い込み）
           - 4角で一気に詰める型（瞬発力型）
           - 終始一定距離型（マイペース先行）
        3. 馬身差の変化率を特徴量化
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースの馬身差パターンのみ使用",
        market_blind_spot=MarketBlindSpot.TACTICAL_ADVANTAGE,
        expected_impact="中〜高：走法パターンの詳細分析は専門家レベル",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="final_corner_acceleration",
        description="最終コーナー(C4)からゴールまでの加速能力",
        data_sources=[DataSource.CORNERS, DataSource.RACES],
        calculation_logic="""
        1. C4での順位 vs 最終順位の差
        2. C4での先頭との馬身差 vs 最終着差
        3. 「4角で後方でも差し切れる馬」を識別
        
        既存のfinal_corner_to_finishを拡張
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ、shift(1)適用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中：末脚の安定性を定量化",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="optimal_position_at_corners",
        description="各コーナーでの最適ポジション（勝馬の平均位置との乖離）",
        data_sources=[DataSource.CORNERS, DataSource.RACES],
        calculation_logic="""
        1. 過去の勝馬の各コーナー平均順位を計算（コース・距離別）
        2. 当該馬の通常ポジションとの差を特徴量化
        3. 「このコースでは4角5番手以内が有利」等のパターン
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去の勝馬統計を事前計算",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="中：コース適性の詳細分析",
        implementation_complexity="中"
    ),
]

# ============================================================================
# カテゴリ4: オッズ情報分析（Odds Intelligence）
# ============================================================================
odds_features = [
    FeatureProposal(
        name="odds_movement",
        description="朝オッズ→最終オッズの変動率",
        data_sources=[DataSource.SHUTUBA, DataSource.RACES],
        calculation_logic="""
        1. morning_odds（shutuba.parquet）と win_odds（races.parquet）を比較
        2. odds_change_ratio = win_odds / morning_odds
        3. ratio < 1: 支持上昇（良い情報あり）
        4. ratio > 1: 支持低下（悪い情報あり）
        
        重要：これは「市場情報」として非常に価値が高い
        """,
        leak_risk=LeakRisk.MEDIUM,
        leak_prevention="""
        【注意】最終オッズは発走直前の情報
        - 予測時点で利用可能なオッズを使用すること
        - リアルタイム予測なら発走前オッズは使用可能
        - 学習時は慎重に（最終オッズは結果と相関しすぎる可能性）
        """,
        market_blind_spot=MarketBlindSpot.ODDS_INFORMATION,
        expected_impact="高：オッズ変動は「内部情報」の代理指標",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="morning_odds_vs_ability",
        description="朝オッズと実力指標の乖離度",
        data_sources=[DataSource.SHUTUBA, DataSource.RACES],
        calculation_logic="""
        1. 朝オッズから算出した勝率期待値
        2. 過去成績から算出した実力指標
        3. 乖離度 = 実力指標 - オッズ期待値
        4. 正の乖離 = 市場過小評価（狙い目）
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="朝オッズと過去成績のみ使用",
        market_blind_spot=MarketBlindSpot.ODDS_INFORMATION,
        expected_impact="高：まさにEdge Scoreの考え方を事前段階で",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="popularity_vs_form",
        description="人気と直近調子の乖離",
        data_sources=[DataSource.SHUTUBA, DataSource.RACES],
        calculation_logic="""
        1. 人気順位（市場評価）
        2. 直近5走の成績傾向（上昇/下降/安定）
        3. 人気低いが調子上昇中 = 狙い目
        4. 人気高いが調子下降中 = 過大評価
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去成績と朝人気のみ使用",
        market_blind_spot=MarketBlindSpot.MOMENTUM,
        expected_impact="中：市場は直近の大敗を過度に嫌う傾向",
        implementation_complexity="低"
    ),
]

# ============================================================================
# カテゴリ5: 身体コンディション分析（Physical Condition）
# ============================================================================
condition_features = [
    FeatureProposal(
        name="optimal_weight_deviation",
        description="最適体重からの乖離度",
        data_sources=[DataSource.RACES, DataSource.SHUTUBA],
        calculation_logic="""
        1. 馬の過去勝利時の平均体重を「最適体重」と定義
        2. 当日体重との差を計算
        3. 最適体重±10kg以内が理想
        4. 大幅増減は調整失敗の可能性
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去の勝利時体重のみ使用、当日体重は出馬表で公開済",
        market_blind_spot=MarketBlindSpot.PHYSICAL_CONDITION,
        expected_impact="中：体重管理の成功/失敗を定量化",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="weight_change_trend",
        description="体重変動のトレンド（成長期/減量期/安定期）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 過去5走の体重推移を分析
        2. 線形回帰で傾きを計算
        3. 正の傾き = 成長期（若馬に多い）
        4. 負の傾き = 減量期（消耗の可能性）
        5. 傾きほぼゼロ = 安定期（好調サイン）
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースの体重データのみ使用",
        market_blind_spot=MarketBlindSpot.PHYSICAL_CONDITION,
        expected_impact="中：体重トレンドは調教状態の間接指標",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="weight_change_volatility",
        description="体重変動の安定性（標準偏差）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 過去レースでの体重変動(horse_weight_change)の標準偏差
        2. 低い = 安定した調整（プロの管理）
        3. 高い = 不安定な調整（体調波の可能性）
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用",
        market_blind_spot=MarketBlindSpot.PHYSICAL_CONDITION,
        expected_impact="低〜中：調教安定性の間接指標",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="age_performance_curve",
        description="年齢別パフォーマンス曲線上の位置",
        data_sources=[DataSource.HORSES, DataSource.RACES],
        calculation_logic="""
        1. 一般的な競走馬のパフォーマンス曲線
           - 3歳: 成長期
           - 4歳: ピーク前半
           - 5歳: ピーク後半
           - 6歳以降: 衰退期
        2. 当該馬の年齢と曲線上の位置
        3. 同年齢馬との相対パフォーマンス
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="年齢は確定情報、一般的曲線は事前知識",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="低〜中：年齢による能力減衰の定量化",
        implementation_complexity="低"
    ),
]

# ============================================================================
# カテゴリ6: 環境・条件適性分析（Environmental Fit）
# ============================================================================
environmental_features = [
    FeatureProposal(
        name="venue_specialization",
        description="競馬場別の成績偏り（コース巧者）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 馬の競馬場別成績を集計
        2. 特定競馬場での勝率が全体平均より高い = コース巧者
        3. 例：中山巧者、東京巧者など
        4. 小回り/大回り、坂の有無などが影響
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ、shift(1)適用",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="中：コース適性は市場が部分的に反映済だが定量化不足",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="season_performance",
        description="季節別のパフォーマンス傾向",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. レース月から季節を分類（春/夏/秋/冬）
        2. 馬の季節別成績を集計
        3. 夏に強い馬、冬に強い馬など
        4. 毛色・血統による暑さ/寒さ耐性の差
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="低〜中：季節適性は見落とされやすい",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="distance_flexibility",
        description="距離適性の柔軟性（守備範囲の広さ）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 馬が好成績を収めた距離の範囲を計算
        2. 範囲広い = 柔軟性高い（距離変更に強い）
        3. 範囲狭い = スペシャリスト（得意距離で強い）
        4. 距離変更時のリスク/チャンス評価に使用
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="中：距離変更への対応力",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="track_surface_preference",
        description="芝/ダート適性の強さ",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 芝レースと ダートレースの成績差
        2. 転向初戦の成績傾向
        3. 血統的な芝/ダート適性との整合性
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用",
        market_blind_spot=MarketBlindSpot.CONDITION_SPECIALIST,
        expected_impact="低：市場は比較的反映済",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="rest_days_effect",
        description="休養日数と成績の関係",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 前走からの日数を計算
        2. 馬の最適休養期間を過去データから推定
        3. 短期間連戦 vs 長期休養明けの成績差
        4. 「叩き2戦目」効果の定量化
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="レース日付は確定情報",
        market_blind_spot=MarketBlindSpot.PHYSICAL_CONDITION,
        expected_impact="中：休養効果は市場が部分的に反映",
        implementation_complexity="低"
    ),
]

# ============================================================================
# カテゴリ7: 組み合わせ特徴量（Interaction Features）
# ============================================================================
interaction_features = [
    FeatureProposal(
        name="jockey_trainer_synergy",
        description="騎手×調教師の相性（コンビ成績）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 騎手-調教師ペアの過去成績を集計
        2. 個々の勝率の単純積より高い = 相性良好
        3. シナジースコア = 実績勝率 / (騎手勝率 × 調教師勝率)
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ、shift(1)適用",
        market_blind_spot=MarketBlindSpot.TACTICAL_ADVANTAGE,
        expected_impact="中：人間関係・コミュニケーションの影響",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="jockey_horse_familiarity",
        description="騎手の当該馬への騎乗経験",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 当該騎手の当該馬への過去騎乗回数
        2. 騎乗経験あり = 馬の特性を理解
        3. 初騎乗 = 手探り状態のリスク
        4. 乗り替わり効果の定量化
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="過去の騎乗履歴のみ使用",
        market_blind_spot=MarketBlindSpot.TACTICAL_ADVANTAGE,
        expected_impact="中：騎手と馬の関係性",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="sire_distance_track_interaction",
        description="父×距離×馬場の3way interaction",
        data_sources=[DataSource.PEDIGREES, DataSource.RACES],
        calculation_logic="""
        1. 父産駒の成績を距離×馬場状態でクロス集計
        2. 例：「ディープインパクト産駒×2000m×良馬場」の勝率
        3. 血統の条件適性をより細かく捕捉
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="固定統計として事前計算",
        market_blind_spot=MarketBlindSpot.PEDIGREE_DEPTH,
        expected_impact="中：条件特化の血統適性",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="pace_match_score_v2",
        description="予測ペース×馬の好むペースのマッチ度（拡張版）",
        data_sources=[DataSource.RACES, DataSource.RACE_DETAILS, DataSource.CORNERS],
        calculation_logic="""
        1. expected_pace_from_running_stylesで予測ペース算出
        2. horse_pace_preferenceで馬の好むペース取得
        3. マッチ度 = 予測ペースと好みペースの一致度
        4. 既存のpace_style_match_scoreを高度化
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="すべて予測・過去データのみ使用",
        market_blind_spot=MarketBlindSpot.PACE_MISMATCH,
        expected_impact="高：展開予測の核心",
        implementation_complexity="高"
    ),
]

# ============================================================================
# カテゴリ8: 市場心理・バイアス特徴量（Market Psychology）
# ============================================================================
market_psychology_features = [
    FeatureProposal(
        name="recency_bias_indicator",
        description="直近成績への市場過剰反応指標",
        data_sources=[DataSource.RACES, DataSource.SHUTUBA],
        calculation_logic="""
        1. 直近1走の成績と人気の関係を分析
        2. 前走大敗 → 人気急落（過剰反応の可能性）
        3. 前走勝利 → 人気急上昇（過剰反応の可能性）
        4. 過去5走平均との乖離で過剰反応度を計算
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去成績と朝人気のみ使用",
        market_blind_spot=MarketBlindSpot.MOMENTUM,
        expected_impact="高：市場の最大バイアスの一つ",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="class_change_market_adjustment",
        description="クラス変更に対する市場の過不足反応",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. クラス昇格時の過去成績を分析
        2. 市場は昇格馬を過小評価しがち（実績不足）
        3. 市場は降格馬を過大評価しがち（格下げショック）
        4. 実際の成績と市場評価の乖離を特徴量化
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去のクラス変更履歴と成績のみ使用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中〜高：既存のis_class_upを拡張",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="jockey_change_signal",
        description="騎手変更の意味（格上げ/格下げ）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 前走騎手と今回騎手の勝率差を計算
        2. 格上げ（強い騎手に変更）= 陣営の期待
        3. 格下げ（弱い騎手に変更）= 期待低下
        4. 騎手変更は陣営の「隠れた意図」を反映
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="騎手情報は出馬表で公開済、過去勝率で評価",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中：陣営の意図を読み取る",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="field_size_effect",
        description="出走頭数による成績変動",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 出走頭数別の馬の成績を分析
        2. 少頭数に強い馬/多頭数に強い馬
        3. 多頭数は不利が多く、実力馬が崩れやすい
        4. 少頭数は実力通りになりやすい
        """,
        leak_risk=LeakRisk.NONE,
        leak_prevention="出走頭数は出馬表で確定",
        market_blind_spot=MarketBlindSpot.ENVIRONMENTAL_FIT,
        expected_impact="低〜中：頭数効果の個体差",
        implementation_complexity="低"
    ),
]

# ============================================================================
# カテゴリ9: 高度な統計特徴量（Advanced Statistics）
# ============================================================================
advanced_features = [
    FeatureProposal(
        name="elo_rating",
        description="馬のEloレーティング（チェス式レーティング）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. チェスのEloレーティングを競馬に適用
        2. 勝敗だけでなく、相手の強さを考慮
        3. 強い馬に勝つとレーティング大幅上昇
        4. 弱い馬に負けるとレーティング大幅下降
        
        時系列で累積計算、各レース後に更新
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="当該レース以前のデータのみで計算、shift(1)必須",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="高：相手の強さを考慮した実力評価",
        implementation_complexity="高"
    ),
    
    FeatureProposal(
        name="finishing_time_standardized",
        description="走破タイムの標準化（条件補正後）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 距離・馬場状態・競馬場でタイムを補正
        2. 同条件レースの平均タイムとの差
        3. スピード指数的な考え方
        4. 「絶対的な速さ」の指標
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースタイムのみ使用、shift(1)適用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中〜高：タイム分析の標準化",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="beaten_margin_analysis",
        description="着差の詳細分析（負けた相手の強さ考慮）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 着差(margin_seconds)と勝馬の強さを組み合わせ
        2. 強い馬に0.1秒差 vs 弱い馬に0.1秒差は意味が違う
        3. 「惜敗」と「完敗」を定量的に区別
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースデータのみ使用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中：着差の質的分析",
        implementation_complexity="中"
    ),
    
    FeatureProposal(
        name="consistency_score",
        description="成績の安定性スコア",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 過去成績の標準偏差を計算
        2. 低い = 安定した成績（予測しやすい）
        3. 高い = 不安定な成績（大穴or大敗）
        4. リスク・リターンのトレードオフ指標
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去成績のみ使用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="低〜中：予測可能性の指標",
        implementation_complexity="低"
    ),
    
    FeatureProposal(
        name="last3f_relative_to_field",
        description="上がり3Fの相対評価（レース内順位）",
        data_sources=[DataSource.RACES],
        calculation_logic="""
        1. 各レースでの上がり3F順位を計算
        2. 絶対タイムより相対順位が重要
        3. 馬場状態による補正不要
        4. 末脚の相対的な強さを測定
        """,
        leak_risk=LeakRisk.LOW,
        leak_prevention="過去レースの上がり3F順位のみ使用",
        market_blind_spot=MarketBlindSpot.HIDDEN_FORM,
        expected_impact="中：末脚の相対評価",
        implementation_complexity="低"
    ),
]

# ============================================================================
# 全特徴量のサマリー出力
# ============================================================================
all_features = {
    "血統深層分析": pedigree_features,
    "ペース・ラップ分析": pace_features,
    "コーナー通過詳細": corner_features,
    "オッズ情報": odds_features,
    "身体コンディション": condition_features,
    "環境・条件適性": environmental_features,
    "組み合わせ特徴量": interaction_features,
    "市場心理・バイアス": market_psychology_features,
    "高度な統計": advanced_features,
}

def print_summary():
    print("=" * 80)
    print("KeibaAI_v2 特徴量拡張提案サマリー")
    print("=" * 80)
    
    total = 0
    high_impact = []
    
    for category, features in all_features.items():
        print(f"\n【{category}】 ({len(features)}個)")
        for f in features:
            impact_marker = "★" if "高" in f.expected_impact else ""
            risk_marker = "⚠" if f.leak_risk in [LeakRisk.MEDIUM, LeakRisk.HIGH] else ""
            print(f"  {impact_marker}{risk_marker} {f.name}: {f.description}")
            
            if "高" in f.expected_impact:
                high_impact.append(f)
            total += 1
    
    print("\n" + "=" * 80)
    print(f"合計: {total}個の特徴量提案")
    print(f"高インパクト: {len(high_impact)}個")
    print("=" * 80)
    
    print("\n【高インパクト特徴量 TOP推奨】")
    for f in high_impact:
        print(f"\n  ★ {f.name}")
        print(f"    説明: {f.description}")
        print(f"    市場盲点: {f.market_blind_spot.value}")
        print(f"    リーク防止: {f.leak_prevention[:50]}...")

if __name__ == "__main__":
    print_summary()
