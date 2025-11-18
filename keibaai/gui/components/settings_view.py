"""
設定管理ビュー: YAMLファイルのGUI編集
"""

import streamlit as st
import sys
from pathlib import Path
import yaml

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_settings():
    """設定管理のメイン表示"""

    st.title("⚙️ 設定管理")

    st.markdown("""
    KeibaAI_v2の各種設定をGUIで編集できます。
    設定は`keibaai/configs/`ディレクトリのYAMLファイルに保存されます。
    """)

    st.markdown("---")

    # タブで各設定ファイルを分ける
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔧 基本設定",
        "🌐 スクレイピング設定",
        "🔬 特徴量設定",
        "🤖 モデル設定",
        "💰 最適化設定"
    ])

    with tab1:
        render_default_settings()

    with tab2:
        render_scraping_settings()

    with tab3:
        render_features_settings()

    with tab4:
        render_models_settings()

    with tab5:
        render_optimization_settings()


def render_default_settings():
    """基本設定セクション"""

    st.markdown("### 🔧 基本設定 (default.yaml)")

    config_path = project_root / "keibaai" / "configs" / "default.yaml"

    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # データパス設定
        st.markdown("#### データパス")
        data_path = st.text_input(
            "データルートパス",
            value=config.get('data_path', 'data'),
            key="default_data_path"
        )

        # ログ設定
        st.markdown("#### ログ設定")
        col1, col2 = st.columns(2)

        with col1:
            log_level = st.selectbox(
                "ログレベル",
                options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                index=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].index(
                    config.get('logging', {}).get('level', 'INFO')
                ),
                key="log_level"
            )

        with col2:
            log_format = st.text_input(
                "ログフォーマット",
                value=config.get('logging', {}).get('format', '%(asctime)s - %(levelname)s - %(message)s'),
                key="log_format"
            )

        # 保存ボタン
        if st.button("💾 設定を保存", key="save_default"):
            updated_config = config.copy()
            updated_config['data_path'] = data_path
            updated_config['logging']['level'] = log_level
            updated_config['logging']['format'] = log_format

            try:
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(updated_config, f, default_flow_style=False, allow_unicode=True)
                st.success("✅ 設定を保存しました")
            except Exception as e:
                st.error(f"❌ 保存エラー: {str(e)}")

        # 現在の設定を表示
        with st.expander("📄 現在の設定 (YAML)"):
            st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language='yaml')

    else:
        st.error(f"設定ファイルが見つかりません: {config_path}")


def render_scraping_settings():
    """スクレイピング設定セクション"""

    st.markdown("### 🌐 スクレイピング設定 (scraping.yaml)")

    config_path = project_root / "keibaai" / "configs" / "scraping.yaml"

    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # スクレイピング間隔
        st.markdown("#### リクエスト設定")
        col1, col2 = st.columns(2)

        with col1:
            sleep_min = st.number_input(
                "最小待機時間（秒）",
                min_value=1.0,
                max_value=10.0,
                value=float(config.get('sleep_interval', {}).get('min', 2.5)),
                step=0.5,
                key="sleep_min"
            )

        with col2:
            sleep_max = st.number_input(
                "最大待機時間（秒）",
                min_value=1.0,
                max_value=20.0,
                value=float(config.get('sleep_interval', {}).get('max', 5.0)),
                step=0.5,
                key="sleep_max"
            )

        # リトライ設定
        st.markdown("#### リトライ設定")
        col3, col4 = st.columns(2)

        with col3:
            max_retries = st.number_input(
                "最大リトライ回数",
                min_value=1,
                max_value=10,
                value=config.get('retry', {}).get('max_retries', 3),
                step=1,
                key="max_retries"
            )

        with col4:
            retry_delay = st.number_input(
                "リトライ待機時間（秒）",
                min_value=1,
                max_value=120,
                value=config.get('retry', {}).get('delay', 60),
                step=5,
                key="retry_delay"
            )

        # 保存ボタン
        if st.button("💾 設定を保存", key="save_scraping"):
            st.info("スクレイピング設定の保存機能は開発中です")

        # 現在の設定を表示
        with st.expander("📄 現在の設定 (YAML)"):
            st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language='yaml')

    else:
        st.error(f"設定ファイルが見つかりません: {config_path}")


def render_features_settings():
    """特徴量設定セクション"""

    st.markdown("### 🔬 特徴量設定 (features.yaml)")

    config_path = project_root / "keibaai" / "configs" / "features.yaml"

    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # 特徴量カテゴリの有効化/無効化
        st.markdown("#### 特徴量カテゴリ")

        feature_categories = {
            "race_features": "レース特徴量",
            "horse_features": "馬特徴量",
            "jockey_features": "騎手特徴量",
            "trainer_features": "調教師特徴量",
            "pedigree_features": "血統特徴量"
        }

        enabled_features = {}
        for key, label in feature_categories.items():
            enabled_features[key] = st.checkbox(
                label,
                value=config.get(key, {}).get('enabled', True),
                key=f"feature_{key}"
            )

        # パーティション設定
        st.markdown("#### データパーティション")
        partition_by = st.multiselect(
            "パーティション分割",
            options=["year", "month", "day"],
            default=config.get('output', {}).get('partition_by', ['year', 'month']),
            key="partition_by"
        )

        # 保存ボタン
        if st.button("💾 設定を保存", key="save_features"):
            st.info("特徴量設定の保存機能は開発中です")

        # 現在の設定を表示
        with st.expander("📄 現在の設定 (YAML)"):
            st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language='yaml')

    else:
        st.error(f"設定ファイルが見つかりません: {config_path}")


def render_models_settings():
    """モデル設定セクション"""

    st.markdown("### 🤖 モデル設定 (models.yaml)")

    config_path = project_root / "keibaai" / "configs" / "models.yaml"

    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # LightGBMハイパーパラメータ
        st.markdown("#### LightGBM ハイパーパラメータ")

        lgbm_config = config.get('models', {}).get('lgbm_ranker', {}).get('hyperparameters', {})

        col1, col2, col3 = st.columns(3)

        with col1:
            n_estimators = st.number_input(
                "n_estimators",
                min_value=100,
                max_value=5000,
                value=lgbm_config.get('n_estimators', 2000),
                step=100,
                key="model_n_estimators"
            )

        with col2:
            learning_rate = st.number_input(
                "learning_rate",
                min_value=0.001,
                max_value=0.5,
                value=lgbm_config.get('learning_rate', 0.01),
                step=0.001,
                format="%.3f",
                key="model_learning_rate"
            )

        with col3:
            num_leaves = st.number_input(
                "num_leaves",
                min_value=7,
                max_value=255,
                value=lgbm_config.get('num_leaves', 31),
                step=1,
                key="model_num_leaves"
            )

        # 保存ボタン
        if st.button("💾 設定を保存", key="save_models"):
            st.info("モデル設定の保存機能は開発中です")

        # 現在の設定を表示
        with st.expander("📄 現在の設定 (YAML)"):
            st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language='yaml')

    else:
        st.error(f"設定ファイルが見つかりません: {config_path}")


def render_optimization_settings():
    """最適化設定セクション"""

    st.markdown("### 💰 最適化設定 (optimization.yaml)")

    config_path = project_root / "keibaai" / "configs" / "optimization.yaml"

    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # ケリー設定
        st.markdown("#### ケリー基準設定")

        col1, col2 = st.columns(2)

        with col1:
            kelly_fraction = st.slider(
                "ケリー係数",
                min_value=0.1,
                max_value=1.0,
                value=float(config.get('kelly', {}).get('fraction', 0.25)),
                step=0.05,
                key="kelly_fraction"
            )

        with col2:
            max_bet_ratio = st.slider(
                "最大賭け金比率",
                min_value=0.01,
                max_value=0.5,
                value=float(config.get('kelly', {}).get('max_bet_ratio', 0.1)),
                step=0.01,
                key="max_bet_ratio"
            )

        # EV閾値
        st.markdown("#### フィルタリング設定")
        ev_threshold = st.number_input(
            "EV閾値",
            min_value=0.0,
            max_value=2.0,
            value=float(config.get('filters', {}).get('ev_threshold', 1.05)),
            step=0.05,
            key="ev_threshold"
        )

        # 保存ボタン
        if st.button("💾 設定を保存", key="save_optimization"):
            st.info("最適化設定の保存機能は開発中です")

        # 現在の設定を表示
        with st.expander("📄 現在の設定 (YAML)"):
            st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language='yaml')

    else:
        st.error(f"設定ファイルが見つかりません: {config_path}")
