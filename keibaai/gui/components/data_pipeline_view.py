"""
データパイプラインビュー: スクレイピング、パース、特徴量生成の実行
"""

import streamlit as st
import subprocess
import sys
from pathlib import Path
from datetime import datetime, date
import threading
import queue

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_data_pipeline():
    """データパイプラインのメイン表示"""

    st.title("📊 データパイプライン")

    st.markdown("""
    このページでは、KeibaAI_v2のデータパイプラインを実行できます。
    各フェーズを個別に実行することも、一括で実行することもできます。
    """)

    st.markdown("---")

    # タブで各フェーズを分ける
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌐 データ取得 (スクレイピング)",
        "📝 パース処理",
        "🔧 特徴量生成",
        "🚀 一括実行"
    ])

    with tab1:
        render_scraping_section()

    with tab2:
        render_parsing_section()

    with tab3:
        render_feature_generation_section()

    with tab4:
        render_batch_execution_section()


def render_scraping_section():
    """スクレイピングセクション"""

    st.markdown("### 🌐 データ取得 (スクレイピング)")

    st.info("""
    **注意**: スクレイピングは対象サーバーに負荷をかけます。
    適切な間隔（2.5-5秒）で実行され、HTTP 400エラー時は60秒待機します。
    """)

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "開始日",
            value=date.today().replace(month=1, day=1),
            key="scrape_start_date"
        )

    with col2:
        end_date = st.date_input(
            "終了日",
            value=date.today(),
            key="scrape_end_date"
        )

    # スクレイピングオプション
    with st.expander("⚙️ 詳細オプション"):
        scrape_races = st.checkbox("レース結果", value=True)
        scrape_shutuba = st.checkbox("出馬表", value=True)
        scrape_horses = st.checkbox("馬プロフィール", value=True)
        scrape_pedigrees = st.checkbox("血統データ", value=True)

        sleep_interval = st.slider(
            "リクエスト間隔（秒）",
            min_value=2.0,
            max_value=10.0,
            value=3.0,
            step=0.5,
            help="各リクエスト間の待機時間"
        )

    # 実行ボタン
    if st.button("🚀 スクレイピング開始", type="primary", use_container_width=True):
        st.warning("スクレイピングを開始します。この処理には時間がかかる場合があります。")

        # プログレスバーとログ表示
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # スクレイピングスクリプトの実行
        script_path = project_root / "keibaai" / "src" / "run_scraping_pipeline_with_args.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--start_date", start_date.strftime("%Y-%m-%d"),
                "--end_date", end_date.strftime("%Y-%m-%d")
            ]

            try:
                status_text.text("スクレイピング実行中...")
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    cwd=str(project_root)
                )

                logs = []
                for line in process.stdout:
                    logs.append(line.strip())
                    log_area.text("\n".join(logs[-50:]))  # 最新50行を表示
                    progress_bar.progress(min(len(logs) / 100, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ スクレイピングが正常に完了しました")
                    st.balloons()
                else:
                    status_text.error(f"❌ スクレイピングがエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")


def render_parsing_section():
    """パース処理セクション"""

    st.markdown("### 📝 パース処理")

    st.info("""
    スクレイピングで取得したHTMLデータをParquet形式にパースします。
    - レース結果 → `races.parquet`
    - 出馬表 → `shutuba.parquet`
    - 馬プロフィール → `horses.parquet`
    - 血統データ → `pedigrees.parquet`
    """)

    # パースオプション
    with st.expander("⚙️ パースオプション"):
        parse_races = st.checkbox("レース結果をパース", value=True, key="parse_races")
        parse_shutuba = st.checkbox("出馬表をパース", value=True, key="parse_shutuba")
        parse_horses = st.checkbox("馬データをパース", value=True, key="parse_horses")
        parse_pedigrees = st.checkbox("血統データをパース", value=True, key="parse_pedigrees")

    # 実行ボタン
    if st.button("🚀 パース処理開始", type="primary", use_container_width=True, key="run_parsing"):
        st.warning("パース処理を開始します。この処理には時間がかかる場合があります。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # パースシングスクリプトの実行
        script_path = project_root / "keibaai" / "src" / "run_parsing_pipeline_local.py"

        if script_path.exists():
            cmd = [sys.executable, str(script_path)]

            try:
                status_text.text("パース処理実行中...")
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    cwd=str(project_root)
                )

                logs = []
                for line in process.stdout:
                    logs.append(line.strip())
                    log_area.text("\n".join(logs[-50:]))
                    progress_bar.progress(min(len(logs) / 100, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ パース処理が正常に完了しました")
                    st.balloons()
                else:
                    status_text.error(f"❌ パース処理がエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")


def render_feature_generation_section():
    """特徴量生成セクション"""

    st.markdown("### 🔧 特徴量生成")

    st.info("""
    パースされたデータから機械学習用の特徴量を生成します。
    生成される特徴量は27+種類で、年月ごとにパーティション分割されます。
    """)

    col1, col2 = st.columns(2)

    with col1:
        feature_start_date = st.date_input(
            "開始日",
            value=date.today().replace(month=1, day=1),
            key="feature_start_date"
        )

    with col2:
        feature_end_date = st.date_input(
            "終了日",
            value=date.today(),
            key="feature_end_date"
        )

    # 特徴量オプション
    with st.expander("⚙️ 特徴量オプション"):
        st.markdown("**有効化する特徴量カテゴリ:**")
        enable_race_features = st.checkbox("レース特徴量", value=True, help="距離、馬場、天候など")
        enable_horse_features = st.checkbox("馬特徴量", value=True, help="年齢、性別、体重など")
        enable_jockey_features = st.checkbox("騎手特徴量", value=True, help="勝率、連対率など")
        enable_trainer_features = st.checkbox("調教師特徴量", value=True, help="厩舎成績など")
        enable_pedigree_features = st.checkbox("血統特徴量", value=True, help="父馬、母馬など")

    # 実行ボタン
    if st.button("🚀 特徴量生成開始", type="primary", use_container_width=True, key="run_features"):
        st.warning("特徴量生成を開始します。この処理には時間がかかる場合があります。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # 特徴量生成スクリプトの実行
        script_path = project_root / "keibaai" / "src" / "features" / "generate_features.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--start_date", feature_start_date.strftime("%Y-%m-%d"),
                "--end_date", feature_end_date.strftime("%Y-%m-%d")
            ]

            try:
                status_text.text("特徴量生成実行中...")
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    cwd=str(project_root)
                )

                logs = []
                for line in process.stdout:
                    logs.append(line.strip())
                    log_area.text("\n".join(logs[-50:]))
                    progress_bar.progress(min(len(logs) / 100, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ 特徴量生成が正常に完了しました")
                    st.balloons()
                else:
                    status_text.error(f"❌ 特徴量生成がエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")


def render_batch_execution_section():
    """一括実行セクション"""

    st.markdown("### 🚀 一括実行")

    st.info("""
    データパイプライン全体を一括で実行します。
    以下の順序で処理されます:
    1. スクレイピング
    2. パース処理
    3. 特徴量生成
    """)

    st.warning("⚠️ 一括実行は非常に時間がかかります（数時間～1日以上）")

    col1, col2 = st.columns(2)

    with col1:
        batch_start_date = st.date_input(
            "開始日",
            value=date.today().replace(month=1, day=1),
            key="batch_start_date"
        )

    with col2:
        batch_end_date = st.date_input(
            "終了日",
            value=date.today(),
            key="batch_end_date"
        )

    # 確認チェックボックス
    confirm = st.checkbox("上記の設定で一括実行することを確認しました", key="batch_confirm")

    # 実行ボタン
    if st.button(
        "🚀 一括実行開始",
        type="primary",
        use_container_width=True,
        disabled=not confirm,
        key="run_batch"
    ):
        st.error("一括実行機能は現在実装中です。各フェーズを個別に実行してください。")
