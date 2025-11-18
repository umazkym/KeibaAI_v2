"""
ログビューア: リアルタイムログ表示とモニタリング
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_logs():
    """ログビューアのメイン表示"""

    st.title("📝 ログビューア")

    st.markdown("""
    KeibaAI_v2の各種ログをリアルタイムで確認できます。
    ログは`keibaai/data/logs/`ディレクトリに日付ごとに保存されます。
    """)

    st.markdown("---")

    # タブでログタイプを分ける
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 アプリケーションログ",
        "🌐 スクレイピングログ",
        "🤖 モデル学習ログ",
        "⚠️ エラーログ"
    ])

    with tab1:
        render_application_logs()

    with tab2:
        render_scraping_logs()

    with tab3:
        render_training_logs()

    with tab4:
        render_error_logs()


def render_application_logs():
    """アプリケーションログセクション"""

    st.markdown("### 📊 アプリケーションログ")

    # 日付選択
    log_date = st.date_input(
        "ログ日付",
        value=date.today(),
        key="app_log_date"
    )

    # ログレベルフィルタ
    log_levels = st.multiselect(
        "ログレベル",
        options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=["INFO", "WARNING", "ERROR", "CRITICAL"],
        key="app_log_levels"
    )

    # 検索キーワード
    search_keyword = st.text_input(
        "検索キーワード",
        placeholder="ログ内を検索...",
        key="app_log_search"
    )

    # ログファイル読み込み
    logs_dir = project_root / "keibaai" / "data" / "logs"
    log_file = logs_dir / f"{log_date.strftime('%Y%m%d')}_app.log"

    if log_file.exists():
        with st.spinner("ログを読み込み中..."):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_lines = f.readlines()

                # フィルタリング
                filtered_lines = []
                for line in log_lines:
                    # レベルフィルタ
                    if any(level in line for level in log_levels):
                        # キーワード検索
                        if not search_keyword or search_keyword.lower() in line.lower():
                            filtered_lines.append(line.strip())

                st.info(f"📄 {len(filtered_lines)}行 / 全{len(log_lines)}行")

                # ログ表示
                log_text = "\n".join(filtered_lines[-500:])  # 最新500行
                st.text_area(
                    "ログ内容",
                    value=log_text,
                    height=500,
                    key="app_log_content"
                )

                # ダウンロードボタン
                st.download_button(
                    label="📥 ログをダウンロード",
                    data=log_text,
                    file_name=f"{log_date}_app.log",
                    mime="text/plain"
                )

            except Exception as e:
                st.error(f"❌ ログ読み込みエラー: {str(e)}")

    else:
        st.warning(f"ログファイルが見つかりません: {log_file}")

        # 利用可能なログファイルを表示
        if logs_dir.exists():
            available_logs = sorted(logs_dir.glob("*_app.log"), reverse=True)[:10]
            if available_logs:
                st.markdown("#### 利用可能なログファイル")
                for log in available_logs:
                    st.text(f"- {log.name}")


def render_scraping_logs():
    """スクレイピングログセクション"""

    st.markdown("### 🌐 スクレイピングログ")

    st.info("スクレイピング専用のログを表示します。")

    # 日付選択
    scrape_log_date = st.date_input(
        "ログ日付",
        value=date.today(),
        key="scrape_log_date"
    )

    # ログファイル読み込み
    logs_dir = project_root / "keibaai" / "data" / "logs"
    log_file = logs_dir / f"{scrape_log_date.strftime('%Y%m%d')}_scraping.log"

    if log_file.exists():
        with st.spinner("ログを読み込み中..."):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_lines = f.readlines()

                st.info(f"📄 全{len(log_lines)}行")

                # ログ統計
                col1, col2, col3 = st.columns(3)

                with col1:
                    success_count = sum(1 for line in log_lines if "成功" in line or "SUCCESS" in line)
                    st.metric("成功", success_count)

                with col2:
                    error_count = sum(1 for line in log_lines if "ERROR" in line or "エラー" in line)
                    st.metric("エラー", error_count)

                with col3:
                    warning_count = sum(1 for line in log_lines if "WARNING" in line or "警告" in line)
                    st.metric("警告", warning_count)

                # ログ表示
                log_text = "\n".join([line.strip() for line in log_lines[-500:]])
                st.text_area(
                    "ログ内容",
                    value=log_text,
                    height=500,
                    key="scrape_log_content"
                )

            except Exception as e:
                st.error(f"❌ ログ読み込みエラー: {str(e)}")

    else:
        st.warning(f"スクレイピングログが見つかりません: {log_file}")


def render_training_logs():
    """モデル学習ログセクション"""

    st.markdown("### 🤖 モデル学習ログ")

    st.info("モデル学習の進捗とメトリクスを表示します。")

    # ログファイル読み込み
    logs_dir = project_root / "keibaai" / "data" / "logs"
    log_files = sorted(logs_dir.glob("*_training.log"), reverse=True)

    if log_files:
        # 最新のログファイルを選択
        selected_log = st.selectbox(
            "学習ログを選択",
            options=[log.name for log in log_files],
            key="training_log_select"
        )

        log_file = logs_dir / selected_log

        with st.spinner("ログを読み込み中..."):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_lines = f.readlines()

                st.info(f"📄 全{len(log_lines)}行")

                # 学習メトリクスの抽出（サンプル）
                st.markdown("#### 学習メトリクス")

                # プレースホルダー
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("エポック数", "2000")

                with col2:
                    st.metric("最終Loss", "0.187")

                with col3:
                    st.metric("検証精度", "78.2%")

                # ログ表示
                log_text = "\n".join([line.strip() for line in log_lines[-500:]])
                st.text_area(
                    "ログ内容",
                    value=log_text,
                    height=500,
                    key="training_log_content"
                )

            except Exception as e:
                st.error(f"❌ ログ読み込みエラー: {str(e)}")

    else:
        st.warning("学習ログが見つかりません")


def render_error_logs():
    """エラーログセクション"""

    st.markdown("### ⚠️ エラーログ")

    st.info("すべてのエラーと警告をまとめて表示します。")

    # 期間選択
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "開始日",
            value=date.today() - timedelta(days=7),
            key="error_start_date"
        )

    with col2:
        end_date = st.date_input(
            "終了日",
            value=date.today(),
            key="error_end_date"
        )

    # エラーログの収集
    logs_dir = project_root / "keibaai" / "data" / "logs"

    if logs_dir.exists():
        error_entries = []

        # 期間内のログファイルを検索
        current_date = start_date
        while current_date <= end_date:
            log_files = list(logs_dir.glob(f"{current_date.strftime('%Y%m%d')}*.log"))

            for log_file in log_files:
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        for i, line in enumerate(f):
                            if "ERROR" in line or "CRITICAL" in line or "エラー" in line:
                                error_entries.append({
                                    "日時": current_date.strftime("%Y-%m-%d"),
                                    "ファイル": log_file.name,
                                    "行番号": i + 1,
                                    "内容": line.strip()[:100]  # 最初の100文字
                                })
                except:
                    continue

            current_date += timedelta(days=1)

        if error_entries:
            st.error(f"⚠️ {len(error_entries)}件のエラーが見つかりました")

            df_errors = pd.DataFrame(error_entries)
            st.dataframe(df_errors, use_container_width=True, hide_index=True)

            # エラーの詳細を表示
            if st.checkbox("エラーの詳細を表示"):
                for entry in error_entries[-20:]:  # 最新20件
                    with st.expander(f"{entry['日時']} - {entry['ファイル']}:{entry['行番号']}"):
                        st.code(entry['内容'])

        else:
            st.success("✅ エラーはありません")

    else:
        st.warning(f"ログディレクトリが見つかりません: {logs_dir}")
