"""
モデル学習ビュー: μ, σ, ν モデルの学習とモニタリング
"""

import streamlit as st
import subprocess
import sys
from pathlib import Path
from datetime import datetime, date
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_model_training():
    """モデル学習のメイン表示"""

    st.title("🤖 モデル学習")

    st.markdown("""
    KeibaAI_v2は3つのパラメータモデル（μ, σ, ν）を使用して競馬結果を予測します。
    - **μ (mu)**: 予想タイム（期待値）
    - **σ (sigma)**: 馬ごとの不確実性（分散）
    - **ν (nu)**: レースの混沌度（t分布の自由度）
    """)

    st.markdown("---")

    # タブで各モデルを分ける
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 μモデル (期待値)",
        "📊 σモデル (分散)",
        "🌀 νモデル (混沌度)",
        "📋 学習済みモデル"
    ])

    with tab1:
        render_mu_model_section()

    with tab2:
        render_sigma_model_section()

    with tab3:
        render_nu_model_section()

    with tab4:
        render_trained_models_section()


def render_mu_model_section():
    """μモデル学習セクション"""

    st.markdown("### 📈 μモデル (期待値モデル)")

    st.info("""
    **μモデル**は各馬の予想タイム（期待値）を予測します。
    LightGBMのランカー + 回帰モデルを使用します。
    """)

    # 学習データ期間
    col1, col2 = st.columns(2)

    with col1:
        mu_start_date = st.date_input(
            "学習開始日",
            value=date(2020, 1, 1),
            key="mu_start_date"
        )

    with col2:
        mu_end_date = st.date_input(
            "学習終了日",
            value=date.today(),
            key="mu_end_date"
        )

    # モデル出力先
    output_dir = st.text_input(
        "モデル保存先ディレクトリ",
        value=f"data/models/mu_model_{datetime.now().strftime('%Y%m%d')}",
        key="mu_output_dir"
    )

    # ハイパーパラメータ
    with st.expander("⚙️ ハイパーパラメータ設定"):
        col1, col2, col3 = st.columns(3)

        with col1:
            n_estimators = st.number_input(
                "n_estimators",
                min_value=100,
                max_value=5000,
                value=2000,
                step=100,
                help="ブースティングラウンド数"
            )

        with col2:
            learning_rate = st.number_input(
                "learning_rate",
                min_value=0.001,
                max_value=0.5,
                value=0.01,
                step=0.001,
                format="%.3f",
                help="学習率"
            )

        with col3:
            num_leaves = st.number_input(
                "num_leaves",
                min_value=7,
                max_value=255,
                value=31,
                step=1,
                help="葉の最大数"
            )

        col4, col5, col6 = st.columns(3)

        with col4:
            max_depth = st.number_input(
                "max_depth",
                min_value=-1,
                max_value=20,
                value=-1,
                step=1,
                help="木の最大深さ（-1=無制限）"
            )

        with col5:
            min_data_in_leaf = st.number_input(
                "min_data_in_leaf",
                min_value=1,
                max_value=100,
                value=20,
                step=1,
                help="葉の最小サンプル数"
            )

        with col6:
            feature_fraction = st.number_input(
                "feature_fraction",
                min_value=0.1,
                max_value=1.0,
                value=0.8,
                step=0.1,
                format="%.1f",
                help="各イテレーションでの特徴量サンプリング率"
            )

    # 実行ボタン
    if st.button("🚀 μモデル学習開始", type="primary", use_container_width=True):
        st.warning("μモデルの学習を開始します。この処理には時間がかかる場合があります。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # μモデル学習スクリプトの実行
        script_path = project_root / "keibaai" / "src" / "models" / "train_mu_model.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--start_date", mu_start_date.strftime("%Y-%m-%d"),
                "--end_date", mu_end_date.strftime("%Y-%m-%d"),
                "--output_dir", output_dir
            ]

            try:
                status_text.text("μモデル学習実行中...")
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
                    progress_bar.progress(min(len(logs) / 200, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ μモデル学習が正常に完了しました")
                    st.balloons()
                else:
                    status_text.error(f"❌ μモデル学習がエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")


def render_sigma_model_section():
    """σモデル学習セクション"""

    st.markdown("### 📊 σモデル (分散モデル)")

    st.info("""
    **σモデル**は各馬の予測の不確実性（分散）を推定します。
    これにより、信頼性の高い予測と低い予測を区別できます。
    """)

    st.warning("σモデルとνモデルは一括で学習されます。")

    # 学習データ期間
    col1, col2 = st.columns(2)

    with col1:
        sigma_start_date = st.date_input(
            "学習開始日",
            value=date(2020, 1, 1),
            key="sigma_start_date"
        )

    with col2:
        sigma_end_date = st.date_input(
            "学習終了日",
            value=date.today(),
            key="sigma_end_date"
        )

    # モデル出力先
    sigma_output_dir = st.text_input(
        "モデル保存先ディレクトリ",
        value=f"data/models/sigma_nu_model_{datetime.now().strftime('%Y%m%d')}",
        key="sigma_output_dir"
    )

    # 実行ボタン
    if st.button("🚀 σ・νモデル学習開始", type="primary", use_container_width=True, key="train_sigma_nu"):
        st.warning("σ・νモデルの学習を開始します。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # σ・νモデル学習スクリプトの実行
        script_path = project_root / "keibaai" / "src" / "models" / "train_sigma_nu_models.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--start_date", sigma_start_date.strftime("%Y-%m-%d"),
                "--end_date", sigma_end_date.strftime("%Y-%m-%d"),
                "--output_dir", sigma_output_dir
            ]

            try:
                status_text.text("σ・νモデル学習実行中...")
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
                    progress_bar.progress(min(len(logs) / 200, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ σ・νモデル学習が正常に完了しました")
                    st.balloons()
                else:
                    status_text.error(f"❌ σ・νモデル学習がエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")


def render_nu_model_section():
    """νモデル学習セクション"""

    st.markdown("### 🌀 νモデル (混沌度モデル)")

    st.info("""
    **νモデル**はレースの混沌度を推定します。
    t分布の自由度パラメータとして使用され、外れ値に対する頑健性を提供します。
    """)

    st.warning("νモデルはσモデルと一括で学習されます。上の「σモデル」タブから実行してください。")


def render_trained_models_section():
    """学習済みモデルセクション"""

    st.markdown("### 📋 学習済みモデル一覧")

    st.info("保存されている学習済みモデルの一覧です。")

    # モデルディレクトリを検索
    models_dir = project_root / "keibaai" / "data" / "models"

    if models_dir.exists():
        model_dirs = [d for d in models_dir.iterdir() if d.is_dir()]

        if model_dirs:
            models_info = []
            for model_dir in sorted(model_dirs, reverse=True):
                model_name = model_dir.name
                created_time = datetime.fromtimestamp(model_dir.stat().st_mtime)

                # モデルファイルの存在確認
                has_mu = (model_dir / "mu_model.txt").exists() or (model_dir / "lgbm_ranker.txt").exists()
                has_sigma = (model_dir / "sigma_model.txt").exists()
                has_nu = (model_dir / "nu_model.txt").exists()

                models_info.append({
                    "モデル名": model_name,
                    "作成日時": created_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "μモデル": "✅" if has_mu else "❌",
                    "σモデル": "✅" if has_sigma else "❌",
                    "νモデル": "✅" if has_nu else "❌",
                    "パス": str(model_dir.relative_to(project_root))
                })

            df_models = pd.DataFrame(models_info)
            st.dataframe(df_models, use_container_width=True, hide_index=True)

            # モデル削除機能
            st.markdown("---")
            st.markdown("#### 🗑️ モデル削除")
            model_to_delete = st.selectbox(
                "削除するモデルを選択",
                options=[m["モデル名"] for m in models_info],
                key="model_to_delete"
            )

            if st.button("🗑️ 選択したモデルを削除", type="secondary", key="delete_model"):
                st.warning(f"モデル '{model_to_delete}' の削除機能は現在実装中です。")

        else:
            st.warning("学習済みモデルが見つかりません。モデルを学習してください。")
    else:
        st.error(f"モデルディレクトリが見つかりません: {models_dir}")
