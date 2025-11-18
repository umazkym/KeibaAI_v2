"""
シミュレーションビュー: モンテカルロシミュレーション実行
"""

import streamlit as st
import subprocess
import sys
from pathlib import Path
from datetime import datetime, date
import json

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_simulation():
    """シミュレーションのメイン表示"""

    st.title("🎲 モンテカルロシミュレーション")

    st.markdown("""
    学習済みモデル（μ, σ, ν）を使用して、レース結果のモンテカルロシミュレーションを実行します。
    各馬の勝率、連対率、複勝率を推定します。
    """)

    st.markdown("---")

    # シミュレーション設定
    st.markdown("### ⚙️ シミュレーション設定")

    col1, col2 = st.columns(2)

    with col1:
        sim_date = st.date_input(
            "シミュレーション対象日",
            value=date.today(),
            key="sim_date"
        )

    with col2:
        K = st.number_input(
            "シミュレーション回数 (K)",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="モンテカルロサンプリング回数。多いほど精度が上がりますが時間がかかります。"
        )

    # 詳細設定
    with st.expander("⚙️ 詳細設定"):
        use_t_distribution = st.checkbox(
            "t分布を使用",
            value=True,
            help="νパラメータを使ってt分布でサンプリング（推奨）"
        )

        random_seed = st.number_input(
            "乱数シード",
            min_value=-1,
            max_value=99999,
            value=42,
            step=1,
            help="再現性のための乱数シード（-1でランダム）"
        )

        save_samples = st.checkbox(
            "サンプルデータを保存",
            value=False,
            help="シミュレーション結果の詳細を保存（ファイルサイズ大）"
        )

    st.markdown("---")

    # 実行ボタン
    if st.button("🚀 シミュレーション開始", type="primary", use_container_width=True):
        st.warning(f"シミュレーションを開始します（{K}回試行）。この処理には時間がかかる場合があります。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # シミュレーションスクリプトの実行
        script_path = project_root / "keibaai" / "src" / "sim" / "simulate_daily_races.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--date", sim_date.strftime("%Y-%m-%d"),
                "--K", str(K)
            ]

            try:
                status_text.text("シミュレーション実行中...")
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
                    status_text.success("✅ シミュレーションが正常に完了しました")
                    st.balloons()

                    # 結果表示
                    show_simulation_results(sim_date)

                else:
                    status_text.error(f"❌ シミュレーションがエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")

    st.markdown("---")

    # シミュレーション結果の履歴
    st.markdown("### 📊 シミュレーション履歴")
    show_simulation_history()


def show_simulation_results(target_date):
    """シミュレーション結果を表示"""

    st.markdown("### 📈 シミュレーション結果")

    simulations_dir = project_root / "keibaai" / "data" / "simulations"
    date_str = target_date.strftime("%Y%m%d")

    if simulations_dir.exists():
        sim_files = list(simulations_dir.glob(f"{date_str}*.json"))

        if sim_files:
            st.success(f"{len(sim_files)}件のレースのシミュレーション結果が保存されました")

            # 最初のレース結果を表示
            with open(sim_files[0], 'r', encoding='utf-8') as f:
                result = json.load(f)

            st.markdown(f"#### サンプル: {sim_files[0].stem}")

            if 'probabilities' in result:
                st.json(result['probabilities'])

        else:
            st.warning("シミュレーション結果が見つかりません")
    else:
        st.error(f"シミュレーションディレクトリが見つかりません: {simulations_dir}")


def show_simulation_history():
    """シミュレーション履歴を表示"""

    simulations_dir = project_root / "keibaai" / "data" / "simulations"

    if simulations_dir.exists():
        sim_files = sorted(simulations_dir.glob("*.json"), reverse=True)[:20]

        if sim_files:
            history = []
            for sim_file in sim_files:
                race_id = sim_file.stem
                date_str = race_id[:8]
                created_time = datetime.fromtimestamp(sim_file.stat().st_mtime)

                history.append({
                    "レースID": race_id,
                    "日付": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                    "実行時刻": created_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "ファイルサイズ": f"{sim_file.stat().st_size / 1024:.1f} KB"
                })

            import pandas as pd
            df_history = pd.DataFrame(history)
            st.dataframe(df_history, use_container_width=True, hide_index=True)
        else:
            st.info("シミュレーション履歴がありません")
    else:
        st.warning(f"シミュレーションディレクトリが見つかりません: {simulations_dir}")
