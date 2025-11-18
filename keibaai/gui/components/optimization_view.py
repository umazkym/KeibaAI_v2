"""
ポートフォリオ最適化ビュー: ケリー基準による賭け金最適化
"""

import streamlit as st
import subprocess
import sys
from pathlib import Path
from datetime import datetime, date
import json
import pandas as pd

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_optimization():
    """最適化のメイン表示"""

    st.title("💰 ポートフォリオ最適化")

    st.markdown("""
    フラクショナルケリー基準を使用して、最適な賭け金配分を計算します。
    シミュレーション結果とオッズデータを基に、期待リターンを最大化します。
    """)

    st.markdown("---")

    # 最適化設定
    st.markdown("### ⚙️ 最適化設定")

    col1, col2 = st.columns(2)

    with col1:
        opt_date = st.date_input(
            "最適化対象日",
            value=date.today(),
            key="opt_date"
        )

    with col2:
        W_0 = st.number_input(
            "初期資金 (円)",
            min_value=1000,
            max_value=10000000,
            value=100000,
            step=10000,
            help="運用する総資金額"
        )

    # 詳細設定
    with st.expander("⚙️ 詳細設定"):
        kelly_fraction = st.slider(
            "ケリー係数",
            min_value=0.1,
            max_value=1.0,
            value=0.25,
            step=0.05,
            help="フルケリーに対する割合（0.25 = クォーターケリー、推奨）"
        )

        ev_threshold = st.number_input(
            "EV閾値",
            min_value=0.0,
            max_value=2.0,
            value=1.05,
            step=0.05,
            help="期待値がこの値を下回る馬は除外"
        )

        max_bet_per_race = st.number_input(
            "1レースあたりの最大賭け金 (円)",
            min_value=100,
            max_value=100000,
            value=10000,
            step=1000,
            help="1レースに賭ける上限額"
        )

        min_bet = st.number_input(
            "最小賭け金 (円)",
            min_value=100,
            max_value=10000,
            value=1000,
            step=100,
            help="この金額未満の賭けは除外"
        )

    st.markdown("---")

    # 実行ボタン
    if st.button("🚀 最適化実行", type="primary", use_container_width=True):
        st.warning("ポートフォリオ最適化を開始します。")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.expander("📝 実行ログ", expanded=True)

        with log_container:
            log_area = st.empty()

        # 最適化スクリプトの実行
        script_path = project_root / "keibaai" / "src" / "optimizer" / "optimize_daily_races.py"

        if script_path.exists():
            cmd = [
                sys.executable,
                str(script_path),
                "--date", opt_date.strftime("%Y-%m-%d"),
                "--W_0", str(W_0)
            ]

            try:
                status_text.text("最適化実行中...")
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
                    progress_bar.progress(min(len(logs) / 50, 1.0))

                process.wait()

                if process.returncode == 0:
                    status_text.success("✅ 最適化が正常に完了しました")
                    st.balloons()

                    # 結果表示
                    show_optimization_results(opt_date)

                else:
                    status_text.error(f"❌ 最適化がエラーで終了しました (code: {process.returncode})")

            except Exception as e:
                status_text.error(f"❌ エラー: {str(e)}")
        else:
            st.error(f"スクリプトが見つかりません: {script_path}")

    st.markdown("---")

    # 最適化結果の履歴
    st.markdown("### 📊 最適化履歴")
    show_optimization_history()


def show_optimization_results(target_date):
    """最適化結果を表示"""

    st.markdown("### 📈 最適化結果")

    orders_dir = project_root / "keibaai" / "data" / "orders"
    date_str = target_date.strftime("%Y%m%d")

    if orders_dir.exists():
        order_files = list(orders_dir.glob(f"{date_str}*_order.json"))

        if order_files:
            st.success(f"{len(order_files)}件のレースの最適化が完了しました")

            total_bets = 0
            all_orders = []

            for order_file in order_files:
                with open(order_file, 'r', encoding='utf-8') as f:
                    order = json.load(f)

                if 'bets' in order:
                    for bet in order['bets']:
                        total_bets += bet.get('amount', 0)
                        all_orders.append({
                            "レースID": order_file.stem.replace('_order', ''),
                            "馬番": bet.get('horse_number', 'N/A'),
                            "賭け金": f"{bet.get('amount', 0):,.0f}円",
                            "オッズ": bet.get('odds', 'N/A'),
                            "期待値": bet.get('ev', 'N/A')
                        })

            if all_orders:
                df_orders = pd.DataFrame(all_orders)
                st.dataframe(df_orders, use_container_width=True, hide_index=True)

                st.metric("総賭け金", f"{total_bets:,.0f}円")

        else:
            st.warning("最適化結果が見つかりません")
    else:
        st.error(f"オーダーディレクトリが見つかりません: {orders_dir}")


def show_optimization_history():
    """最適化履歴を表示"""

    orders_dir = project_root / "keibaai" / "data" / "orders"

    if orders_dir.exists():
        order_files = sorted(orders_dir.glob("*_order.json"), reverse=True)[:20]

        if order_files:
            history = []
            for order_file in order_files:
                race_id = order_file.stem.replace('_order', '')
                date_str = race_id[:8]
                created_time = datetime.fromtimestamp(order_file.stat().st_mtime)

                # オーダーファイルを読んで賭け金を集計
                try:
                    with open(order_file, 'r', encoding='utf-8') as f:
                        order = json.load(f)
                        total_amount = sum(bet.get('amount', 0) for bet in order.get('bets', []))
                except:
                    total_amount = 0

                history.append({
                    "レースID": race_id,
                    "日付": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                    "実行時刻": created_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "総賭け金": f"{total_amount:,.0f}円"
                })

            df_history = pd.DataFrame(history)
            st.dataframe(df_history, use_container_width=True, hide_index=True)
        else:
            st.info("最適化履歴がありません")
    else:
        st.warning(f"オーダーディレクトリが見つかりません: {orders_dir}")
