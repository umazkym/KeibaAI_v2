"""
ダッシュボードビュー: システム概要とステータス表示（改善版）
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timedelta
import os

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))


def render_dashboard():
    """ダッシュボードのメイン表示"""

    # ヘッダー（大きく目立つように）
    st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1 style='font-size: 3rem; margin: 0;'>🐴 KeibaAI_v2</h1>
        <p style='font-size: 1.2rem; color: #666; margin-top: 0.5rem;'>競馬AI予測システム 統合ダッシュボード</p>
    </div>
    """, unsafe_allow_html=True)

    # データ分析
    data_stats = analyze_data_status()

    # 次にやるべきことを表示（最重要）
    show_next_steps(data_stats)

    st.markdown("---")

    # データ詳細統計
    show_detailed_data_stats(data_stats)

    st.markdown("---")

    # パイプライン状態
    show_pipeline_status(data_stats)

    st.markdown("---")

    # データ可視化
    show_data_visualization(data_stats)

    st.markdown("---")

    # クイックアクション
    show_quick_actions()


def analyze_data_status():
    """データの詳細な状態を分析"""

    stats = {
        "races": {"exists": False, "count": 0, "path": None, "date_range": None},
        "shutuba": {"exists": False, "count": 0, "path": None},
        "horses": {"exists": False, "count": 0, "path": None},
        "pedigrees": {"exists": False, "count": 0, "path": None},
        "features": {"exists": False, "count": 0, "path": None, "date_range": None},
        "models": {"exists": False, "count": 0, "paths": []},
        "simulations": {"exists": False, "count": 0, "latest_date": None},
        "raw_html": {"exists": False, "race_count": 0, "horse_count": 0},
    }

    # レースデータ
    races_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
    if races_parquet.exists():
        try:
            df = pd.read_parquet(races_parquet)
            stats["races"]["exists"] = True
            stats["races"]["count"] = len(df)
            stats["races"]["path"] = races_parquet
            if "race_date" in df.columns:
                stats["races"]["date_range"] = (df["race_date"].min(), df["race_date"].max())
        except:
            pass

    # 出馬表データ
    shutuba_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "shutuba" / "shutuba.parquet"
    if shutuba_parquet.exists():
        try:
            df = pd.read_parquet(shutuba_parquet)
            stats["shutuba"]["exists"] = True
            stats["shutuba"]["count"] = len(df)
            stats["shutuba"]["path"] = shutuba_parquet
        except:
            pass

    # 馬データ
    horses_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "horses" / "horses.parquet"
    if horses_parquet.exists():
        try:
            df = pd.read_parquet(horses_parquet)
            stats["horses"]["exists"] = True
            stats["horses"]["count"] = len(df)
            stats["horses"]["path"] = horses_parquet
        except:
            pass

    # 血統データ
    pedigrees_parquet = project_root / "keibaai" / "data" / "parsed" / "parquet" / "pedigrees" / "pedigrees.parquet"
    if pedigrees_parquet.exists():
        try:
            df = pd.read_parquet(pedigrees_parquet)
            stats["pedigrees"]["exists"] = True
            stats["pedigrees"]["count"] = len(df)
            stats["pedigrees"]["path"] = pedigrees_parquet
        except:
            pass

    # 特徴量データ
    features_dir = project_root / "keibaai" / "data" / "features" / "parquet"
    if features_dir.exists():
        try:
            # パーティションされたデータを検索
            parquet_files = list(features_dir.rglob("*.parquet"))
            if parquet_files:
                stats["features"]["exists"] = True
                stats["features"]["count"] = len(parquet_files)
                stats["features"]["path"] = features_dir
                # 日付範囲を推定（ディレクトリ名から）
                years = set()
                for f in parquet_files:
                    if "year=" in str(f):
                        year = str(f).split("year=")[1].split("/")[0]
                        years.add(year)
                if years:
                    stats["features"]["date_range"] = (min(years), max(years))
        except:
            pass

    # モデルデータ
    models_dir = project_root / "keibaai" / "data" / "models"
    if models_dir.exists():
        try:
            model_dirs = [d for d in models_dir.iterdir() if d.is_dir()]
            if model_dirs:
                stats["models"]["exists"] = True
                stats["models"]["count"] = len(model_dirs)
                stats["models"]["paths"] = model_dirs
        except:
            pass

    # シミュレーション結果
    sim_dir = project_root / "keibaai" / "data" / "simulations"
    if sim_dir.exists():
        try:
            sim_files = list(sim_dir.glob("*.json"))
            if sim_files:
                stats["simulations"]["exists"] = True
                stats["simulations"]["count"] = len(sim_files)
                # 最新の日付を取得
                latest_file = max(sim_files, key=lambda f: f.stem)
                stats["simulations"]["latest_date"] = latest_file.stem[:8]
        except:
            pass

    # Raw HTMLデータ
    raw_html_dir = project_root / "keibaai" / "data" / "raw" / "html"
    if raw_html_dir.exists():
        try:
            race_dir = raw_html_dir / "race"
            horse_dir = raw_html_dir / "horse"

            if race_dir.exists():
                race_files = list(race_dir.glob("*.bin")) + list(race_dir.glob("*.html"))
                stats["raw_html"]["race_count"] = len(race_files)
                if race_files:
                    stats["raw_html"]["exists"] = True

            if horse_dir.exists():
                horse_files = list(horse_dir.glob("*.bin")) + list(horse_dir.glob("*.html"))
                stats["raw_html"]["horse_count"] = len(horse_files)
                if horse_files:
                    stats["raw_html"]["exists"] = True
        except:
            pass

    return stats


def show_next_steps(stats):
    """次にやるべきことを明確に表示"""

    st.markdown("## 🎯 次にやること")

    # ステップの判定
    step = 0
    if not stats["raw_html"]["exists"] or stats["raw_html"]["race_count"] == 0:
        step = 1  # データ取得が必要
    elif not stats["races"]["exists"] or stats["races"]["count"] == 0:
        step = 2  # パース処理が必要
    elif not stats["features"]["exists"] or stats["features"]["count"] == 0:
        step = 3  # 特徴量生成が必要
    elif not stats["models"]["exists"] or stats["models"]["count"] == 0:
        step = 4  # モデル学習が必要
    else:
        step = 5  # 予測・シミュレーション実行

    # ステップごとの表示
    if step == 1:
        st.error("### ⚠️ データがありません - まずはデータを取得しましょう")
        st.markdown("""
        現在、競馬データがまだ取得されていません。まずはデータをスクレイピングする必要があります。

        **📌 やるべきこと:**
        1. 左サイドバーから「📊 データパイプライン」を選択
        2. 「データ取得（スクレイピング）」タブを開く
        3. 開始日と終了日を選択（まずは1週間分から試すのがおすすめ）
        4. 「🚀 スクレイピング開始」ボタンをクリック

        **⏱️ 所要時間:** 1週間分で約10-30分
        """)

        if st.button("📊 データパイプラインへ移動", type="primary", use_container_width=True):
            st.session_state.navigation = "📊 データパイプライン"
            st.rerun()

    elif step == 2:
        st.warning("### ⚡ データを処理しましょう")
        st.markdown(f"""
        データ取得は完了しています！（{stats['raw_html']['race_count']:,}件のレースHTML）

        次はこのデータを機械学習で使える形式（Parquet）に変換します。

        **📌 やるべきこと:**
        1. 左サイドバーから「📊 データパイプライン」を選択
        2. 「パース処理」タブを開く
        3. 「🚀 パース処理開始」ボタンをクリック

        **⏱️ 所要時間:** 約5-15分
        """)

        if st.button("📊 データパイプラインへ移動", type="primary", use_container_width=True):
            st.session_state.navigation = "📊 データパイプライン"
            st.rerun()

    elif step == 3:
        st.warning("### 🔧 特徴量を生成しましょう")
        st.markdown(f"""
        パース処理完了！（{stats['races']['count']:,}件のレースデータ）

        次は機械学習用の特徴量を生成します。

        **📌 やるべきこと:**
        1. 左サイドバーから「📊 データパイプライン」を選択
        2. 「特徴量生成」タブを開く
        3. 日付範囲を選択
        4. 「🚀 特徴量生成開始」ボタンをクリック

        **⏱️ 所要時間:** 約10-30分
        """)

        if st.button("📊 データパイプラインへ移動", type="primary", use_container_width=True):
            st.session_state.navigation = "📊 データパイプライン"
            st.rerun()

    elif step == 4:
        st.warning("### 🤖 AIモデルを学習しましょう")
        st.markdown(f"""
        特徴量生成完了！（{stats['features']['count']:,}個のファイル）

        次はAIモデルを学習します。これが予測の核心部分です。

        **📌 やるべきこと:**
        1. 左サイドバーから「🤖 モデル学習」を選択
        2. 「μモデル」タブを開く
        3. 学習期間を選択（データ全期間を推奨）
        4. 「🚀 μモデル学習開始」ボタンをクリック

        **⏱️ 所要時間:** 約1-3時間（データ量による）

        **💡 ヒント:** 学習中は他の作業ができます。ログで進捗を確認できます。
        """)

        if st.button("🤖 モデル学習へ移動", type="primary", use_container_width=True):
            st.session_state.navigation = "🤖 モデル学習"
            st.rerun()

    else:
        st.success("### ✅ 準備完了！予測を開始できます")
        st.markdown(f"""
        すべてのセットアップが完了しました！🎉

        **現在の状態:**
        - ✅ レースデータ: {stats['races']['count']:,}件
        - ✅ 特徴量: {stats['features']['count']:,}ファイル
        - ✅ 学習済みモデル: {stats['models']['count']}個

        **📌 次のステップ（お好きなものを）:**
        1. **シミュレーション実行** - レース結果をシミュレート
        2. **最適化実行** - 最適な賭け金を計算
        3. **結果分析** - モデルの精度を確認

        **💡 おすすめ:** まずはシミュレーションから始めましょう！
        """)

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🎲 シミュレーション", type="primary", use_container_width=True):
                st.session_state.navigation = "🎲 シミュレーション"
                st.rerun()
        with col2:
            if st.button("💰 最適化", type="primary", use_container_width=True):
                st.session_state.navigation = "💰 ポートフォリオ最適化"
                st.rerun()
        with col3:
            if st.button("📈 結果分析", type="primary", use_container_width=True):
                st.session_state.navigation = "📈 結果分析"
                st.rerun()


def show_detailed_data_stats(stats):
    """詳細なデータ統計を表示"""

    st.markdown("## 📊 データ詳細統計")

    # カードレイアウト
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if stats["races"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #1f77b4;">🏇 レースデータ</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['races']['count']:,}</p>
                <p style="color: #666; margin: 0;">件</p>
            </div>
            """, unsafe_allow_html=True)

            if stats["races"]["date_range"]:
                st.caption(f"📅 {stats['races']['date_range'][0]} ～ {stats['races']['date_range'][1]}")
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #fff3cd;">
                <h3 style="margin: 0;">🏇 レースデータ</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">❌ なし</p>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        if stats["horses"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #2ca02c;">🐴 馬データ</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['horses']['count']:,}</p>
                <p style="color: #666; margin: 0;">頭</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #fff3cd;">
                <h3 style="margin: 0;">🐴 馬データ</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">❌ なし</p>
            </div>
            """, unsafe_allow_html=True)

    with col3:
        if stats["pedigrees"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #d62728;">🧬 血統データ</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['pedigrees']['count']:,}</p>
                <p style="color: #666; margin: 0;">レコード (5世代)</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #fff3cd;">
                <h3 style="margin: 0;">🧬 血統データ</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">❌ なし</p>
            </div>
            """, unsafe_allow_html=True)

    with col4:
        if stats["features"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #ff7f0e;">🔬 特徴量</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['features']['count']:,}</p>
                <p style="color: #666; margin: 0;">ファイル</p>
            </div>
            """, unsafe_allow_html=True)

            if stats["features"]["date_range"]:
                st.caption(f"📅 {stats['features']['date_range'][0]}年 ～ {stats['features']['date_range'][1]}年")
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #fff3cd;">
                <h3 style="margin: 0;">🔬 特徴量</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">❌ なし</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 第2行
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if stats["models"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #9467bd;">🤖 モデル</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['models']['count']}</p>
                <p style="color: #666; margin: 0;">個</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #fff3cd;">
                <h3 style="margin: 0;">🤖 モデル</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">❌ なし</p>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        if stats["simulations"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #8c564b;">🎲 シミュレーション</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['simulations']['count']:,}</p>
                <p style="color: #666; margin: 0;">件</p>
            </div>
            """, unsafe_allow_html=True)

            if stats["simulations"]["latest_date"]:
                date_str = stats["simulations"]["latest_date"]
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                st.caption(f"📅 最新: {formatted_date}")
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #f0f2f6;">
                <h3 style="margin: 0;">🎲 シミュレーション</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">0</p>
                <p style="color: #666; margin: 0;">件</p>
            </div>
            """, unsafe_allow_html=True)

    with col3:
        if stats["raw_html"]["exists"]:
            st.markdown(f"""
            <div class="metric-card">
                <h3 style="margin: 0; color: #7f7f7f;">📁 Raw HTML</h3>
                <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{stats['raw_html']['race_count']:,}</p>
                <p style="color: #666; margin: 0;">ファイル</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="metric-card" style="background-color: #f0f2f6;">
                <h3 style="margin: 0;">📁 Raw HTML</h3>
                <p style="font-size: 1.5rem; margin: 0.5rem 0;">0</p>
            </div>
            """, unsafe_allow_html=True)

    with col4:
        # ストレージ使用量（概算）
        total_size = 0
        data_dir = project_root / "keibaai" / "data"
        if data_dir.exists():
            try:
                for root, dirs, files in os.walk(data_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        total_size += os.path.getsize(file_path)

                size_mb = total_size / (1024 * 1024)
                size_gb = size_mb / 1024

                if size_gb >= 1:
                    size_str = f"{size_gb:.2f} GB"
                else:
                    size_str = f"{size_mb:.1f} MB"

                st.markdown(f"""
                <div class="metric-card">
                    <h3 style="margin: 0; color: #bcbd22;">💾 ストレージ</h3>
                    <p style="font-size: 2rem; font-weight: bold; margin: 0.5rem 0;">{size_str}</p>
                    <p style="color: #666; margin: 0;">使用中</p>
                </div>
                """, unsafe_allow_html=True)
            except:
                st.markdown("""
                <div class="metric-card">
                    <h3 style="margin: 0;">💾 ストレージ</h3>
                    <p style="font-size: 1.5rem; margin: 0.5rem 0;">計算中...</p>
                </div>
                """, unsafe_allow_html=True)


def show_pipeline_status(stats):
    """パイプラインの状態を表示"""

    st.markdown("## 🔄 パイプライン実行状態")

    # パイプラインの各フェーズの状態を判定
    phases = []

    # Phase 1: データ取得
    if stats["raw_html"]["exists"] and stats["raw_html"]["race_count"] > 0:
        phases.append({
            "フェーズ": "1️⃣ データ取得",
            "ステータス": "✅ 完了",
            "詳細": f"{stats['raw_html']['race_count']:,}ファイル",
            "アクション": ""
        })
    else:
        phases.append({
            "フェーズ": "1️⃣ データ取得",
            "ステータス": "❌ 未実行",
            "詳細": "データなし",
            "アクション": "データパイプライン→スクレイピング"
        })

    # Phase 2: パース処理
    if stats["races"]["exists"] and stats["races"]["count"] > 0:
        phases.append({
            "フェーズ": "2️⃣ パース処理",
            "ステータス": "✅ 完了",
            "詳細": f"{stats['races']['count']:,}レース",
            "アクション": ""
        })
    else:
        phases.append({
            "フェーズ": "2️⃣ パース処理",
            "ステータス": "❌ 未実行" if stats["raw_html"]["exists"] else "⏸️ 待機中",
            "詳細": "データなし",
            "アクション": "データパイプライン→パース処理" if stats["raw_html"]["exists"] else "先にデータ取得が必要"
        })

    # Phase 3: 特徴量生成
    if stats["features"]["exists"] and stats["features"]["count"] > 0:
        phases.append({
            "フェーズ": "3️⃣ 特徴量生成",
            "ステータス": "✅ 完了",
            "詳細": f"{stats['features']['count']:,}ファイル",
            "アクション": ""
        })
    else:
        phases.append({
            "フェーズ": "3️⃣ 特徴量生成",
            "ステータス": "❌ 未実行" if stats["races"]["exists"] else "⏸️ 待機中",
            "詳細": "データなし",
            "アクション": "データパイプライン→特徴量生成" if stats["races"]["exists"] else "先にパース処理が必要"
        })

    # Phase 4: モデル学習
    if stats["models"]["exists"] and stats["models"]["count"] > 0:
        phases.append({
            "フェーズ": "4️⃣ モデル学習",
            "ステータス": "✅ 完了",
            "詳細": f"{stats['models']['count']}個のモデル",
            "アクション": ""
        })
    else:
        phases.append({
            "フェーズ": "4️⃣ モデル学習",
            "ステータス": "❌ 未実行" if stats["features"]["exists"] else "⏸️ 待機中",
            "詳細": "モデルなし",
            "アクション": "モデル学習→μモデル学習" if stats["features"]["exists"] else "先に特徴量生成が必要"
        })

    # Phase 5: シミュレーション
    if stats["simulations"]["exists"] and stats["simulations"]["count"] > 0:
        phases.append({
            "フェーズ": "5️⃣ シミュレーション",
            "ステータス": "✅ 実行済み",
            "詳細": f"{stats['simulations']['count']:,}件",
            "アクション": "シミュレーション→再実行可能"
        })
    else:
        phases.append({
            "フェーズ": "5️⃣ シミュレーション",
            "ステータス": "⏸️ 未実行" if stats["models"]["exists"] else "⏸️ 待機中",
            "詳細": "実行なし",
            "アクション": "シミュレーション→実行" if stats["models"]["exists"] else "先にモデル学習が必要"
        })

    df_pipeline = pd.DataFrame(phases)

    # スタイル付きで表示
    st.dataframe(
        df_pipeline,
        use_container_width=True,
        hide_index=True,
        column_config={
            "フェーズ": st.column_config.TextColumn("フェーズ", width="medium"),
            "ステータス": st.column_config.TextColumn("ステータス", width="small"),
            "詳細": st.column_config.TextColumn("詳細", width="medium"),
            "アクション": st.column_config.TextColumn("次のアクション", width="large"),
        }
    )


def show_data_visualization(stats):
    """データの可視化"""

    st.markdown("## 📈 データ可視化")

    if not stats["races"]["exists"] or stats["races"]["count"] == 0:
        st.info("📊 レースデータがまだありません。データ取得後にグラフが表示されます。")
        return

    try:
        import plotly.graph_objects as go
        import plotly.express as px

        # レースデータを読み込み
        df = pd.read_parquet(stats["races"]["path"])

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📅 月別レース数")
            if "race_date" in df.columns:
                df["race_date"] = pd.to_datetime(df["race_date"], errors='coerce')
                monthly_counts = df.groupby(df["race_date"].dt.to_period("M")).size().reset_index(name='count')
                monthly_counts["race_date"] = monthly_counts["race_date"].astype(str)

                fig = px.bar(
                    monthly_counts,
                    x="race_date",
                    y="count",
                    labels={"race_date": "月", "count": "レース数"},
                    color="count",
                    color_continuous_scale="Blues"
                )
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### 🏇 競馬場別レース数")
            if "venue" in df.columns:
                venue_counts = df["venue"].value_counts().head(10).reset_index()
                venue_counts.columns = ["venue", "count"]

                fig = px.pie(
                    venue_counts,
                    names="venue",
                    values="count",
                    hole=0.4
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.warning("📊 Plotlyがインストールされていません。グラフ表示には `pip install plotly` が必要です。")
    except Exception as e:
        st.error(f"グラフ表示エラー: {str(e)}")


def show_quick_actions():
    """クイックアクション"""

    st.markdown("## ⚡ クイックアクション")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background-color: #e3f2fd; border-radius: 10px;">
            <h3>📊</h3>
            <p>データパイプライン</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("実行する", key="quick_pipeline", use_container_width=True):
            st.session_state.navigation = "📊 データパイプライン"
            st.rerun()

    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background-color: #f3e5f5; border-radius: 10px;">
            <h3>🤖</h3>
            <p>モデル学習</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("実行する", key="quick_model", use_container_width=True):
            st.session_state.navigation = "🤖 モデル学習"
            st.rerun()

    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background-color: #fff3e0; border-radius: 10px;">
            <h3>🎲</h3>
            <p>シミュレーション</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("実行する", key="quick_sim", use_container_width=True):
            st.session_state.navigation = "🎲 シミュレーション"
            st.rerun()

    with col4:
        st.markdown("""
        <div style="text-align: center; padding: 1rem; background-color: #e8f5e9; border-radius: 10px;">
            <h3>📈</h3>
            <p>結果分析</p>
        </div>
        """, unsafe_allow_html=True)
        if st.button("実行する", key="quick_results", use_container_width=True):
            st.session_state.navigation = "📈 結果分析"
            st.rerun()
