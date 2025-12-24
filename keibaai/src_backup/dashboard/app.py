import streamlit as st
from pathlib import Path
import sys
import yaml

# プロジェクトルートをパスに追加
# app.py -> dashboard -> src -> keibaai -> Keiba_AI_v2 (4階層上)
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

st.set_page_config(
    page_title="KeibaAI ダッシュボード",
    page_icon="🐎",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🐎 KeibaAI ダッシュボード")

st.markdown("""
### KeibaAI ダッシュボードへようこそ

この画面では、AIが「どれくらい賢いか」「どんな予想をしているか」「儲かっているか」を確認できます。

#### 👈 左のメニューから見たい項目を選んでください

- **📊 AIの成績表 (Evaluation Report)**: 
    - AIの予想がどれくらい当たっているかを確認します。
    - 「タイムの誤差」や「順位予想の正確さ」を見ることができます。
- **🎲 レース予想とシミュレーション (Simulation Visualizer)**: 
    - 特定のレースで、AIがどの馬を「勝つ」と予想しているかを見ます。
    - 「このオッズなら買うべきか？」を判断するのに役立ちます。
- **🧬 予想の根拠 (Feature Analysis)**: 
    - AIが予想するときに「何を重視しているか」を解明します。
    - 「騎手の勝率」や「過去のタイム」など、どのデータが重要か分かります。
- **💰 収支分析 (ROI Analysis)**: 
    - AIの予想通りに馬券を買った場合、資金がどう増減するかを確認します。
    - 最終的にプラスになったか、途中でどれくらい減ったかが見れます。
""")

# 設定ファイルの読み込み確認
try:
    config_path = project_root / "keibaai" / "configs" / "default.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    st.success(f"✅ 設定ファイルを読み込みました: {config_path}")
except Exception as e:
    st.error(f"❌ 設定ファイルの読み込みに失敗しました: {e}")

# 最新のログを表示（オプション）
st.subheader("📝 最新のシステムログ")
try:
    log_dir = project_root / "keibaai" / "data" / "logs"
    # 最新のログファイルを探す (YYYY/MM/DD/*.log)
    log_files = sorted(log_dir.glob("**/*.log"), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if log_files:
        latest_log = log_files[0]
        st.info(f"最新ログ: {latest_log.relative_to(project_root)}")
        with open(latest_log, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            st.code("".join(lines[-20:]), language="text") # 最後の20行を表示
    else:
        st.warning("ログファイルが見つかりません。")
except Exception as e:
    st.error(f"ログの読み込みに失敗しました: {e}")
