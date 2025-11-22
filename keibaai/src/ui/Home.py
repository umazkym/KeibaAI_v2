import streamlit as st
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
# 現在のファイル: keibaai/src/ui/Home.py
# 3つ上がプロジェクトルート
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

from components.data_loader import DataLoader

st.set_page_config(
    page_title="KeibaAI Model Validator",
    page_icon="🏇",
    layout="wide"
)

def main():
    st.title("🏇 KeibaAI Model Validator")
    st.markdown("""
    このダッシュボードは、AIモデルの挙動を検証し、改善点を発見するためのツールです。
    運用時の買い目決定ではなく、**「なぜその予測になったのか」**を理解することに主眼を置いています。
    """)
    
    # データローダー初期化
    loader = DataLoader()
    
    # サイドバー: モデル選択
    st.sidebar.header("モデル選択")
    available_models = loader.get_available_models()
    
    if not available_models:
        st.sidebar.error("モデルが見つかりません。")
        return

    # デフォルトで最新のモデルを選択
    selected_model = st.sidebar.selectbox(
        "分析対象モデル",
        available_models,
        index=0
    )
    
    # セッションステートに保存
    st.session_state['selected_model'] = selected_model
    st.session_state['loader'] = loader
    
    st.sidebar.info(f"選択中: **{selected_model}**")
    
    # メインコンテンツ
    st.header("機能一覧")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📊 モデル分析")
        st.write("AIが何を重視して予測しているかを確認します。")
        st.info("予測の決め手（重要度）を確認")
        
    with col2:
        st.subheader("🐎 レース詳細")
        st.write("個別のレース予測を詳しく見ます。")
        st.info("「なぜ？」ボタンで根拠を確認")
        
    with col3:
        st.subheader("📈 過去検証")
        st.write("過去のシミュレーション結果を振り返ります。")
        st.info("成功・失敗パターンを発見")

    st.markdown("---")
    st.caption("KeibaAI Project - Phase C: Strategy & UI")

if __name__ == "__main__":
    main()
