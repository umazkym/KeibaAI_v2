import streamlit as st
import plotly.express as px
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
# 現在のファイル: keibaai/src/ui/pages/X_Page.py
# 4つ上がプロジェクトルート
project_root = Path(__file__).resolve().parents[4]
sys.path.append(str(project_root))

def main():
    st.title("📊 モデル分析 (Model Analysis)")
    
    if 'loader' not in st.session_state or 'selected_model' not in st.session_state:
        st.warning("Homeページからモデルを選択してください。")
        return

    loader = st.session_state['loader']
    selected_model = st.session_state['selected_model']
    
    st.write(f"分析対象: **{selected_model}**")
    
    # 特徴量重要度
    st.header("予測の決め手（Feature Importance）")
    
    with st.spinner("データを読み込んでいます..."):
        df_importance = loader.load_feature_importance(selected_model)
        
    if df_importance.empty:
        st.error("特徴量重要度の読み込みに失敗しました。")
        return
        
    # タブで表示切り替え
    tab1, tab2 = st.tabs(["精度の向上度 (Gain)", "条件の分岐回数 (Split)"])
    
    with tab1:
        st.subheader("精度の向上度 (Gain)")
        st.caption("この要素がどれだけ予測の正確さに貢献したかを表します。数値が高いほど重要です。")
        
        # 上位20個を表示
        top_n = st.slider("表示件数", 10, 50, 20, key='gain_slider')
        df_plot = df_importance.sort_values('gain', ascending=False).head(top_n)
        
        fig = px.bar(
            df_plot,
            x='gain',
            y='feature',
            orientation='h',
            title=f"予測への貢献度トップ {top_n}",
            height=600,
            labels={'gain': '貢献度', 'feature': '特徴量'}
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
    with tab2:
        st.subheader("条件の分岐回数 (Split)")
        st.caption("AIが判断を下す際に、この要素を何回チェックしたかを表します。")
        
        top_n_split = st.slider("表示件数", 10, 50, 20, key='split_slider')
        df_plot_split = df_importance.sort_values('split', ascending=False).head(top_n_split)
        
        fig_split = px.bar(
            df_plot_split,
            x='split',
            y='feature',
            orientation='h',
            title=f"判断に使われた回数トップ {top_n_split}",
            height=600,
            labels={'split': '回数', 'feature': '特徴量'}
        )
        fig_split.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_split, use_container_width=True)

    # 生データ表示
    with st.expander("詳細データを見る"):
        st.dataframe(df_importance)

if __name__ == "__main__":
    main()
