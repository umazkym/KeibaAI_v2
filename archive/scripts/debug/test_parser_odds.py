import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src.modules.parsers.results_parser import parse_results_html

# サンプルHTMLファイルでテスト
sample_file = 'keibaai/data/raw/html/race/202001010101.bin'

print(f"パーサーテスト: {sample_file}")
print("="*60)

# パース実行
df = parse_results_html(sample_file, race_id='202001010101')

if df is not None and not df.empty:
    print(f"パース成功: {len(df)}行")
    
    # win_oddsカラムがあるか確認
    if 'win_odds' in df.columns:
        print(f"\nwin_oddsカラム: ✅ 存在")
        print(f"  非null数: {df['win_odds'].notna().sum()}/{len(df)}")
        print(f"  null数: {df['win_odds'].isna().sum()}/{len(df)}")
        
        # サンプル値
        print(f"\nサンプル値（最初の5行）:")
        print(df[['horse_name', 'finish_position', 'win_odds', 'popularity']].head())
        
        # 統計
        if df['win_odds'].notna().any():
            print(f"\nオッズ統計:")
            print(f"  最小値: {df['win_odds'].min()}")
            print(f"  最大値: {df['win_odds'].max()}")
            print(f"  平均値: {df['win_odds'].mean():.2f}")
        else:
            print("\n⚠️ 警告: 全てのwin_oddsがNullです")
    else:
        print("\n❌ エラー: win_oddsカラムが見つかりません")
        print(f"利用可能なカラム: {df.columns.tolist()}")
else:
    print("❌ パース失敗")
