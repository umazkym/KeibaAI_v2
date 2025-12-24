"""
morning_odds/morning_popularity抽出のテストスクリプト
修正したshutuba_parser.pyが正しく動作するか確認する
"""

import sys
from pathlib import Path

# プロジェクトルートをPythonパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from keibaai.src.parsers.shutuba_parser import parse_shutuba_html

def test_shutuba_parser():
    """
    出馬表パーサーのテスト
    """
    print("=" * 80)
    print("🧪 shutuba_parser.py テスト実行")
    print("=" * 80)
    print()

    # テスト対象のHTMLファイルを探す
    html_dir = project_root / 'keibaai' / 'data' / 'raw' / 'html' / 'shutuba'

    if not html_dir.exists():
        print(f"❌ エラー: {html_dir} が存在しません")
        return

    # .binファイルを取得（最初の1件）
    bin_files = list(html_dir.glob('*.bin'))

    if not bin_files:
        print(f"❌ エラー: {html_dir} に.binファイルが見つかりません")
        return

    test_file = bin_files[0]
    print(f"📄 テストファイル: {test_file.name}")
    print()

    # パース実行
    try:
        df = parse_shutuba_html(str(test_file))

        if df.empty:
            print("❌ エラー: パース結果が空です")
            return

        print(f"✅ パース成功: {len(df)}行")
        print()

        # morning_oddsの欠損率を確認
        total_rows = len(df)
        morning_odds_missing = df['morning_odds'].isna().sum()
        morning_popularity_missing = df['morning_popularity'].isna().sum()

        morning_odds_rate = (morning_odds_missing / total_rows) * 100
        morning_popularity_rate = (morning_popularity_missing / total_rows) * 100

        print("📊 欠損率:")
        print(f"  - morning_odds:        {morning_odds_missing}/{total_rows} ({morning_odds_rate:.1f}%)")
        print(f"  - morning_popularity:  {morning_popularity_missing}/{total_rows} ({morning_popularity_rate:.1f}%)")
        print()

        # サンプルデータを表示
        print("🔍 サンプルデータ（先頭5行）:")
        print("-" * 80)
        sample_df = df[['horse_number', 'horse_name', 'morning_odds', 'morning_popularity']].head(5)
        print(sample_df.to_string(index=False))
        print()

        # 結果判定
        if morning_odds_rate < 50:
            print("✅ morning_odds抽出: 成功（欠損率 < 50%）")
        else:
            print(f"⚠️  morning_odds抽出: 失敗の可能性（欠損率 {morning_odds_rate:.1f}%）")

        if morning_popularity_rate < 50:
            print("✅ morning_popularity抽出: 成功（欠損率 < 50%）")
        else:
            print(f"⚠️  morning_popularity抽出: 失敗の可能性（欠損率 {morning_popularity_rate:.1f}%）")

        print()
        print("=" * 80)
        print("🎉 テスト完了")
        print("=" * 80)

    except Exception as e:
        print(f"❌ エラー発生: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_shutuba_parser()
