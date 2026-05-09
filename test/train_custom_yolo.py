import os
from ultralytics import YOLO

def train_custom_model(data_yaml_path: str):
    """
    Roboflowからエクスポートしたデータセットを使用してYOLOv8をファインチューニングします。
    """
    print(f"[{data_yaml_path}] を使用して学習を開始します...")
    
    # 既存の重みから学習開始（高精度な yolov8x をベースにする）
    model = YOLO("yolov8x.pt")
    
    # 学習の実行
    model.train(
        data=data_yaml_path,  # エクスポートしたデータセットのパス
        epochs=50,            # 50エポック（まずはこの程度から）
        imgsz=1280,           # 高解像度での学習
        batch=4,              # CPUの場合は2〜4。メモリ不足エラーが出る場合は 2 に下げる
        patience=15,          # 15エポック連続で精度改善がなければ早期終了
        device="cpu",         # GPUがある場合は "cuda" に変更してください
        project="keiba_model",
        name="horse_detector_v1",
    )
    print("\n学習が完了しました！")
    print("生成されたカスタムモデルは 'keiba_model/horse_detector_v1/weights/best.pt' に保存されています。")

if __name__ == "__main__":
    # ==========================================================
    # 手順:
    # 1. Roboflow でアノテーション（枠囲み）を行い、データセットを作成
    # 2. YOLOv8 形式で zip エクスポートし、手元にダウンロード
    # 3. zip を解凍し、そのフォルダ名（または data.yaml のパス）を以下に指定
    # ==========================================================
    
    # 例: 解凍したフォルダが "dataset" という名前の場合
    dataset_yaml = "dataset/data.yaml"
    
    if not os.path.exists(dataset_yaml):
        print(f"エラー: データセットが見つかりません: {dataset_yaml}")
        print("Roboflowからダウンロードしたデータを配置し、パスを書き換えてください。")
    else:
        train_custom_model(dataset_yaml)
