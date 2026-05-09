import os
from pathlib import Path
from ultralytics import YOLO
import cv2
import numpy as np

try:
    from sahi import AutoDetectionModel
    from sahi.predict import get_sliced_prediction
except ImportError:
    pass

try:
    from sklearn.cluster import DBSCAN
except ImportError:
    pass


def remove_duplicate_detections(horses_data, distance_threshold=40):
    """
    近すぎる検出（同一馬の重複）を信頼度の高い方だけ残す
    """
    if not horses_data:
        return horses_data
    
    # 信頼度で降順ソート
    sorted_horses = sorted(horses_data, key=lambda h: h.get("confidence", h.get("conf", 0)), reverse=True)
    kept = []

    for horse in sorted_horses:
        too_close = False
        for kept_horse in kept:
            dx = horse["x"] - kept_horse["x"]
            dy = horse["y"] - kept_horse["y"]
            dist = np.sqrt(dx**2 + dy**2)
            if dist < distance_threshold:
                too_close = True
                break
        if not too_close:
            kept.append(horse)

    return kept

def detect_horses(image_path: str, model_name: str = "yolov8x.pt", imgsz: int = 1280, conf: float = 0.15):
    """
    指定された画像からYOLOv8を使用して馬を検出し、座標を出力します。
    
    Args:
        image_path (str): 画像ファイルのパス
        model_name (str): YOLOモデルの名前（例: yolov8n.pt, yolov8m.pt, yolov8x.pt）
        imgsz (int): 推論時の画像サイズ。大きいほど小さな馬を検出しやすい（デフォルト1280）
        conf (float): 検出の信頼度のしきい値。下げるほど多く検出するが誤検知も増える（デフォルト0.15）
    """
    if not os.path.exists(image_path):
        print(f"エラー: 画像が見つかりません: {image_path}")
        return

    print(f"[{image_path}] の処理を開始します...")
    print(f"使用モデル: {model_name}, 画像サイズ: {imgsz}, しきい値: {conf}")
    
    # モデルのロード（初回はダウンロードされます）
    model = YOLO(model_name)
    
    # 馬のクラスIDはCOCOデータセットで17
    # (0: person, 1: bicycle, ... 17: horse)
    results = model(image_path, classes=[17], imgsz=imgsz, conf=conf, iou=0.3, max_det=20, augment=True)
    
    horses_data = []
    
    # 結果の解析
    for r in results:
        # 検出結果の画像を表示・保存したい場合のために描画済み画像を取得
        im_array = r.plot()
        output_path = f"detected_{Path(image_path).name}"
        cv2.imwrite(output_path, im_array)
        print(f"検出結果を保存しました: {output_path}")

        for i, box in enumerate(r.boxes):
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            cx = (x1 + x2) / 2  # 中心X
            cy = (y1 + y2) / 2  # 中心Y
            conf_val = box.conf[0].item()
            
            horses_data.append({
                "id": i + 1,
                "x": cx,
                "y": cy,
                "confidence": conf_val
            })
            
    # 重複除去（距離40px以内は同一馬とみなす）
    horses_data = remove_duplicate_detections(horses_data, distance_threshold=40)

    print(f"\n--- 検出された馬の座標（重複除去後）---")
    for i, h in enumerate(horses_data):
        print(f"馬 {i+1}: 中心座標({h['x']:.1f}, {h['y']:.1f}), 信頼度: {h['confidence']:.2f}")
    print(f"合計: {len(horses_data)}頭")
    print("------------------------\n")
    return horses_data

def process_directory(directory_path: str, model_name: str = "yolov8x.pt", imgsz: int = 1280, conf: float = 0.15):
    """
    ディレクトリ内の全ての画像から馬を検出します。
    """
    dir_path = Path(directory_path)
    if not dir_path.exists():
        print(f"エラー: ディレクトリが見つかりません: {directory_path}")
        return

    model = YOLO(model_name)
    
    print(f"[{directory_path}] 内の画像を処理します...")
    print(f"使用モデル: {model_name}, 画像サイズ: {imgsz}, しきい値: {conf}")
    
    # jpg, png, jpegなどの画像を検索
    image_extensions = ["*.png", "*.jpg", "*.jpeg"]
    image_files = []
    for ext in image_extensions:
        image_files.extend(dir_path.glob(ext))
        
    if not image_files:
        print("画像ファイルが見つかりませんでした。")
        return

    all_results = []
    for img_path in image_files:
        print(f"\n処理中: {img_path.name}")
        results = model(str(img_path), classes=[17], imgsz=imgsz, conf=conf, iou=0.3, max_det=20, augment=True)
        
        horses = []
        for r in results:
            # 検出画像の保存
            output_path = dir_path / f"detected_{img_path.name}"
            im_array = r.plot()
            cv2.imwrite(str(output_path), im_array)

            for box in r.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                horses.append({
                    "x": float((x1 + x2) / 2),
                    "y": float((y1 + y2) / 2),
                    "confidence": float(box.conf[0].item())
                })
        
        horses = remove_duplicate_detections(horses, distance_threshold=40)
        
        all_results.append({
            "file": img_path.name,
            "horse_count": len(horses),
            "horses": horses
        })
        print(f"検出数: {len(horses)}頭")
        
    return all_results

def detect_horses_sahi(image_path: str, conf: float = 0.15):
    """SAHIを使用したスライス推論による馬の検出"""
    if not os.path.exists(image_path):
        print(f"エラー: 画像が見つかりません: {image_path}")
        return

    print(f"[{image_path}] SAHIによるスライス推論を開始します...")
    
    # モデルのロード
    detection_model = AutoDetectionModel.from_pretrained(
        model_type="yolov8",
        model_path="yolov8x.pt",
        confidence_threshold=conf,
        device="cpu",  # GPU環境がある場合は "cuda" に変更してください
    )

    # スライス推論（画像を分割して高精度検出）
    result = get_sliced_prediction(
        image_path,
        detection_model,
        slice_height=400,
        slice_width=400,
        overlap_height_ratio=0.2,
        overlap_width_ratio=0.2,
    )

    # 馬だけフィルタ（カテゴリ名が "horse" または IDが17 の場合）
    horses = [
        obj for obj in result.object_prediction_list
        if obj.category.name == "horse" or obj.category.id == 17
    ]

    print(f"\n--- SAHI検出結果 ---")
    horses_data = []
    for i, h in enumerate(horses):
        bbox = h.bbox
        cx = (bbox.minx + bbox.maxx) / 2
        cy = (bbox.miny + bbox.maxy) / 2
        score = h.score.value
        print(f"馬 {i+1}: 中心座標({cx:.1f}, {cy:.1f}), 信頼度: {score:.2f}")
        horses_data.append({"x": cx, "y": cy, "confidence": score})
    print(f"合計: {len(horses)}頭")

    # 結果画像の保存
    output_dir = Path(image_path).parent
    result.export_visuals(export_dir=str(output_dir), file_name=f"sahi_detected_{Path(image_path).stem}")
    print(f"SAHI検出結果を保存しました: {output_dir}/sahi_detected_{Path(image_path).stem}.png")

    return horses_data

# シーンタイプを自動判定
def classify_scene(image_path: str) -> dict:
    img = cv2.imread(image_path)
    if img is None:
        return {"width": 1280, "height": 720, "green_ratio": 0.2, "is_wide": True}
    h, w = img.shape[:2]
    aspect = w / h

    # 画像の輝度・色分布から状況を推定
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    green_mask = cv2.inRange(hsv, (35, 40, 40), (85, 255, 255))
    green_ratio = green_mask.sum() / (h * w * 255)

    return {
        "width": w,
        "height": h,
        "green_ratio": green_ratio,           # 芝の割合（高い=引きのカメラ）
        "is_wide": aspect > 1.5,              # 横長かどうか
    }

def filter_by_horse_cluster(detections, eps=150, min_samples=3):
    """
    DBSCANクラスタリングで最大クラスタ（馬の集団）のみ残す
    係員・スタッフなど孤立した検出を除去する
    """
    if len(detections) < 2:
        return detections

    coords = np.array([[d["x"], d["y"]] for d in detections])
    
    try:
        labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(coords)
    except NameError:
        print("警告: scikit-learnがインストールされていません。クラスタリングをスキップします。")
        return detections

    from collections import Counter
    valid_labels = [l for l in labels if l != -1]
    if not valid_labels:
        return detections

    # ② 最大クラスタだけでなく、上位N個のクラスタを統合
    cluster_counts = Counter(valid_labels)
    
    # 全クラスタの中で最大クラスタの半分以上の大きさなら統合
    max_count = cluster_counts.most_common(1)[0][1]
    major_clusters = {
        label for label, count in cluster_counts.items()
        if count >= max(2, max_count * 0.4)  # 最大の40%以上のクラスタも含める
    }

    filtered = [d for d, l in zip(detections, labels) if l in major_clusters]
    removed = len(detections) - len(filtered)
    if removed > 0:
        print(f"  → クラスタリングで {removed} 件の外れ値を除去しました")
    return filtered

def get_sahi_params_v2(scene: dict) -> dict:
    """修正版：緑が多い画像でも400x400を基本とする"""
    if scene["green_ratio"] > 0.15 and scene["width"] > 1200:
        # ③ 斜め俯瞰（緑多）→ 400x400に戻す（350は逆効果だった）
        return {"slice_height": 400, "slice_width": 400, "overlap": 0.25}
    elif not scene["is_wide"]:
        # 縦長カメラ
        return {"slice_height": 500, "slice_width": 400, "overlap": 0.25}
    else:
        # 標準（これが最も安定）
        return {"slice_height": 400, "slice_width": 400, "overlap": 0.2}

def detect_horses_adaptive(image_path: str, conf: float = 0.15, custom_model: str = "keiba_model/horse_detector_v1/weights/best.pt"):
    """シーンに応じてパラメータを自動調整する検出"""
    if not os.path.exists(image_path):
        print(f"エラー: 画像が見つかりません: {image_path}")
        return []

    scene = classify_scene(image_path)
    params = get_sahi_params_v2(scene)

    # ④ 斜め配列（対角線状）はepsを大きく取る
    cluster_eps = 300 if scene["green_ratio"] > 0.15 else 200

    print(f"\n[{image_path}] Adaptive検出を開始します...")
    print(f"シーン判定: 緑比率={scene['green_ratio']:.2f}, パラメータ={params}")

    # カスタムモデルが存在すればそちらを使用、なければ標準の yolov8x.pt
    model_path = custom_model if os.path.exists(custom_model) else "yolov8x.pt"
    if model_path == custom_model:
        print(f"★ カスタムモデルを読み込みました: {model_path}")

    detection_model = AutoDetectionModel.from_pretrained(
        model_type="yolov8",
        model_path=model_path,
        confidence_threshold=conf,
        device="cpu",
    )

    result = get_sliced_prediction(
        image_path,
        detection_model,
        slice_height=params["slice_height"],
        slice_width=params["slice_width"],
        overlap_height_ratio=params["overlap"],
        overlap_width_ratio=params["overlap"],
    )

    # ① 馬のみ取得（騎手補完は廃止）
    horses  = [o for o in result.object_prediction_list if o.category.name == "horse" or o.category.id == 17]

    horse_centers = []
    for h in horses:
        b = h.bbox
        horse_centers.append({
            "x": (b.minx + b.maxx) / 2,
            "y": (b.miny + b.maxy) / 2,
            "confidence": h.score.value,
            "source": "horse"
        })

    # ② 重複除去
    horse_centers = remove_duplicate_detections(horse_centers, distance_threshold=50)

    # ③ クラスタリングで外れ値除去（係員・ノイズを除去）
    horse_centers = filter_by_horse_cluster(horse_centers, eps=cluster_eps, min_samples=2)

    print(f"\n--- 検出結果 ---")
    for i, h in enumerate(horse_centers):
        print(f"馬 {i+1}: ({h['x']:.0f}, {h['y']:.0f}), 信頼度:{h['confidence']:.2f}")
    print(f"合計: {len(horse_centers)}頭\n")

    # 結果画像の保存
    output_dir = Path(image_path).parent
    result.export_visuals(export_dir=str(output_dir), file_name=f"adaptive_detected_{Path(image_path).stem}")
    print(f"Adaptive検出結果を保存しました: {output_dir}/adaptive_detected_{Path(image_path).stem}.png")

    return horse_centers

if __name__ == "__main__":
    # テスト用のベースディレクトリを作成（なければ）
    base_dir = Path("test_images")
    base_dir.mkdir(exist_ok=True)
    
    print(f"[{base_dir}] ディレクトリ（サブフォルダ含む）から画像を検索しています...")

    # 処理済みファイルのプレフィックス（除外対象）
    EXCLUDE_PREFIXES = ("adaptive_detected_", "sahi_detected_", "detected_")

    # jpg, png, jpegなどの画像をサブフォルダ含めて再帰的に検索
    image_extensions = ["*.png", "*.jpg", "*.jpeg", "*.PNG", "*.JPG", "*.JPEG"]
    image_files = []
    for ext in image_extensions:
        for f in base_dir.rglob(ext):
            # ① 処理済みファイルを除外
            if not f.name.startswith(EXCLUDE_PREFIXES):
                image_files.append(f)

    # 一意なファイルのリストにする
    image_files = list(set(image_files))

    if not image_files:
        print(f"画像ファイルが見つかりませんでした。")
        print(f"[{base_dir}] フォルダ内（またはそのサブフォルダ）に画像を配置してください。")
    else:
        print(f"合計 {len(image_files)} 枚の画像が見つかりました。連続処理を開始します...\n")
        
        all_results = []
        for i, img_path in enumerate(image_files):
            print(f"==================================================")
            print(f"処理中 ({i+1}/{len(image_files)}): {img_path}")
            print(f"==================================================")
            
            # シーン適応型のマルチクラス検出を実行
            horses = detect_horses_adaptive(str(img_path), conf=0.15)
            
            all_results.append({
                "file": str(img_path),
                "horse_count": len(horses),
                "horses": horses
            })
            
        print("\n全ての画像の処理が完了しました！")
