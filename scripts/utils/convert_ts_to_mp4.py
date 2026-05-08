import subprocess
import logging
import sys
from pathlib import Path
from typing import Optional

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def convert_ts_to_mp4(input_path: str, output_path: Optional[str] = None) -> bool:
    """
    .tsファイルを.mp4ファイルに変換する関数
    
    Args:
        input_path (str): 入力ファイル(.ts)のパス
        output_path (str, optional): 出力ファイル(.mp4)のパス。指定しない場合は入力と同じディレクトリに作成される。
        
    Returns:
        bool: 変換が成功した場合はTrue、失敗した場合はFalse
    """
    input_file = Path(input_path)
    if not input_file.exists():
        logger.error(f"入力ファイルが見つかりません: {input_path}")
        return False
        
    if not output_path:
        output_file = input_file.with_suffix('.mp4')
    else:
        output_file = Path(output_path)
        
    logger.info(f"変換開始: {input_file} -> {output_file}")
    
    # ffmpegコマンドの構成
    # -y: 出力ファイルが存在する場合に上書きする
    # -c copy: ビデオとオーディオのストリームを再エンコードせずにコピー（高速で品質劣化なし）
    command = [
        "ffmpeg",
        "-y",
        "-i", str(input_file),
        "-c", "copy",
        str(output_file)
    ]
    
    try:
        # ffmpegを実行
        process = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logger.info(f"変換が完了しました: {output_file}")
        return True
        
    except FileNotFoundError:
        logger.error("ffmpegが見つかりません。システムにffmpegがインストールされ、PATHが通っているか確認してください。")
        return False
        
    except subprocess.CalledProcessError as e:
        logger.warning(f"ストリームコピーに失敗しました。再エンコードを試みます。\n詳細: {e.stderr.decode('utf-8', errors='ignore')}")
        
        # -c copy でエラーになる場合（タイムスタンプの不整合等）、再エンコードを試す
        fallback_command = [
            "ffmpeg",
            "-y",
            "-i", str(input_file),
            "-c:v", "libx264",
            "-c:a", "aac",
            str(output_file)
        ]
        
        try:
            logger.info("再エンコード(-c:v libx264 -c:a aac)を実行中...")
            subprocess.run(
                fallback_command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logger.info(f"変換（再エンコード）が完了しました: {output_file}")
            return True
            
        except subprocess.CalledProcessError as e2:
            logger.error(f"再エンコードも失敗しました。\n詳細: {e2.stderr.decode('utf-8', errors='ignore')}")
            return False

if __name__ == "__main__":
    # 対象のファイルパス
    target_file = r"C:\Users\zk-ht\Downloads\Video\202602050301.ts"
    
    # 実行引数がある場合はそちらを優先する
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        
    success = convert_ts_to_mp4(target_file)
    if not success:
        sys.exit(1)
