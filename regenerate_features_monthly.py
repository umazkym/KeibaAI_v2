import subprocess
import sys
from pathlib import Path
import calendar

def main():
    project_root = Path(__file__).resolve().parent
    script_path = project_root / 'keibaai/src/features/generate_features.py'
    
    year = 2024
    
    print("=== Starting Monthly Feature Regeneration for 2024 ===")
    
    for month in range(1, 13):
        # 月末日を計算
        _, last_day = calendar.monthrange(year, month)
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day}"
        
        print(f"[{month}/12] Generating features for {start_date} to {end_date}...")
        
        cmd = [
            sys.executable,
            str(script_path),
            '--start_date', start_date,
            '--end_date', end_date,
            '--log_level', 'INFO'
        ]
        
        # Windowsエンコーディング対応
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='cp932', errors='replace')
            
            if result.returncode != 0:
                print(f"Error generating features for {month}月:")
                print("STDERR:", result.stderr)
                print("STDOUT:", result.stdout)
            else:
                print(f"Success: {month}月")
        except Exception as e:
            print(f"Exception during subprocess: {e}")

    print("=== Completed ===")

if __name__ == '__main__':
    main()
