import sqlite3
import os
from pathlib import Path

def initialize_database():
    """
    仕様書で定義されたスキーマに基づいてSQLiteデータベースを初期化する
    """
    # スクリプトの場所を基準にプロジェクトルート（keibaaiディレクトリ）を決定
    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / 'data' / 'metadata' / 'db.sqlite3'
    
    # ディレクトリが存在しない場合は作成
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # fetch_log テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fetch_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT NOT NULL,
        file_path TEXT NOT NULL,
        fetched_ts TEXT NOT NULL,  -- ISO8601+09:00
        sha256 TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        fetch_method TEXT NOT NULL,  -- 'requests' or 'selenium'
        http_status INTEGER,
        error_message TEXT,
        UNIQUE(url, fetched_ts)
    );
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fetch_log_url ON fetch_log(url);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fetch_log_sha256 ON fetch_log(sha256);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_fetch_log_fetched_ts ON fetch_log(fetched_ts);')

    # model_metadata テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS model_metadata (
        model_id TEXT PRIMARY KEY,
        model_type TEXT NOT NULL,  -- 'mu_regressor', 'mu_ranker', 'sigma', 'nu'
        commit_hash TEXT NOT NULL,
        training_start TEXT NOT NULL,  -- ISO8601+09:00
        training_end TEXT NOT NULL,    -- ISO8601+09:00
        hyperparams TEXT NOT NULL,     -- JSON
        calibration_method TEXT,
        data_version TEXT NOT NULL,
        random_seed INTEGER NOT NULL,
        library_versions TEXT NOT NULL,  -- JSON
        performance_metrics TEXT,        -- JSON
        created_ts TEXT NOT NULL,        -- ISO8601+09:00
        notes TEXT
    );
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_model_metadata_created_ts ON model_metadata(created_ts);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_model_metadata_model_type ON model_metadata(model_type);')

    # data_versions テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS data_versions (
        version_id TEXT PRIMARY KEY,
        table_name TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        start_date TEXT NOT NULL,  -- ISO8601+09:00
        end_date TEXT NOT NULL,    -- ISO8601+09:00
        file_paths TEXT NOT NULL,  -- JSON array
        created_ts TEXT NOT NULL,  -- ISO8601+09:00
        sha256_manifest TEXT NOT NULL,  -- JSON: {file_path: sha256}
        notes TEXT
    );
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_versions_table_name ON data_versions(table_name);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_versions_created_ts ON data_versions(created_ts);')

    # parse_failures テーブル
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS parse_failures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        parser_name TEXT NOT NULL,
        source_file TEXT NOT NULL,
        race_id TEXT,
        horse_id TEXT,
        error_type TEXT NOT NULL,
        error_message TEXT,
        stack_trace TEXT,
        failed_ts TEXT NOT NULL,  -- ISO8601+09:00
        retry_count INTEGER DEFAULT 0,
        resolved BOOLEAN DEFAULT 0,
        resolved_ts TEXT,
        notes TEXT
    );
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parse_failures_parser_name ON parse_failures(parser_name);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parse_failures_race_id ON parse_failures(race_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parse_failures_resolved ON parse_failures(resolved);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_parse_failures_failed_ts ON parse_failures(failed_ts);')

    conn.commit()
    conn.close()
    
    print(f"データベース '{db_path}' が正常に初期化されました。")

if __name__ == '__main__':
    initialize_database()
