# CLAUDE.md - KeibaAI_v2 Developer Guide for AI Assistants

**Last Updated**: 2025-11-16
**Project**: KeibaAI_v2 - Horse Racing AI Prediction & Optimal Investment System
**Purpose**: Guide for AI assistants working with this codebase

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture & Codebase Structure](#architecture--codebase-structure)
3. [Data Pipeline Flow](#data-pipeline-flow)
4. [Key Modules & Responsibilities](#key-modules--responsibilities)
5. [Development Workflows](#development-workflows)
6. [Configuration Management](#configuration-management)
7. [Testing Infrastructure](#testing-infrastructure)
8. [Common Tasks](#common-tasks)
9. [Code Conventions & Patterns](#code-conventions--patterns)
10. [Important Notes & Gotchas](#important-notes--gotchas)
11. [File Organization](#file-organization)
12. [Git Workflow](#git-workflow)

---

## 🎯 Project Overview

### What is KeibaAI_v2?

A **sophisticated horse racing AI prediction and optimal investment system** that:
- Scrapes horse racing data from netkeiba.com and JRA sources
- Parses HTML to structured data (Parquet format)
- Generates ML features from race history, pedigrees, and performance data
- Trains probabilistic models (μ, σ, ν) using LightGBM
- Runs Monte Carlo simulations to estimate win probabilities
- Optimizes betting portfolios using Fractional Kelly Criterion
- Tracks performance metrics (Brier score, ECE, ROI)

### Key Statistics

- **~5,025 lines** of core production code
- **278,098 race records** in current dataset
- **1,377,361 pedigree records** (5 generations)
- **100% test-driven** with unit, integration, and regression tests
- **Zero cloud costs** - fully local/personal use system

### Technology Stack

- **Python 3.10+** with type hints
- **Data**: pandas, pyarrow (Parquet), NumPy
- **ML**: LightGBM, scikit-learn
- **Scraping**: requests, BeautifulSoup4, Selenium
- **Storage**: Parquet files, SQLite metadata
- **Testing**: pytest
- **Config**: YAML files

---

## 🏗️ Architecture & Codebase Structure

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    KeibaAI_v2 System                         │
├─────────────────────────────────────────────────────────────┤
│  [Scraping] → [Parsing] → [Features] → [Models]             │
│      ↓           ↓            ↓            ↓                 │
│   HTML.bin   Parquet    Feature.pq   LightGBM               │
│                                          ↓                   │
---

## 🔄 Data Pipeline Flow

### Complete End-to-End Pipeline

```
---

## 📂 Project Structure and Placement Rules

**Last Updated**: 2025-11-24

### Directory Placement Rules

> [!IMPORTANT]
> **明確な配置ルール（2025-11-24 標準化完了）**:
> - **`keibaai/src/`**: Pythonパッケージとして `import` されるモジュール・クラス・関数のみ。一部レガシー実行スクリプトも残存。
> - **`scripts/`**: CLIから直接実行するスクリプト（エントリーポイント）。全て `python scripts/xxx/yyy.py` で実行。
> - **`scripts/training/experimental/`**: 実験的スクリプト・過去バージョン保存（モデル開発段階用）。
> - **`docs/`**: プロジェクトドキュメント。レポートは `docs/reports/` に集約。
> - **`archive/`**: 使用されなくなった古いスクリプト・ログ。

### Complete Directory Structure

```
KeibaAI_v2/
├── keibaai/                          # Main Python package
│   ├── src/                          # Source code (modules for import)
│   │   ├── modules/                  # Core business logic
│   │   │   ├── preparing/           # Web scraping modules
│   │   │   ├── parsers/             # HTML parsers
│   │   │   ├── features/            # Feature engineering modules
│   │   │   ├── models/              # Model classes (estimators)
│   │   │   ├── sim/                 # Simulation modules
│   │   │   ├── optimizer/           # Optimization modules
│   │   │   ├── executor/            # Execution modules (disabled)
│   │   │   ├── monitoring/          # Model analysis & monitoring
│   │   │   ├── validation/          # Data quality validation
│   │   │   └── constants/           # Constant definitions
│   │   ├── features/                # Feature modules
│   │   │   ├── feature_engine.py        # FeatureEngine class
│   │   │   ├── advanced_features.py     # Advanced features
│   │   │   ├── mu_v2_features.py        # μv2.0 features
│   │   │   ├── time_series_features.py  # Time series features
│   │   │   ├── track_bias.py            # Track bias features
│   │   │   └── generate_features.py     # [MOVED] -> scripts/pipelines/
│   │   ├── models/                  # Model modules
│   │   │   ├── model_train.py           # MuEstimator class
│   │   │   ├── calibration.py           # Probability calibration
│   │   │   ├── sigma_estimator.py       # SigmaEstimator class
│   │   │   └── nu_estimator.py          # NuEstimator class
│   │   ├── optimizer/               # Optimization modules
│   │   │   ├── optimizer.py             # PortfolioOptimizer class
│   │   │   └── daily_allocator.py       # ★LEGACY: Daily allocation script
│   │   ├── sim/                     # Simulation modules
│   │   │   └── simulator.py             # RaceSimulator class
│   │   ├── dashboard/               # Streamlit dashboard
│   │   │   ├── pages/               # Dashboard pages
│   │   │   └── app.py               # Dashboard entry point
│   │   ├── utils/                   # Utility functions
│   │   └── pipeline_core.py         # Pipeline common logic
│   ├── configs/                     # YAML configuration files
│   ├── data/                        # Local data storage (.gitignored)
│   │   ├── raw/html/                # Raw HTML files (.bin format)
│   │   │   ├── race/                # Race results
│   │   │   ├── shutuba/             # Entry lists
│   │   │   ├── horse/               # Horse data
│   │   │   └── ped/                 # Pedigree data
│   │   ├── parsed/parquet/          # Parsed Parquet files
│   │   │   ├── races/               # Race results
│   │   │   ├── shutuba/             # Entry lists
│   │   │   ├── horses/              # Horse profiles
│   │   │   └── pedigrees/           # Pedigree data
│   │   ├── features/parquet/        # Feature data (partitioned by year/month)
│   │   ├── models/                  # Trained models
│   │   ├── predictions/             # Prediction results
│   │   ├── simulations/             # Simulation results
│   │   ├── orders/                  # Order logs
│   │   ├── logs/                    # Application logs
│   │   └── quality_reports/         # Quality check reports
│   └── tests/                       # Test suite
│       ├── unit/                    # Unit tests
│       ├── integration/             # Integration tests
│       └── regression/              # Regression tests
├── scripts/                         # Executable scripts (CLI tools) ★
│   ├── pipelines/                   # Data pipeline scripts (9 scripts)
│   │   ├── run_scraping_resumable.py
│   │   ├── run_scraping_pipeline_local.py
│   │   ├── run_scraping_pipeline_with_args.py
│   │   ├── run_parsing_pipeline_local.py
│   │   └── run_parsing_resumable.py
│   ├── training/                    # Model training & prediction ★
│   │   ├── train_mu_model.py            # μ model training
│   │   ├── train_mu_v2_model.py         # μv2.0 model training
│   │   ├── train_sigma_nu_models.py     # σ/ν model training
│   │   ├── optimize_hyperparameters.py  # Hyperparameter optimization
│   │   ├── optimize_sigma_nu.py         # σ/ν optimization
│   │   ├── evaluate_model.py            # Model evaluation
│   │   ├── evaluate_model_advanced.py   # Advanced evaluation
│   │   ├── predict.py                   # Prediction (⚠️ needs fix)
│   │   └── experimental/                # Experimental scripts (model dev) ★
│   │       ├── README.md                # Purpose & usage guide
│   │       ├── train_full_pipeline.py   # Full pipeline experiment
│   │       └── train_sigma_nu_phase_d.py # Phase D implementation
│   ├── optimization/                # Portfolio optimization ★
│   │   └── optimize_daily_races.py      # Kelly criterion optimization
│   ├── simulation/                  # Simulation ★
│   │   └── simulate_daily_races.py      # Monte Carlo simulation
│   ├── debug/                       # Debug & validation tools (40+ scripts)
│   │   ├── debug_scraping_issues.py
│   │   ├── debug_combined.py
│   │   └── check_*.py, validate_*.py など
│   ├── analysis/                    # Analysis scripts
│   │   └── analyze_model_performance.py
│   └── temp/                        # Temporary work scripts
├── docs/                            # Documentation
│   ├── system/                      # System documentation (14 files)
│   │   ├── 01_システム概要.md
│   │   ├── 02_アーキテクチャ.md
│   │   ├── 10_運用ガイド.md
│   │   └── ...
│   ├── reports/                     # Reports ★
│   │   ├── FIX_REPORT_20251122.md
│   │   └── ...
│   ├── archive/                     # Archived docs
│   │   └── reports/                 # Old reports
│   ├── gemini.md                    # AI assistant guidelines
│   ├── instructions.md              # User instructions
│   └── schema.md                    # Data schema
├── archive/                         # Archived files ★
│   ├── scripts/                     # Deprecated scripts
│   │   └── debug/                   # Old debug scripts (160+ files)
│   └── logs/                        # Old logs
├── CLAUDE.md                        # This file (AI developer guide)
├── README.md                        # User-facing README
├── .gitignore                       # Git ignore rules
└── <utility scripts>                # Setup & utility scripts
    ├── standardize_structure.ps1
    ├── fix_moved_imports.py
    └── ...
```

**★ マーク**: 2025-11-24 標準化で整理されたディレクトリ・ファイル

### File Placement Decision Tree

新しいファイルを作成するときの配置ガイド:

```
新しいファイルを作成する
    ↓
[Q1] これは実行スクリプト (python xxx.py で直接実行)?
    YES → ┐
    NO  → keibaai/src/<category>/xxx.py (モジュール・クラス定義)
          ↓
    [Q2] 本番環境で使用?
        YES → scripts/<category>/xxx.py
        NO  → ┐
              ↓
        [Q3] 実験的スクリプト? (過去バージョン、試行錯誤)
            YES → scripts/training/experimental/xxx.py
            NO  → scripts/temp/xxx.py (一時スクリプト)
```

### Experimental Scripts (`scripts/training/experimental/`)

**Purpose**: モデル開発段階での実験的スクリプト・過去バージョンの保存

**保存対象**:
- 過去の訓練アプローチ (`train_sigma_nu_phase_d.py`)
- 実験的な実装 (`train_full_pipeline.py`)
- 異なるアルゴリズムの試行

**使用方法**:
- **参照**: 過去のアプローチを理解・比較
- **再現**: 特定バージョンのモデルを再現
- **分析**: 試行錯誤の記録として後から推論・分析

**注意**: これらは本番では使用されません。動作保証もありません。

### Legacy Scripts

一部の実行スクリプトが `keibaai/src/` に残存:

| ファイル | 理由 | 将来計画 |
|---------|------|---------|
| `keibaai/src/features/generate_features.py` | 使用頻度が高い | 段階的に `scripts/` へ移行検討 |
| `keibaai/src/optimizer/daily_allocator.py` | レガシー | 必要に応じて移行 |

---

## 🔄 Data Pipeline Flow

### Complete End-to-End Pipeline

```
┌────────────────────────────────────────────────────────────────┐
│ PHASE 0: Data Acquisition (Deep Night Batch - 03:00)          │
└────────────────────────────────────────────────────────────────┘
    ├─► [Scrape race dates] → kaisai_dates.csv
    ├─► [Scrape race IDs] → race_id_list.csv
    ├─► [Scrape race results] → data/raw/html/race/*.bin
    ├─► [Scrape entry lists] → data/raw/html/shutuba/*.bin
    ├─► [Extract horse IDs] → horse_id_list.txt
    ├─► [Scrape horse profiles] → data/raw/html/horse/*_profile.bin
    ├─► [Scrape horse performance] → data/raw/html/horse/*_perf.bin
    └─► [Scrape pedigrees] → data/raw/html/ped/*.bin

┌────────────────────────────────────────────────────────────────┐
│ PHASE 1: Parsing (Immediately After Scraping)                 │
└────────────────────────────────────────────────────────────────┘
    ├─► [Parse race results] → races.parquet (27+ columns)
    ├─► [Parse entry lists] → shutuba.parquet (21 columns)
    ├─► [Parse horse profiles] → horses.parquet
    └─► [Parse pedigrees] → pedigrees.parquet (5 generations)

┌────────────────────────────────────────────────────────────────┐
│ PHASE 2: Feature Engineering (After Parsing)                  │
└────────────────────────────────────────────────────────────────┘
    ├─► Load: races, shutuba, horses, pedigrees
    ├─► Generate: race features (distance, surface, weather)
    ├─► Generate: horse features (rolling stats, trends)
    ├─► Generate: jockey/trainer features (win rates, ROI)
    ├─► Generate: pedigree features (sire/dam encoding)
    └─► Save: features/parquet/year=YYYY/month=MM/ (partitioned)

┌────────────────────────────────────────────────────────────────┐
│ PHASE 3: Model Training (Weekly/On-Demand)                    │
└────────────────────────────────────────────────────────────────┘
    ├─► Train μ model (LightGBM Ranker + Regressor)
    ├─► Train σ model (variance estimation)
    ├─► Train ν model (chaos parameter, t-distribution)
    ├─► Calibrate probabilities (temperature scaling)
    └─► Save models → data/models/{model_id}/

┌────────────────────────────────────────────────────────────────┐
│ PHASE 4: Daily Prediction (Race Day Morning - 10:00)          │
└────────────────────────────────────────────────────────────────┘
    ├─► Load today's features
    ├─► Run μ, σ, ν inference
    └─► Save predictions → predictions/parquet/

┌────────────────────────────────────────────────────────────────┐
│ PHASE 5: Simulation (10-30 min before race)                   │
└────────────────────────────────────────────────────────────────┘
    ├─► Load predictions (μ, σ, ν)
    ├─► Run Monte Carlo (K=1000 iterations)
    ├─► Compute win/place/exacta probabilities
    └─► Save simulations → simulations/{race_id}.json

┌────────────────────────────────────────────────────────────────┐
│ PHASE 6: Optimization (Just before betting deadline)          │
└────────────────────────────────────────────────────────────────┘
    ├─► Scrape latest JRA odds (Selenium)
    ├─► Load simulation results
    ├─► Run Fractional Kelly optimization
    ├─► Filter by EV threshold
    └─► Save orders → orders/{race_id}_order.json

┌────────────────────────────────────────────────────────────────┐
│ PHASE 7: Execution (MANUAL - Disabled by Default)             │
└────────────────────────────────────────────────────────────────┘
    └─► Paper trading only (logs for backtesting)

┌────────────────────────────────────────────────────────────────┐
│ PHASE 8: Monitoring (Post-Race)                               │
└────────────────────────────────────────────────────────────────┘
    ├─► Scrape final results
    ├─► Calculate actual returns
    ├─► Update Brier score, ECE, ROI metrics
    └─► Save metrics → metrics/weekly_report.json
```

---

## 🔧 Key Modules & Responsibilities

### 1. Preparing Module (`modules/preparing/`)

**Purpose**: Web scraping from netkeiba.com

**Key Files**:
- `_scrape_html.py` (23KB): Main scraping engine
  - Anti-ban measures: random user agents, sleep intervals, retry backoff
  - Handles HTTP 400 → 60-second pause
  - Saves as `.bin` files (EUC-JP encoded HTML)
- `_scrape_jra_odds.py`: JRA official odds (currently stub)
- `_requests_utils.py`: HTTP utilities with robust error handling

**Technologies**: requests, BeautifulSoup4, Selenium

### 2. Parsers Module (`modules/parsers/`)

**Purpose**: Convert raw HTML to structured DataFrames

**4 Parsers**:

1. **`results_parser.py`** (14KB)
   - Parses race results HTML
   - Extracts: finish data, race metadata, jockey/trainer/owner info
   - **Recent improvements**: Enhanced metadata extraction (distance_m, track_surface, weather, venue, race_class, etc.)
   - **Robustness**: 4-level fallback for distance/surface extraction
   - Handles obstacle races, regional races, multiple HTML formats

2. **`shutuba_parser.py`** (14KB)
   - Parses entry lists (出馬表)
   - Extracts: morning odds, blinkers, prediction marks

3. **`horse_info_parser.py`** (16KB)
   - Parses horse profile pages
   - Extracts: birth date, breeder, physical stats
   - Parses past performance tables

4. **`pedigree_parser.py`** (9KB)
   - Parses 5-generation pedigree trees
   - Extracts: ancestor IDs, names, coat colors, birth years

**Common Utilities** (`common_utils.py`):
- `parse_time_to_seconds()`: HH:MM.S → seconds
- `parse_margin_to_seconds()`: body lengths → seconds
- `parse_sex_age()`: "牡3" → sex="牡", age=3
- `parse_horse_weight()`: "476(+2)" → weight=476, change=+2

**Output Format**: pandas DataFrame → Parquet files

### 3. Features Module (`features/`)

**Purpose**: Generate ML features from parsed data

**Key Components**:
- `feature_engine.py` (25KB): Central feature generation engine
- `generate_features.py` (10KB): CLI script for feature generation
- `advanced_features.py` (9KB): Advanced features (interaction terms, domain-specific)
- `time_series_features.py` (3KB): Rolling window calculations, trend detection

**Feature Categories**:
- **Race features**: distance, surface, weather, track_condition, class
- **Horse features**: age, sex, weight, weight_change, past performance aggregates
- **Jockey features**: win_rate, place_rate, ROI
- **Trainer features**: stable statistics
- **Pedigree features**: sire/dam performance encoding
- **Derived features**: pace_index, position_changes, popularity_finish_diff

### 4. Models Module (`models/`)

**Purpose**: Train probabilistic models

**Mathematical Framework**: Three-parameter model (μ, σ, ν)
- **μ (mu)**: Expected finish time for each horse
- **σ (sigma)**: Horse-specific variance (uncertainty)
- **ν (nu)**: Race-level "chaos" parameter (t-distribution degrees of freedom)

**Key Files**:
- `model_train.py`: MuEstimator class (LightGBM)
- `train_mu_model.py`: CLI script for μ model training
- `sigma_estimator.py`: σ model (variance)
- `nu_estimator.py`: ν model (heavy-tailed distribution)
- `train_sigma_nu_models.py`: Combined σ and ν training
- `calibration.py`: Probability calibration (temperature scaling)
- `predict.py`: Inference script
- `evaluate_model.py`: Metrics (Brier score, log loss, accuracy)

### 5. Simulation Module (`sim/`)

**Purpose**: Monte Carlo simulation for win probability

**Algorithm**:
```python
For k = 1 to K (default K=1000):
    For each horse i:
        time_i,k ~ t_ν(μ_i, σ_i²)
    Sort horses by time
    Record finish order
Estimate P(win) = count(1st place) / K
```

**Key Files**:
- `simulator.py`: RaceSimulator class
- `simulate_daily_races.py`: CLI script for daily simulations

### 6. Optimization Module (`optimizer/`)

**Purpose**: Portfolio optimization using Fractional Kelly Criterion

**Formula**:
```
Maximize: Σ f_i * log(1 + b_i * o_i)
Subject to:
  - Σ f_i ≤ W_0 (capital constraint)
  - f_i ≥ 0 (no short selling)
  - EV_i > threshold (expected value filter)
```

**Key Files**:
- `optimizer.py`: PortfolioOptimizer class
- `optimize_daily_races.py`: CLI script for daily optimization

### 7. Monitoring Module (`monitoring/`)

**Purpose**: Performance tracking

**Metrics**:
- Brier score (probability calibration)
- ECE (Expected Calibration Error)
- ROI (Return on Investment)
- Hit rate (accuracy)

---

## 🛠️ Development Workflows

### Workflow 1: Data Quality Improvement

**Evidence from codebase**:
- 20+ `debug_*.py` scripts in root
- `DEBUG_REPORT.md`: Documents HTML parser improvements
- `PROGRESS.md`: Tracks data quality issues

**Recent Fixes**:
1. Changed HTML parser from `lxml` to `html.parser` (compatibility)
2. Reduced race result missing data from 0.8% to near-zero
3. Added 11 new metadata columns to race results
4. Implemented 4-level fallback for distance/surface extraction

### Workflow 2: Parser Development

**Process**:
1. Write parser in `modules/parsers/`
2. Create debug script (e.g., `debug_scraping_and_parsing.py`)
3. Validate with `validate_parquet.py`, `validate_parsed_data.py`
4. Update `schema.md` with new columns
5. Run regression tests

**Key Pattern**:
```python
# Always use html.parser (NOT lxml)
soup = BeautifulSoup(html_text, 'html.parser')

# Always use nullable integer types
df['finish_position'] = df['finish_position'].astype('Int64')

# Always handle multiple HTML formats (fallback patterns)
race_data = soup.find('div', class_='data_intro')
if not race_data:
    race_data = soup.find('div', class_='diary_snap_cut')
if not race_data:
    race_data = soup.find('dl', class_='racedata')
```

### Workflow 3: Feature Engineering Experimentation

**Process**:
1. Explore data in Jupyter notebooks (`notebooks/`)
2. Implement features in `features/advanced_features.py`
3. Validate with `check_features_data.py`
4. Update `configs/features.yaml` to enable new features
5. Regenerate features with `generate_features.py`

### Workflow 4: Model Development Cycle

**Process**:
1. Train models weekly: `python scripts/training/train_mu_model.py`
2. Evaluate on validation set: `python scripts/training/evaluate_model.py`
3. Calibrate probabilities: Use calibration module
4. Backtest on historical data
5. Monitor Brier score and ECE

---

## ⚙️ Configuration Management

### 5 YAML Configuration Files

Located in: `keibaai/configs/`

1. **`default.yaml`**: Base paths and logging
   ```yaml
   data_path: "data"
   database: {path: "${metadata_path}/db.sqlite3"}
   logging: {level: "INFO", format: "%(asctime)s - %(levelname)s - %(message)s"}
   ```

2. **`scraping.yaml`**: Scraping behavior
   - Date ranges, retry settings, sleep intervals

3. **`features.yaml`**: Feature generation settings
   ```yaml
   feature_engine: {version: "v1.0"}
   race_features: {enabled: true}
   horse_features: {enabled: true}
   jockey_features: {enabled: true}
   trainer_features: {enabled: true}
   pedigree_features: {enabled: true}
   output: {partition_by: [year, month]}
   ```

4. **`models.yaml`**: Model hyperparameters
   ```yaml
   models:
     lgbm_ranker:
       objective: "lambdarank"
       hyperparameters:
         n_estimators: 2000
         learning_rate: 0.01
         num_leaves: 31
   ```

5. **`optimization.yaml`**: Portfolio constraints
   - Kelly fraction, max bet size, EV thresholds

**Variable Substitution**: Supports `${data_path}` placeholders

---

## 🧪 Testing Infrastructure

### Test Organization

Located in: `keibaai/tests/`

1. **Unit Tests** (`tests/unit/`):
   - `test_parsers.py`: Parser validation with fixture HTML
   - `test_features.py`: Feature engineering logic
   - `test_models.py`: Model components

2. **Integration Tests** (`tests/integration/`):
   - `test_pipeline_e2e.py`: End-to-end pipeline validation

3. **Regression Tests** (`tests/regression/`):
   - `test_parser_regression.py`: Ensures parser output consistency

### Test Fixtures

- Sample HTML files in `tests/fixtures/`
- Organized by type: `race_samples/`, `shutuba_samples/`, `horse_samples/`, `ped_samples/`

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_parsers.py

# Run with coverage
pytest --cov=keibaai
```

---

## 📝 Common Tasks

### Task 1: Scrape New Data

```bash
# Edit date range in script first
python scripts/pipelines/run_scraping_resumable.py
```

**What it does**:
1. Scrapes race dates → `kaisai_dates.csv`
2. Scrapes race IDs → `race_id_list.csv`
3. Scrapes race results → `data/raw/html/race/*.bin`
4. Scrapes entry lists → `data/raw/html/shutuba/*.bin`
5. Scrapes horse data → `data/raw/html/horse/*.bin`

### Task 2: Parse Scraped Data

```bash
python scripts/pipelines/run_parsing_resumable.py
```

**What it does**:
1. Parses race results → `data/parsed/parquet/races/races.parquet`
2. Parses shutuba → `data/parsed/parquet/shutuba/shutuba.parquet`
3. Parses horses → `data/parsed/parquet/horses/horses.parquet`
4. Parses pedigrees → `data/parsed/parquet/pedigrees/pedigrees.parquet`

### Task 3: Generate Features

```bash
python scripts/pipelines/generate_features.py \
  --start_date 2023-01-01 \
  --end_date 2023-12-31
```

**Output**: `data/features/parquet/year=YYYY/month=MM/`

### Task 4: Train Models

```bash
# Train μ model
python scripts/training/train_mu_v2_model.py

# Train σ and ν models
python scripts/training/train_sigma_nu_models.py
```

### Task 5: Run Predictions

```bash
python scripts/training/predict.py \
  --date 2023-10-01 \
  --model_dir data/models/latest
```

### Task 6: Run Simulation

```bash
python scripts/simulation/simulate_daily_races.py \
  --date 2023-10-01 \
  --K 1000
```

### Task 7: Optimize Portfolio

```bash
python scripts/optimization/optimize_daily_races.py \
  --date 2023-10-01 \
  --W_0 100000
```

### Task 8: Validate Data Quality

```bash
# Check parsed data
python check_parsed_data.py

# Check features
python check_features_data.py

# Validate parquet files
python validate_parquet.py
```

---

## 📐 Code Conventions & Patterns

### 1. File Naming

- **Modules**: `snake_case.py`
- **Scripts**: `scripts/pipelines/`, `scripts/training/` etc.
- **Debug scripts**: `scripts/debug/debug_*.py`
- **Validation scripts**: `scripts/debug/check_*.py`

### 2. Data Files

- **Raw HTML**: `.bin` extension, EUC-JP encoding
- **Parsed data**: `.parquet` (columnar format)
- **Features**: `.parquet`, partitioned by year/month
- **Models**: Directory per model (e.g., `data/models/20231001_lgbm/`)
- **Logs**: JSON or text, organized by date

### 3. Pandas DataFrame Patterns

**Always use nullable integer types**:
```python
# ❌ BAD: float64 for integers
df['finish_position'] = df['finish_position'].astype('float64')

# ✅ GOOD: nullable integer
df['finish_position'] = df['finish_position'].astype('Int64')
```

**Always use explicit dtype mapping**:
```python
int_columns = [
    'finish_position', 'bracket_number', 'horse_number', 'age',
    'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
    'popularity', 'horse_weight', 'horse_weight_change',
    'distance_m', 'head_count', 'round_of_year', 'day_of_meeting'
]

for col in int_columns:
    if col in df.columns:
        df[col] = df[col].astype('Int64')
```

### 4. HTML Parsing Patterns

**Always use `html.parser` (NOT `lxml`)**:
```python
# ✅ CORRECT
soup = BeautifulSoup(html_text, 'html.parser')

# ❌ WRONG
soup = BeautifulSoup(html_text, 'lxml')
```

**Always implement fallback patterns**:
```python
# 4-level fallback for robustness
race_data = soup.find('div', class_='data_intro')
if not race_data:
    race_data = soup.find('div', class_='diary_snap_cut')
if not race_data:
    race_data_dl = soup.find('dl', class_='racedata')
    if race_data_dl:
        race_data = race_data_dl.find('dd')
if not race_data:
    # Log warning and return empty
    logging.warning(f"Race data not found in {file_path}")
    return {}
```

### 5. Error Handling

**Always use try-except with logging**:
```python
try:
    df = parse_race_results(file_path)
except Exception as e:
    logging.error(f"Parse error in {file_path}: {e}")
    # Save to error database
    save_parse_failure(file_path, str(e))
    return pd.DataFrame()
```

### 6. Configuration Loading

**Always use centralized config loading**:
```python
from keibaai.src.utils.config_utils import load_config

config = load_config('default')  # Loads configs/default.yaml
```

### 7. Path Handling

**Always use Path objects**:
```python
from pathlib import Path

data_path = Path(config['data_path'])
parquet_path = data_path / 'parsed' / 'parquet' / 'races'
parquet_path.mkdir(parents=True, exist_ok=True)
```

### 8. Logging

**Always use module-level logger**:
```python
import logging

logger = logging.getLogger(__name__)

logger.info("Processing started")
logger.warning("Missing data detected")
logger.error("Parse failed", exc_info=True)
```

---

## ⚠️ Important Notes & Gotchas

### 1. HTML Encoding

**Issue**: Raw HTML files are **EUC-JP encoded**, not UTF-8

**Solution**:
```python
with open(file_path, 'rb') as f:
    html_bytes = f.read()

try:
    html_text = html_bytes.decode('euc_jp', errors='replace')
except:
    html_text = html_bytes.decode('utf-8', errors='replace')
```

### 2. Data Leakage Prevention

**CRITICAL**: Never use final odds in training

```python
# ❌ DATA LEAKAGE
X_train = features[['distance_m', 'win_odds', 'popularity']]

# ✅ CORRECT (use morning odds only)
X_train = features[['distance_m', 'morning_odds', 'jockey_win_rate']]
```

**Reason**: Final odds are determined just before race start and already incorporate all public information. Using them in training would give artificially high accuracy that doesn't translate to real predictions.

### 3. Time Series Validation

**Always use TimeSeriesSplit, NOT KFold**:
```python
from sklearn.model_selection import TimeSeriesSplit

# ✅ CORRECT
tscv = TimeSeriesSplit(n_splits=5)
for train_idx, valid_idx in tscv.split(X):
    # Train on past, validate on future
    X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
```

**Reason**: Horse racing data is time-dependent. Random splits cause data leakage from future to past.

### 4. Parquet Partitioning

**Features are partitioned by year/month**:
```python
# Reading partitioned data
import pyarrow.parquet as pq

table = pq.read_table('data/features/parquet/',
                      filters=[('year', '=', 2023), ('month', '=', 10)])
df = table.to_pandas()
```

### 5. Nullable vs. NaN

**Use pd.NA for missing values, NOT np.nan**:
```python
# ✅ CORRECT
df['finish_position'] = pd.NA

# ❌ WRONG (converts to float)
df['finish_position'] = np.nan
```

### 6. Race ID Format

**Format**: `YYYYPPNNDDRR`
- `YYYY`: Year (4 digits)
- `PP`: Venue code (2 digits)
  - 05 = Tokyo (東京)
  - 06 = Nakayama (中山)
  - 08 = Kyoto (京都)
  - 09 = Hanshin (阪神)
  - Others: See venue master data
- `NN`: Round number (2 digits) - e.g., 04 = 4th round
- `DD`: Day of meeting (2 digits) - e.g., 03 = 3rd day
- `RR`: Race number (2 digits) - e.g., 01 = Race 1, 12 = Race 12

**Example**: `202305040301` =
- 2023年 (Year 2023)
- 東京競馬場 (Tokyo, code 05)
- 第4回開催 (4th round)
- 3日目 (3rd day)
- 1R (Race 1)

**URL Example**: `https://db.netkeiba.com/race/202305040301`

### 7. Horse ID Format

**Format**: `YYYYNNNNNN`
- `YYYY`: Birth year (4 digits)
- `NNNNNN`: Sequential number (6 digits)

**Example**: `2009100502` = Born in 2009, ID 100502

### 8. Disabled Executor

**The order executor is DISABLED by default**:
- Legal/ethical concerns about automated gambling
- Only generates order logs for paper trading
- Enable only for research/backtesting

### 9. Memory Management

**Large datasets require memory optimization**:
```python
# Use categorical dtypes for repeated strings
df['jockey_name'] = df['jockey_name'].astype('category')
df['track_surface'] = df['track_surface'].astype('category')

# Use nullable integers
df['finish_position'] = df['finish_position'].astype('Int64')
```

### 10. Anti-Ban Strategy

**Scraping must respect server load**:
- Random sleep: 2.5-5 seconds between requests
- HTTP 400 detection → 60-second pause
- Random user agents
- Retry with exponential backoff

**Do NOT**:
- Run multiple scrapers in parallel
- Reduce sleep intervals below 2 seconds
- Ignore HTTP 400/403 errors

---

## 📂 File Organization

### Data Files (.gitignored)

```
data/
├── raw/html/              # Never commit (large binary files)
├── parsed/parquet/        # Never commit (generated from raw)
├── features/parquet/      # Never commit (generated from parsed)
├── models/                # Never commit (large model files)
├── simulations/           # Never commit (ephemeral)
├── orders/                # Commit sample only
├── logs/                  # Never commit
└── metadata/db.sqlite3    # Consider committing schema only
```

### Documentation Files (commit these)

```
root/
├── schema.md              # Data schema documentation
├── PROGRESS.md            # Data quality tracking
├── DEBUG_REPORT.md        # Parser improvement reports
├── CLAUDE.md              # This file
├── 指示.md                # Japanese requirements/instructions
└── README.md              # User-facing documentation (TODO)
```

### Debug Scripts (commit selectively)

```
root/
├── debug_*.py             # Keep useful ones, remove obsolete
├── check_*.py             # Keep validation scripts
├── validate_*.py          # Keep validation scripts
└── inspect_*.py           # Keep inspection scripts
```

---

## 🔀 Git Workflow

### Branch Strategy

**Main branch**: `main` or `master` (production-ready code)

**Feature branches**: `claude/claude-md-mi12qj9z2fr7948a-01EkmapzBs2F3dK9kganLBYD` (session-specific)

### Commit Message Conventions

**Format**: `<type>: <description>`

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code refactoring (no functional change)
- `perf`: Performance improvement
- `docs`: Documentation update
- `test`: Test addition/modification
- `chore`: Build/tooling changes

**Examples**:
```
feat: Add 11 new metadata columns to race results parser
fix: Use html.parser instead of lxml for better compatibility
refactor: Extract common HTML parsing utilities to shared module
perf: Optimize feature generation with vectorized operations
docs: Update schema.md with new race metadata columns
```

### Commit Workflow

```bash
# Make changes
git add .

# Commit with clear message
git commit -m "fix: Improve distance/surface extraction with 4-level fallback"

# Push to feature branch
git push -u origin claude/claude-md-mi12qj9z2fr7948a-01EkmapzBs2F3dK9kganLBYD
```

### Pull Request Guidelines

**When creating PR**:
1. Include summary of changes
2. Reference related issues/documents (e.g., "Fixes issue #123", "See DEBUG_REPORT.md")
3. List testing performed
4. Note any breaking changes

**Example PR template**:
```markdown
## Summary
Improved race metadata extraction by implementing 4-level fallback for distance/surface parsing.

## Changes
- Modified `results_parser.py` to try 4 different HTML selectors
- Reduced missing data from 10.2% to 0%
- Added regression tests for obstacle races

## Testing
- Validated with 440 race samples from 2023-10-09
- All regression tests pass
- No breaking changes to output schema

## References
- See DEBUG_REPORT.md for detailed analysis
```

---

## 🎓 Learning Resources

### Understanding the System

1. **Start with**: `schema.md` - Understand data structure
2. **Then read**: `DEBUG_REPORT.md` - Learn about recent improvements
3. **Review**: `PROGRESS.md` - Understand data quality journey
4. **Explore**: `指示.md` - Original requirements (Japanese)

### Key Papers/Concepts

1. **Fractional Kelly Criterion**: Portfolio optimization under uncertainty
2. **LightGBM**: Gradient boosting for ranking
3. **Probability Calibration**: Temperature scaling, isotonic regression
4. **Monte Carlo Simulation**: Estimating probability distributions
5. **Expected Calibration Error (ECE)**: Measuring probability accuracy

### Similar Systems

- **betfair**: Betting exchange with trading API
- **pymc**: Probabilistic programming for Bayesian models
- **mlflow**: ML experiment tracking
- **optuna**: Hyperparameter optimization

---

## 🚀 Future Enhancements

### Short-term (1-3 months)

1. Complete JRA odds scraping (Selenium implementation)
2. Build monitoring dashboard (Streamlit/Dash)
3. Automate model retraining pipeline
4. Expand test coverage to 80%+

### Medium-term (3-6 months)

1. Add support for different bet types (trifecta, quinella)
2. Implement ensemble models (stacking)
3. Add feature importance visualization
4. Create comprehensive API documentation

### Long-term (6-12 months)

1. Deploy as web service (optional)
2. Add real-time odds tracking
3. Implement reinforcement learning for dynamic betting
4. Create mobile app for notifications

---

## 📞 Contact & Support

### For AI Assistants

When working with this codebase:
1. **Always read**: `schema.md`, `PROGRESS.md`, `DEBUG_REPORT.md` first
2. **Never commit**: Large data files (`.bin`, `.parquet`, models)
3. **Always validate**: Changes with regression tests
4. **Always document**: New features in this file
5. **Always follow**: Code conventions in this guide

### For Human Developers

- **GitHub Issues**: Report bugs, request features
- **Pull Requests**: Contribute improvements
- **Documentation**: Update CLAUDE.md when making significant changes

---

## 📜 Version History

| Version | Date       | Changes                                      |
|---------|------------|----------------------------------------------|
| 1.0     | 2025-11-16 | Initial comprehensive CLAUDE.md creation     |

---

**End of CLAUDE.md**

This document should be updated whenever:
- New modules are added
- Data schemas change significantly
- New workflows are established
- Important patterns emerge
- Critical bugs are discovered and fixed
