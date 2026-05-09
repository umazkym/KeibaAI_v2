from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import HuberRegressor, Ridge
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


@dataclass
class IndexModelConfig:
    central_col: str = "is_central"
    race_id_col: str = "race_id"
    horse_id_col: str = "horse_id"
    date_col: str = "race_date"
    finish_time_col: str = "finish_time_seconds"
    last3f_col: str = "last_3f_time"
    pace_col: str = "pace_index"
    position_col: str = "passing_order_4"
    finish_pos_col: str = "finish_position"
    margin_col: str = "margin"
    margin_seconds_col: str = "margin_seconds"
    distance_col: str = "distance_m"
    surface_col: str = "track_surface"
    condition_col: str = "track_condition"
    venue_col: str = "venue"
    class_col: str = "race_class"
    course_direction_col: str = "course_direction"
    is_outer_col: str = "is_outer_course"
    weight_col: Optional[str] = "carried_weight"


def _to_jsonable(obj):
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_to_jsonable(v) for v in obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    if pd.isna(obj):
        return None
    return obj


class RaceIndexModeler:
    """T指数・展開補正T指数・期待上がり差分指数を作る実装。"""

    def __init__(self, config: IndexModelConfig | None = None) -> None:
        self.cfg = config or IndexModelConfig()
        self.t_baseline_table_: Optional[pd.DataFrame] = None
        self.agari_model_: Optional[Pipeline] = None
        self.tenkai_model_: Optional[Pipeline] = None
        self._agari_features_: Optional[Sequence[str]] = None
        self._tenkai_features_: Optional[Sequence[str]] = None

    def validate_schema(self, df: pd.DataFrame) -> None:
        required = [
            self.cfg.race_id_col, self.cfg.horse_id_col, self.cfg.date_col,
            self.cfg.finish_time_col, self.cfg.last3f_col, self.cfg.pace_col,
            self.cfg.distance_col, self.cfg.surface_col, self.cfg.condition_col,
            self.cfg.venue_col, self.cfg.class_col, self.cfg.course_direction_col,
            self.cfg.is_outer_col, self.cfg.position_col, self.cfg.finish_pos_col,
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            present = set(df.columns)
            is_lap_dataset = {"lap_times", "first_half", "second_half"}.issubset(present)
            hint = ""
            if is_lap_dataset:
                hint = (
                    " Detected lap-detail schema. This script requires horse-level result parquet "
                    "(e.g., with horse_id, finish_time_seconds, last_3f_time, pace_index)."
                )
            raise ValueError(f"Missing required columns: {missing}.{hint}")

    @staticmethod
    def _winsorize_series(s: pd.Series, p: float = 0.01) -> pd.Series:
        lo, hi = s.quantile(p), s.quantile(1 - p)
        return s.clip(lower=lo, upper=hi)

    def _build_t_baseline(self, df: pd.DataFrame) -> pd.DataFrame:
        key_cols = [self.cfg.distance_col, self.cfg.surface_col, self.cfg.condition_col]
        tmp = df.dropna(subset=[self.cfg.finish_time_col] + key_cols).copy()
        tmp["finish_time_w"] = self._winsorize_series(tmp[self.cfg.finish_time_col])
        agg = tmp.groupby(key_cols, observed=True).agg(
            n=(self.cfg.finish_time_col, "size"),
            time_mean=("finish_time_w", "mean"),
            time_std=("finish_time_w", "std"),
        ).reset_index()

        gmean = float(tmp["finish_time_w"].mean())
        gstd = float(tmp["finish_time_w"].std(ddof=1))

        # Empirical Bayes shrinkage: N<10の条件を全体統計へ縮約
        k = 20.0
        agg["time_mean_shrunk"] = (agg["n"] * agg["time_mean"] + k * gmean) / (agg["n"] + k)
        agg["time_std_shrunk"] = np.sqrt(
            ((agg["n"] - 1) * agg["time_std"].fillna(gstd) ** 2 + k * gstd**2) / (agg["n"] + k - 1)
        )
        agg["time_std_shrunk"] = agg["time_std_shrunk"].replace(0, gstd).fillna(gstd)
        return agg

    def fit_t_index(self, df: pd.DataFrame) -> "RaceIndexModeler":
        self.validate_schema(df)
        self.t_baseline_table_ = self._build_t_baseline(df)
        return self

    def _merge_baseline(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.t_baseline_table_ is None:
            raise RuntimeError("fit_t_index must be called first")
        key_cols = [self.cfg.distance_col, self.cfg.surface_col, self.cfg.condition_col]
        return df.merge(self.t_baseline_table_, on=key_cols, how="left")

    def transform_t_index(self, df: pd.DataFrame) -> pd.DataFrame:
        merged = self._merge_baseline(df.copy())
        z = (merged["time_mean_shrunk"] - merged[self.cfg.finish_time_col]) / merged["time_std_shrunk"]
        merged["t_index"] = (50 + 10 * z).clip(20, 80)
        return merged

    def _prepare_features_for_agari(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        d = df.copy()
        d["distance_bin"] = pd.cut(d[self.cfg.distance_col], bins=[0, 1400, 1800, 2200, 4000], labels=["sprint", "mile", "middle", "stayer"])
        d["position_rate"] = d[self.cfg.position_col] / d.groupby(self.cfg.race_id_col)[self.cfg.horse_id_col].transform("count").clip(lower=1)
        d["early_load_proxy"] = d[self.cfg.pace_col] * np.log1p(d[self.cfg.distance_col].clip(lower=1000) / 1000)
        features = [
            self.cfg.distance_col, self.cfg.pace_col, "position_rate", "early_load_proxy",
            self.cfg.surface_col, self.cfg.condition_col, self.cfg.venue_col,
            "distance_bin", self.cfg.class_col, self.cfg.course_direction_col, self.cfg.is_outer_col,
        ]
        X = d[features]
        y = d[self.cfg.last3f_col]
        return X, y

    def fit_agari_model(self, df: pd.DataFrame) -> "RaceIndexModeler":
        work = df.dropna(subset=[self.cfg.last3f_col, self.cfg.pace_col, self.cfg.position_col]).copy()
        X, y = self._prepare_features_for_agari(work)
        num_cols = [self.cfg.distance_col, self.cfg.pace_col, "position_rate", "early_load_proxy"]
        cat_cols = [c for c in X.columns if c not in num_cols]
        pre = ColumnTransformer([
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())]), num_cols),
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")), ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
        ])
        model = Pipeline([("pre", pre), ("reg", HuberRegressor(alpha=0.0005, epsilon=1.35, max_iter=2000))])
        model.fit(X, y)
        self.agari_model_ = model
        self._agari_features_ = X.columns.tolist()
        return self

    def transform_agari_index(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.agari_model_ is None:
            raise RuntimeError("fit_agari_model must be called first")
        out = df.copy()
        X, _ = self._prepare_features_for_agari(out)
        out["expected_last3f"] = self.agari_model_.predict(X)
        out["agari_gap"] = out["expected_last3f"] - out[self.cfg.last3f_col]
        out["agari_index"] = (50 + 12 * out["agari_gap"]).clip(20, 80)
        return out

    def _prepare_features_for_tenkai(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        d = df.copy()
        d["position_rate"] = d[self.cfg.position_col] / d.groupby(self.cfg.race_id_col)[self.cfg.horse_id_col].transform("count").clip(lower=1)
        d["pace_x_pos"] = d[self.cfg.pace_col] * d["position_rate"]
        margin_series = d[self.cfg.margin_col] if self.cfg.margin_col in d.columns else d[self.cfg.margin_seconds_col]
        d["margin_per_km"] = pd.to_numeric(margin_series, errors="coerce").fillna(0.0) / (d[self.cfg.distance_col].clip(lower=1000) / 1000)
        y = d["t_index"]
        features = [
            self.cfg.pace_col, "position_rate", "pace_x_pos", "margin_per_km",
            self.cfg.class_col, self.cfg.surface_col, self.cfg.condition_col, self.cfg.venue_col,
            self.cfg.course_direction_col, self.cfg.is_outer_col,
        ]
        X = d[features]
        return X, y

    def fit_tenkai_model(self, df: pd.DataFrame) -> "RaceIndexModeler":
        work = df.dropna(subset=["t_index", self.cfg.pace_col, self.cfg.position_col]).copy()
        X, y = self._prepare_features_for_tenkai(work)
        num_cols = ["pace_x_pos", self.cfg.pace_col, "position_rate", "margin_per_km"]
        cat_cols = [c for c in X.columns if c not in num_cols]
        pre = ColumnTransformer([
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())]), num_cols),
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")), ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
        ])
        model = Pipeline([("pre", pre), ("reg", Ridge(alpha=2.0))])
        model.fit(X, y)
        self.tenkai_model_ = model
        self._tenkai_features_ = X.columns.tolist()
        return self

    def transform_tenkai_t_index(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.tenkai_model_ is None:
            raise RuntimeError("fit_tenkai_model must be called first")
        out = df.copy()
        X, _ = self._prepare_features_for_tenkai(out)
        out["expected_t_index"] = self.tenkai_model_.predict(X)
        out["tenkai_adj_t_index"] = (0.65 * out["t_index"] + 0.35 * out["expected_t_index"]).clip(20, 80)
        return out

    def fit_all(self, df: pd.DataFrame) -> "RaceIndexModeler":
        self.fit_t_index(df)
        d1 = self.transform_t_index(df)
        self.fit_agari_model(d1)
        self.fit_tenkai_model(d1)
        return self

    def transform_all(self, df: pd.DataFrame) -> pd.DataFrame:
        d = self.transform_t_index(df)
        d = self.transform_agari_index(d)
        d = self.transform_tenkai_t_index(d)
        return d


def debug_data_overview(parquet_path: str, sample_n: int = 5) -> Dict[str, object]:
    df = pd.read_parquet(parquet_path)
    overview = {
        "shape": df.shape,
        "columns": list(df.columns),
        "dtypes": {k: str(v) for k, v in df.dtypes.items()},
        "head": df.head(sample_n).to_dict(orient="records"),
        "describe_numeric": df.describe(include=[np.number]).T.reset_index().head(30).to_dict(orient="records"),
    }
    return _to_jsonable(overview)


def run_pipeline(parquet_path: str, output_path: str, central_only: bool = True) -> None:
    df = pd.read_parquet(parquet_path)
    modeler = RaceIndexModeler()
    modeler.validate_schema(df)

    if central_only and modeler.cfg.central_col in df.columns:
        work = df[df[modeler.cfg.central_col] == 1].copy()
    else:
        work = df.copy()

    if len(work) == 0:
        raise ValueError("No rows available for modeling after filtering (check is_central filter or input data).")

    modeler.fit_all(work)
    out = modeler.transform_all(work)
    out.to_parquet(output_path, index=False)

    eval_df = out[["t_index", "expected_t_index"]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(eval_df) == 0:
        metrics = {
            "rows": int(len(out)),
            "eval_rows": 0,
            "t_index_mae_vs_expected": None,
            "t_index_r2_vs_expected": None,
            "warning": "No valid rows for metric evaluation (NaN/inf after transforms).",
        }
    else:
        metrics = {
            "rows": int(len(out)),
            "eval_rows": int(len(eval_df)),
            "t_index_mae_vs_expected": float(mean_absolute_error(eval_df["t_index"], eval_df["expected_t_index"])),
            "t_index_r2_vs_expected": float(r2_score(eval_df["t_index"], eval_df["expected_t_index"])),
        }
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Race index modeling pipeline")
    parser.add_argument("--parquet", required=True, help="Input parquet file path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument("--overview", action="store_true", help="Print data overview JSON")
    parser.add_argument("--run", action="store_true", help="Run modeling pipeline")
    parser.add_argument("--include-local", action="store_true", help="Include local races")
    args = parser.parse_args()

    if args.overview:
        info = debug_data_overview(args.parquet)
        print(json.dumps(info, ensure_ascii=False, indent=2))
    if args.run:
        run_pipeline(args.parquet, args.output, central_only=not args.include_local)
