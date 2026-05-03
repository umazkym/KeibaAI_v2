"""
Pipeline Session Manager (v5.2)

パイプライン実行のセッション管理を行うクラス。
実験追跡、中間データの管理、実行ログの一元化を提供する。

[6-1改修] clawd-codeのPortRuntimeパターンを参考に、
パイプライン実行のコンテキスト管理を導入。

使用例:
    from keibaai.src.pipeline_session import PipelineSession
    
    with PipelineSession(experiment_name="v15_tuned") as session:
        session.log_params({"n_estimators": 1000, "lr": 0.05})
        
        # 特徴量生成
        features = generate_features(...)
        session.log_artifact("features_shape", features.shape)
        
        # モデル学習
        model = train_model(features, ...)
        session.log_metric("train_rmse", rmse_train)
        session.log_metric("val_rmse", rmse_val)
        
        # 結果保存
        session.save_summary()
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

logger = logging.getLogger(__name__)


class PipelineSession:
    """
    パイプライン実行セッション
    
    実験の追跡、パラメータ/メトリクスの記録、
    中間結果の保存を一元管理する。
    """
    
    def __init__(
        self,
        experiment_name: str = "default",
        output_dir: str = "outputs/experiments",
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Args:
            experiment_name: 実験名（ディレクトリ名にも使用）
            output_dir: 出力ディレクトリ
            tags: 実験タグ（任意のメタデータ）
        """
        self.experiment_name = experiment_name
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        self.output_dir = Path(output_dir) / experiment_name / self.session_id
        self.tags = tags or {}
        
        # セッション状態
        self.params: Dict[str, Any] = {}
        self.metrics: Dict[str, Any] = {}
        self.artifacts: Dict[str, Any] = {}
        self.logs: List[Dict] = []
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self._is_active = False
    
    def __enter__(self):
        """コンテキストマネージャ: セッション開始"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャ: セッション終了"""
        if exc_type is not None:
            self.log_event("error", str(exc_val))
        self.end(success=(exc_type is None))
        return False  # 例外は再送出
    
    def start(self):
        """セッション開始"""
        self.start_time = time.time()
        self._is_active = True
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.log_event("session_start", {
            "experiment_name": self.experiment_name,
            "session_id": self.session_id,
            "tags": self.tags,
        })
        
        logger.info(f"Pipeline Session started: {self.experiment_name} [{self.session_id}]")
    
    def end(self, success: bool = True):
        """セッション終了"""
        self.end_time = time.time()
        self._is_active = False
        
        duration = self.end_time - (self.start_time or self.end_time)
        self.log_event("session_end", {
            "success": success,
            "duration_seconds": duration
        })
        
        # サマリーを自動保存
        self.save_summary()
        
        status = "✅ 成功" if success else "❌ 失敗"
        logger.info(f"Pipeline Session ended: {status} ({duration:.1f}s)")
    
    def log_params(self, params: Dict[str, Any]):
        """パラメータを記録"""
        self.params.update(params)
        self.log_event("params", params)
    
    def log_metric(self, name: str, value: Any, step: Optional[int] = None):
        """メトリクスを記録"""
        entry = {"value": value}
        if step is not None:
            entry["step"] = step
        
        if name not in self.metrics:
            self.metrics[name] = []
        self.metrics[name].append(entry)
        
        logger.info(f"  📊 {name}: {value}")
    
    def log_artifact(self, name: str, value: Any):
        """アーティファクト（中間結果）を記録"""
        self.artifacts[name] = {
            "value": str(value),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def log_event(self, event_type: str, data: Any = None):
        """イベントを記録"""
        self.logs.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "data": data
        })
    
    def save_summary(self):
        """セッションサマリーをJSONで保存"""
        summary = {
            "experiment_name": self.experiment_name,
            "session_id": self.session_id,
            "tags": self.tags,
            "start_time": datetime.fromtimestamp(self.start_time, tz=timezone.utc).isoformat() if self.start_time else None,
            "end_time": datetime.fromtimestamp(self.end_time, tz=timezone.utc).isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time) if (self.start_time and self.end_time) else None,
            "params": self.params,
            "metrics": {k: v[-1]["value"] if v else None for k, v in self.metrics.items()},
            "metrics_history": self.metrics,
            "artifacts": self.artifacts,
        }
        
        summary_path = self.output_dir / "summary.json"
        try:
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"セッションサマリー保存: {summary_path}")
        except Exception as e:
            logger.error(f"サマリー保存失敗: {e}")
        
        # ログも保存
        log_path = self.output_dir / "logs.json"
        try:
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(self.logs, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            logger.error(f"ログ保存失敗: {e}")
    
    def get_latest_metric(self, name: str) -> Any:
        """最新のメトリクス値を取得"""
        if name in self.metrics and self.metrics[name]:
            return self.metrics[name][-1]["value"]
        return None
    
    @staticmethod
    def list_experiments(output_dir: str = "outputs/experiments") -> List[Dict]:
        """過去の実験セッション一覧を取得"""
        experiments = []
        base_dir = Path(output_dir)
        
        if not base_dir.exists():
            return experiments
        
        for exp_dir in sorted(base_dir.iterdir()):
            if not exp_dir.is_dir():
                continue
            for session_dir in sorted(exp_dir.iterdir()):
                summary_path = session_dir / "summary.json"
                if summary_path.exists():
                    try:
                        with open(summary_path, "r", encoding="utf-8") as f:
                            summary = json.load(f)
                        experiments.append(summary)
                    except Exception:
                        pass
        
        return experiments
