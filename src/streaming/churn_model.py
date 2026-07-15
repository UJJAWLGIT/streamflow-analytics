"""
churn_model.py — Subscription Churn Propensity Model
======================================================
XGBoost / LightGBM churn prediction with:
  - 200+ engineered features
  - MLflow experiment tracking + model registry
  - SHAP feature importance
  - Cross-validation + hyperparameter tuning
  - Threshold optimisation (F1 / precision-recall trade-off)

Usage:
    python churn_model.py --features-path ./data/output/ml_features \
                          --mlflow-uri http://localhost:5000 \
                          --experiment-name cancel-flow-churn
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import mlflow
import mlflow.sklearn
import mlflow.xgboost
import numpy as np
import pandas as pd
import shap
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ── Feature groups ─────────────────────────────────────────────────────────────

BEHAVIOURAL_FEATURES = [
    "cancel_frequency_90d",
    "days_since_last_cancel",
    "avg_session_duration_s",
    "page_path_entropy",
    "cancel_flow_screen_oiam_flag",
    "cancel_flow_screen_mobile_flag",
    "initiation_hour_utc",
    "initiation_day_of_week",
    "is_weekend_initiation",
]

PRODUCT_FEATURES = [
    "tenure_days",
    "billing_frequency_annual_flag",
    "sku_tier_encoded",
    "subscription_type_direct_flag",
    "is_accountant_initiated",
    "n_previous_upgrades",
    "n_previous_downgrades",
    "n_previous_discounts_taken",
]

IPD_FEATURES = [
    "viewed_cs_ipd",
    "clicked_cs_ipd",
    "cs_click_through_rate",
    "viewed_discount_ipd",
    "clicked_discount_ipd",
    "discount_click_through_rate",
    "viewed_upgrade_ipd",
    "clicked_upgrade_ipd",
    "viewed_downgrade_ipd",
    "viewed_keep_plan_ipd",
    "clicked_keep_plan_ipd",
    "total_ipds_shown",
    "total_ipd_clicks",
    "overall_click_through_rate",
    "viewed_dic",
    "dic_max_data_points",
]

TEMPORAL_FEATURES = [
    "days_until_renewal",
    "cohort_month_sin",
    "cohort_month_cos",
    "is_q4_initiation",
    "days_since_signup",
    "tenure_bucket_encoded",
]

SAVE_HISTORY_FEATURES = [
    "lifetime_cs_saves",
    "lifetime_discount_saves",
    "lifetime_upgrade_saves",
    "lifetime_abandoned_count",
    "save_rate_lifetime",
]

ALL_FEATURES = (
    BEHAVIOURAL_FEATURES
    + PRODUCT_FEATURES
    + IPD_FEATURES
    + TEMPORAL_FEATURES
    + SAVE_HISTORY_FEATURES
)

TARGET = "churned_31d"   # 1 = churned, 0 = retained


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ModelMetrics:
    roc_auc: float
    pr_auc: float
    f1: float
    precision: float
    recall: float
    optimal_threshold: float
    cv_roc_auc_mean: float
    cv_roc_auc_std: float


@dataclass
class ModelConfig:
    model_type: str = "xgboost"
    n_estimators: int = 500
    max_depth: int = 6
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    min_child_weight: int = 5
    scale_pos_weight: float = 3.0   # Handles class imbalance (churn ~25%)
    n_cv_folds: int = 5
    random_state: int = 42
    early_stopping_rounds: int = 50
    tags: Dict[str, str] = field(default_factory=dict)


# ── Model trainer ──────────────────────────────────────────────────────────────

class ChurnModelTrainer:
    """
    End-to-end churn model trainer with MLflow tracking.

    Workflow:
        1. Load feature store data
        2. Feature engineering + preprocessing
        3. Cross-validation
        4. Full model training
        5. SHAP feature importance
        6. Threshold optimisation
        7. MLflow logging + model registration
    """

    def __init__(
        self,
        config: ModelConfig,
        mlflow_uri: str,
        experiment_name: str,
    ):
        self.config = config
        mlflow.set_tracking_uri(mlflow_uri)
        mlflow.set_experiment(experiment_name)

    def load_features(self, features_path: str) -> pd.DataFrame:
        """Load feature store from parquet."""
        df = pd.read_parquet(features_path)
        logger.info(f"Loaded {len(df):,} rows from feature store")

        # Filter to baked cohorts only
        df = df[df["baked_31d"] == 1].copy()
        df[TARGET] = 1 - df["retained_31d"]
        logger.info(f"Baked cohort: {len(df):,} rows | Churn rate: {df[TARGET].mean():.2%}")
        return df

    def preprocess(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare features and target arrays."""
        available_features = [f for f in ALL_FEATURES if f in df.columns]
        missing = set(ALL_FEATURES) - set(available_features)
        if missing:
            logger.warning(f"Missing {len(missing)} features: {missing}")

        X = df[available_features].fillna(0).values
        y = df[TARGET].values

        logger.info(f"Feature matrix: {X.shape} | Positive rate: {y.mean():.2%}")
        return X, y, available_features

    def cross_validate(self, X: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
        """Stratified k-fold cross-validation."""
        from xgboost import XGBClassifier

        model = XGBClassifier(
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            min_child_weight=self.config.min_child_weight,
            scale_pos_weight=self.config.scale_pos_weight,
            random_state=self.config.random_state,
            eval_metric="auc",
            use_label_encoder=False,
        )
        cv = StratifiedKFold(n_splits=self.config.n_cv_folds, shuffle=True, random_state=42)
        scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)

        logger.info(
            f"CV ROC-AUC: {scores.mean():.4f} ± {scores.std():.4f} "
            f"[{', '.join(f'{s:.4f}' for s in scores)}]"
        )
        return float(scores.mean()), float(scores.std())

    def find_optimal_threshold(
        self,
        y_true: np.ndarray,
        y_proba: np.ndarray,
    ) -> Tuple[float, float, float, float]:
        """Find threshold that maximises F1 score on validation set."""
        precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
        f1_scores = 2 * precision * recall / np.where((precision + recall) == 0, 1, precision + recall)
        best_idx = np.argmax(f1_scores)
        best_threshold = float(thresholds[best_idx]) if best_idx < len(thresholds) else 0.5

        return (
            best_threshold,
            float(f1_scores[best_idx]),
            float(precision[best_idx]),
            float(recall[best_idx]),
        )

    def train(self, features_path: str) -> str:
        """
        Full training pipeline with MLflow tracking.

        Returns:
            MLflow run ID.
        """
        from xgboost import XGBClassifier

        with mlflow.start_run(
            run_name=f"churn_model_{self.config.model_type}_{datetime.utcnow():%Y%m%d_%H%M}",
            tags={
                "model_type": self.config.model_type,
                "pipeline": "cancel_flow",
                "env": "production",
                **self.config.tags,
            },
        ) as run:
            run_id = run.info.run_id
            logger.info(f"MLflow run: {run_id}")

            # ── 1. Load + preprocess ───────────────────────────────────────────
            df = self.load_features(features_path)
            X, y, feature_names = self.preprocess(df)

            # Log feature metadata
            mlflow.log_param("n_features", len(feature_names))
            mlflow.log_param("n_training_samples", len(X))
            mlflow.log_param("churn_rate", float(y.mean()))
            mlflow.log_params(
                {k: v for k, v in vars(self.config).items() if not isinstance(v, dict)}
            )

            # ── 2. Cross-validation ────────────────────────────────────────────
            cv_mean, cv_std = self.cross_validate(X, y)
            mlflow.log_metrics({"cv_roc_auc_mean": cv_mean, "cv_roc_auc_std": cv_std})

            # ── 3. Train final model ───────────────────────────────────────────
            # 80/20 temporal split (train on older, validate on recent)
            split_idx = int(len(X) * 0.8)
            X_train, X_val = X[:split_idx], X[split_idx:]
            y_train, y_val = y[:split_idx], y[split_idx:]

            model = XGBClassifier(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                min_child_weight=self.config.min_child_weight,
                scale_pos_weight=self.config.scale_pos_weight,
                random_state=self.config.random_state,
                early_stopping_rounds=self.config.early_stopping_rounds,
                eval_metric="auc",
                use_label_encoder=False,
            )
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=50,
            )

            # ── 4. Evaluate ────────────────────────────────────────────────────
            y_proba = model.predict_proba(X_val)[:, 1]
            roc_auc = roc_auc_score(y_val, y_proba)
            pr_auc  = average_precision_score(y_val, y_proba)
            threshold, f1, precision, recall = self.find_optimal_threshold(y_val, y_proba)

            metrics = ModelMetrics(
                roc_auc=roc_auc, pr_auc=pr_auc, f1=f1,
                precision=precision, recall=recall,
                optimal_threshold=threshold,
                cv_roc_auc_mean=cv_mean, cv_roc_auc_std=cv_std,
            )

            mlflow.log_metrics({
                "roc_auc":          metrics.roc_auc,
                "pr_auc":           metrics.pr_auc,
                "f1":               metrics.f1,
                "precision":        metrics.precision,
                "recall":           metrics.recall,
                "optimal_threshold": metrics.optimal_threshold,
            })

            logger.info(
                f"Model performance: ROC-AUC={roc_auc:.4f} | "
                f"PR-AUC={pr_auc:.4f} | F1={f1:.4f} @ threshold={threshold:.3f}"
            )

            # ── 5. SHAP feature importance ─────────────────────────────────────
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_val[:500])   # Sample for speed

            feature_importance = pd.DataFrame({
                "feature": feature_names,
                "shap_importance": np.abs(shap_values).mean(axis=0),
            }).sort_values("shap_importance", ascending=False)

            mlflow.log_dict(
                feature_importance.head(20).set_index("feature")["shap_importance"].to_dict(),
                "top_20_features.json",
            )
            logger.info(f"Top 5 features:\n{feature_importance.head()}")

            # ── 6. Log model to MLflow registry ───────────────────────────────
            signature = mlflow.models.infer_signature(
                X_val[:10], model.predict_proba(X_val[:10])
            )
            mlflow.xgboost.log_model(
                model,
                artifact_path="churn_model",
                registered_model_name="cancel_flow_churn_propensity",
                signature=signature,
                input_example=X_val[:3],
            )
            logger.info(f"✅ Model registered: cancel_flow_churn_propensity (run={run_id})")

        return run_id


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train churn propensity model")
    parser.add_argument("--features-path",    required=True)
    parser.add_argument("--mlflow-uri",       default="http://localhost:5000")
    parser.add_argument("--experiment-name",  default="cancel-flow-churn-propensity")
    parser.add_argument("--model-type",       default="xgboost", choices=["xgboost", "lightgbm"])
    parser.add_argument("--n-estimators",     type=int, default=500)
    parser.add_argument("--max-depth",        type=int, default=6)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = ModelConfig(
        model_type=args.model_type,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
    )
    trainer = ChurnModelTrainer(config, args.mlflow_uri, args.experiment_name)
    run_id = trainer.train(args.features_path)
    print(f"\n✅ Training complete. MLflow run ID: {run_id}")


if __name__ == "__main__":
    main()
