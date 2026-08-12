"""
Distributed ML Data Pipeline & Model Serving
=============================================
FastAPI inference server: serves ML model predictions with
health checks, versioning, and structured request/response schemas.
"""

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
import logging
import time
import os

from model_loader import ModelLoader, ModelVersion

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="ML Model Inference API",
    description="Real-time inference endpoint for distributed ML pipeline models.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class PredictionRequest(BaseModel):
    """Single-instance prediction request."""
    features: Dict[str, Any] = Field(..., description="Feature name → value mapping")
    model_version: Optional[str] = Field(None, description="Model version to use (default: latest)")
    return_proba: bool = Field(True, description="Return probability scores (classification only)")

    class Config:
        schema_extra = {
            "example": {
                "features": {
                    "user_id": 1042,
                    "ad_category": "technology",
                    "device_type": "mobile",
                    "hour_of_day": 14,
                    "historical_ctr_user": 0.08,
                    "historical_ctr_ad": 0.12,
                },
                "model_version": "v1.3.0",
                "return_proba": True,
            }
        }


class BatchPredictionRequest(BaseModel):
    """Batch prediction request (up to 1000 instances)."""
    instances: List[Dict[str, Any]] = Field(..., description="List of feature dicts")
    model_version: Optional[str] = None

    @validator("instances")
    def validate_batch_size(cls, v):
        if len(v) > 1000:
            raise ValueError("Batch size must not exceed 1000 instances.")
        return v


class PredictionResponse(BaseModel):
    prediction: Any
    probability: Optional[float] = None
    model_version: str
    latency_ms: float


class BatchPredictionResponse(BaseModel):
    predictions: List[Any]
    probabilities: Optional[List[float]] = None
    model_version: str
    latency_ms: float
    batch_size: int


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_version: Optional[str]
    uptime_seconds: float


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_startup_time = time.time()
_model_loader: Optional[ModelLoader] = None


def get_model_loader() -> ModelLoader:
    if _model_loader is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Service initializing.",
        )
    return _model_loader


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    global _model_loader
    logger.info("Loading ML model...")
    model_dir = os.environ.get("MODEL_DIR", "models/")
    _model_loader = ModelLoader(model_dir=model_dir)
    _model_loader.load_latest()
    logger.info(f"Model loaded: version={_model_loader.current_version}")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
async def health_check():
    """Kubernetes liveness/readiness probe endpoint."""
    return HealthResponse(
        status="healthy",
        model_loaded=_model_loader is not None,
        model_version=_model_loader.current_version if _model_loader else None,
        uptime_seconds=round(time.time() - _startup_time, 2),
    )


@app.post("/predict", response_model=PredictionResponse, tags=["Inference"])
async def predict(
    request: PredictionRequest,
    loader: ModelLoader = Depends(get_model_loader),
):
    """Single-instance prediction endpoint."""
    t0 = time.time()
    try:
        result = loader.predict(request.features, version=request.model_version)
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return PredictionResponse(
        prediction=result["label"],
        probability=result.get("probability"),
        model_version=result["model_version"],
        latency_ms=round((time.time() - t0) * 1000, 2),
    )


@app.post("/predict/batch", response_model=BatchPredictionResponse, tags=["Inference"])
async def predict_batch(
    request: BatchPredictionRequest,
    loader: ModelLoader = Depends(get_model_loader),
):
    """Batch prediction endpoint (up to 1000 instances)."""
    t0 = time.time()
    try:
        results = loader.predict_batch(request.instances, version=request.model_version)
    except Exception as e:
        logger.error(f"Batch prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return BatchPredictionResponse(
        predictions=[r["label"] for r in results],
        probabilities=[r.get("probability") for r in results],
        model_version=results[0]["model_version"] if results else "unknown",
        latency_ms=round((time.time() - t0) * 1000, 2),
        batch_size=len(results),
    )


@app.get("/models", tags=["Model Management"])
async def list_models(loader: ModelLoader = Depends(get_model_loader)):
    """List all available model versions in the model registry."""
    return {"versions": loader.list_versions(), "current": loader.current_version}
