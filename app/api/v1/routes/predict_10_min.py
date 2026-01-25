from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.utils.init_spark import init_spark
from pyspark.ml.regression import LinearRegressionModel
import os
from app.api.v1.schemas.prediction import PredictionRequest, PredictionResponse
from app.api.v1.dependencies import get_db
from app.db.models.user import User
from app.services.auth_services import verify_token
from app.services.prediction_service import predict_next_price
from app.services.prediction_db_service import save_prediction

predict_router = APIRouter(prefix="/predict", tags=["Prediction"])

# Configuration
MODEL_PATH = os.path.join(os.getcwd(), "ml/models/BTC_model")
SILVER_PATH = os.path.join(os.getcwd(), "ml/data/silver/silver_dataset")

# Initialisation
spark = init_spark()

try:
    loaded_model = LinearRegressionModel.load(MODEL_PATH)
    print("✅ Modèle LinearRegression chargé avec succès")
except Exception as e:
    print(f" Erreur de chargement du modèle: {e}")
    loaded_model = None


@predict_router.post("/latest", response_model=PredictionResponse)
async def get_latest_prediction(
    data: PredictionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_token)
):
    """
    Génère une prédiction du prix Bitcoin à T+10 minutes.
    
    Le modèle utilise:
    - Prix OHLC actuels
    - Volume et nombre de trades
    - Features calculées: return_1m, MA_5, MA_10, taker_ratio
    
    La prédiction sera validée automatiquement 10 minutes plus tard.
    """
    if loaded_model is None:
        raise HTTPException(503, "Modèle non disponible")
    
    try:
        current_time = datetime.utcnow()
        user_id = current_user.get("sub")
        
        # Prédiction du prix à T+10 minutes avec intervalles de confiance
        prediction_result = predict_next_price(
            spark=spark,
            model=loaded_model,
            silver_path=SILVER_PATH,
            data=data,
            current_time=current_time
        )
        
        # Sauvegarde en DB (actual_price et error seront calculés à T+10)
        save_prediction(
            db=db,
            user_id=user_id,
            current_time=current_time,
            current_price=data.close_price,
            predicted_price=prediction_result["predicted_price"],
            confidence_lower=prediction_result["confidence_lower"],
            confidence_upper=prediction_result["confidence_upper"]
        )
        
        return PredictionResponse(
            user_id=user_id,
            timestamp=current_time,
            current_price=data.close_price,
            predicted_price=prediction_result["predicted_price"],
            prediction_for=current_time + timedelta(minutes=10)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f" Erreur: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Erreur de prédiction: {str(e)}")