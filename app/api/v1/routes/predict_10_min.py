from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from app.utils.init_spark import init_spark
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
import os
from app.api.v1.schemas.prediction import PredictionRequest, PredictionResponse
from app.api.v1.dependencies import get_db
from app.db.models.user import User
from app.db.models.prediction import Prediction
from app.services.auth_services import verify_token

predict_router = APIRouter(prefix="/predict", tags=["Prediction"])

# Initialiser Spark
spark = init_spark()

# Chemin du modèle
MODEL_PATH = os.path.join(os.getcwd(), "ml/models/BTC_model")

# Charger le modèle au démarrage
try:
    loaded_model = LinearRegressionModel.load(MODEL_PATH)
    print("✅ Modèle LinearRegression chargé avec succès")
except Exception as e:
    print(f"❌ Erreur de chargement du modèle: {e}")
    loaded_model = None


@predict_router.post("/latest", response_model=PredictionResponse)
async def get_latest_prediction(
    data: PredictionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(verify_token)
):
    if loaded_model is None:
        raise HTTPException(503, "Modèle non disponible")
    
    try:
        current_time = datetime.utcnow()
        
        SILVER_PATH = os.path.join(os.getcwd(), "ml/data/silver/silver_dataset")
        
        if not os.path.exists(SILVER_PATH):
            raise HTTPException(503, "Données historiques non disponibles")
        
        historical_df = spark.read.parquet(SILVER_PATH)
        
        # Charger 10 bougies pour calculer MA_10 et return_1m
        last_10 = historical_df.select(
            F.unix_timestamp("open_time").cast("bigint").alias("open_time"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.unix_timestamp("close_time").cast("bigint").alias("close_time"),
            F.col("quote_asset_volume"),
            F.col("number_of_trades"),
            F.col("taker_buy_base_volume"),
            F.col("taker_buy_quote_volume")
        ).orderBy("open_time", ascending=False).limit(10)
        
        print(f"📊 Chargement de {last_10.count()} bougies historiques")
        
        # Créer la nouvelle bougie
        new_row = Row(
            open_time=int(current_time.timestamp()),
            open=float(data.open_price),
            high=float(data.high_price),
            low=float(data.low_price),
            close=float(data.close_price),
            volume=float(data.volume),
            close_time=int(current_time.timestamp()) + 60,
            quote_asset_volume=float(data.quote_asset_volume),
            number_of_trades=int(data.number_of_trades),
            taker_buy_base_volume=float(data.taker_buy_base_volume),
            taker_buy_quote_volume=float(data.taker_buy_quote_volume)
        )
        
        new_df = spark.createDataFrame([new_row])
        full_df = last_10.union(new_df).orderBy("open_time")
        
        print(f"📊 Fenêtre complète: {full_df.count()} bougies")
        
        # Calculer les features dérivées
        window = Window.orderBy("open_time")
        
        # return_1m
        full_df = full_df.withColumn(
            "return_1m",
            (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window)
        )
        
        # MA_5
        window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
        full_df = full_df.withColumn("MA_5", F.avg("close").over(window_5))
        
        # MA_10
        window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)
        full_df = full_df.withColumn("MA_10", F.avg("close").over(window_10))
        
        # taker_ratio
        full_df = full_df.withColumn(
            "taker_ratio",
            F.when(F.col("volume") > 0, 
                   F.col("taker_buy_base_volume") / F.col("volume")
            ).otherwise(0.0)
        )
        

        numeric_cols = [
            'open', 'high', 'low', 'close', 'volume',
            'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_volume', 'taker_buy_quote_volume',
            'return_1m', 'MA_5', 'MA_10', 'taker_ratio'
        ]
        
        print(f"📋 Utilisation de {len(numeric_cols)} features (ordre d'entraînement)")
        

        assembler = VectorAssembler(
            inputCols=numeric_cols,
            outputCol="features",
            handleInvalid="skip"
        )
        
        features_df = assembler.transform(full_df)
        
        last_row_df = features_df.orderBy("open_time", ascending=False).limit(1)
        
        feature_row = last_row_df.select(*numeric_cols, "features").first()
        
        if feature_row is None or feature_row.features is None:
            raise HTTPException(500, "Features invalides")
        
        print(f"\n📊 Features calculées pour la prédiction:")
        for i, col in enumerate(numeric_cols, 1):
            val = getattr(feature_row, col)
            print(f"  {i:2d}. {col:25s} = {val}")
        
        print(f"\n✅ Taille du vecteur: {len(feature_row.features)} (attendu: 13)")
        

        prediction_df = loaded_model.transform(last_row_df)
        prediction_row = prediction_df.select("prediction").first()
        
        if not prediction_row:
            raise HTTPException(500, "Impossible de générer une prédiction")
        
        predicted_price = float(prediction_row.prediction)
        
        print(f"\n🎯 Prédiction: {predicted_price:.2f} USD")
        print(f"📊 Prix actuel: {data.close_price:.2f} USD")
        print(f"📈 Variation prédite: {((predicted_price - data.close_price) / data.close_price * 100):.3f}%\n")
        

        try:
            user_id = current_user.get("sub")
            
            new_prediction = Prediction(
                user_id=user_id,
                timestamp=current_time,
                prediction_for=current_time + timedelta(minutes=10),
                current_price=data.close_price,
                predicted_price=predicted_price,
                model_version="v1.0.0"
            )
            
            db.add(new_prediction)
            db.commit()
            db.refresh(new_prediction)
            
            print(f"✅ Prédiction sauvegardée avec ID: {new_prediction.id}")
            
        except Exception as db_error:
            db.rollback()
            print(f"❌ Erreur DB: {db_error}")
            raise HTTPException(500, f"Erreur de sauvegarde: {str(db_error)}")
        
        return PredictionResponse(
            user_id=user_id,
            timestamp=current_time,
            current_price=data.close_price,
            predicted_price=predicted_price,
            prediction_for=current_time + timedelta(minutes=10)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Erreur de prédiction: {str(e)}")