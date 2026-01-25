from pyspark.sql import DataFrame, SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime
from fastapi import HTTPException
import os

NUMERIC_COLS = [
    'open', 'high', 'low', 'close', 'volume',
    'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_volume', 'taker_buy_quote_volume',
    'return_1m', 'MA_5', 'MA_10', 'taker_ratio'
]


def load_historical_data(spark: SparkSession, silver_path: str) -> DataFrame:
    """Charge les 10 dernières bougies historiques."""
    if not os.path.exists(silver_path):
        raise HTTPException(503, "Données historiques non disponibles")
    
    historical_df = spark.read.parquet(silver_path)
    
    return historical_df.select(
        F.unix_timestamp("open_time").cast("bigint").alias("open_time"),
        F.col("open"), F.col("high"), F.col("low"), F.col("close"), F.col("volume"),
        F.unix_timestamp("close_time").cast("bigint").alias("close_time"),
        F.col("quote_asset_volume"), F.col("number_of_trades"),
        F.col("taker_buy_base_volume"), F.col("taker_buy_quote_volume")
    ).orderBy("open_time", ascending=False).limit(10)


def create_new_candle(spark: SparkSession, data, current_time: datetime) -> DataFrame:
    """Crée une nouvelle bougie à partir des données reçues."""
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
    return spark.createDataFrame([new_row])


def compute_features(df: DataFrame) -> DataFrame:
    """Calcule toutes les features dérivées (return_1m, MA_5, MA_10, taker_ratio)."""
    window = Window.orderBy("open_time")
    
    # return_1m = (close(t) - close(t-1)) / close(t-1)
    df = df.withColumn(
        "return_1m",
        (F.col("close") - F.lag("close", 1).over(window)) / F.lag("close", 1).over(window)
    )
    
    # MA_5 = moyenne des 5 dernières closes
    window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
    df = df.withColumn("MA_5", F.avg("close").over(window_5))
    
    # MA_10 = moyenne des 10 dernières closes
    window_10 = Window.orderBy("open_time").rowsBetween(-9, 0)
    df = df.withColumn("MA_10", F.avg("close").over(window_10))
    
    # taker_ratio = taker_buy_base_volume / volume
    df = df.withColumn(
        "taker_ratio",
        F.when(F.col("volume") > 0, F.col("taker_buy_base_volume") / F.col("volume")).otherwise(0.0)
    )
    
    return df


def prepare_features(df: DataFrame) -> DataFrame:
    """Assemble les features en vecteur pour la prédiction."""
    assembler = VectorAssembler(
        inputCols=NUMERIC_COLS,
        outputCol="features",
        handleInvalid="skip"
    )
    
    features_df = assembler.transform(df)
    last_row = features_df.orderBy("open_time", ascending=False).limit(1)
    
    feature_row = last_row.select(*NUMERIC_COLS, "features").first()
    
    if feature_row is None or feature_row.features is None:
        raise HTTPException(500, "Features invalides")
    
    log_features(feature_row)
    return last_row


def make_prediction(model: LinearRegressionModel, features_df: DataFrame, current_price: float) -> dict:
    """Génère la prédiction avec intervalles de confiance."""
    prediction_df = model.transform(features_df)
    prediction_row = prediction_df.select("prediction").first()
    
    if not prediction_row:
        raise HTTPException(500, "Impossible de générer une prédiction")
    
    predicted_price = float(prediction_row.prediction)
    
    # Calculer l'intervalle de confiance basé sur la volatilité récente
    # Estimation: ±2% pour un intervalle de confiance à 95%
    volatility = abs(predicted_price - current_price) / current_price
    confidence_margin = predicted_price * max(0.02, volatility * 2)  # Au minimum 2%
    
    return {
        "predicted_price": predicted_price,
        "confidence_lower": predicted_price - confidence_margin,
        "confidence_upper": predicted_price + confidence_margin
    }


def log_features(feature_row):
    """Affiche les features calculées pour debug."""
    print(f"\n Features calculées:")
    for i, col in enumerate(NUMERIC_COLS, 1):
        val = getattr(feature_row, col)
        print(f"  {i:2d}. {col:25s} = {val}")
    print(f"\n Taille du vecteur: {len(feature_row.features)}")


def log_prediction_results(predicted_price: float, current_price: float):
    """Affiche les résultats de la prédiction."""
    variation = ((predicted_price - current_price) / current_price * 100)
    print(f"\n Prédiction T+10: {predicted_price:.2f} USD")
    print(f" Prix actuel (T): {current_price:.2f} USD")
    print(f" Variation prédite: {variation:.3f}%\n")


def predict_next_price(
    spark: SparkSession,
    model: LinearRegressionModel,
    silver_path: str,
    data,
    current_time: datetime
) -> dict:
    """Pipeline complet de prédiction du prix à T+10 minutes."""
    # 1. Charger historique
    historical_df = load_historical_data(spark, silver_path)
    print(f" Chargement de {historical_df.count()} bougies historiques")
    
    # 2. Créer nouvelle bougie (T)
    new_candle = create_new_candle(spark, data, current_time)
    full_df = historical_df.union(new_candle).orderBy("open_time")
    print(f" Fenêtre complète: {full_df.count()} bougies")
    
    # 3. Calculer features (return_1m, MA_5, MA_10, taker_ratio)
    full_df = compute_features(full_df)
    
    # 4. Préparer pour prédiction
    features_df = prepare_features(full_df)
    
    # 5. Prédire le prix à T+10 minutes
    prediction_result = make_prediction(model, features_df, data.close_price)
    
    # 6. Logger résultats
    log_prediction_results(prediction_result["predicted_price"], data.close_price)
    
    return prediction_result
