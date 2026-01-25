from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Chemins de stockage temporaire (utilise le dossier ml/data déjà monté)
TEMP_DATA_PATH = "/opt/airflow/ml/data/temp"
MODEL_PATH = "/opt/airflow/ml/data/models"

# start spark 
def start_spark(app_name="BTC_Prediction"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# new features
def add_features(df):
    window_spec = Window.orderBy("open_time")

    df = df.withColumn(
        "return_1m",
        (F.col("close") - F.lag("close",1).over(window_spec)) /
        F.lag("close",1).over(window_spec)
    )

    df = df.withColumn("MA_5", F.avg("close").over(window_spec.rowsBetween(-4,0)))
    df = df.withColumn("MA_10", F.avg("close").over(window_spec.rowsBetween(-9,0)))

    df = df.withColumn("taker_ratio", F.col("taker_buy_base_volume") / F.col("volume"))

    numeric_cols = [
        c for c, t in df.dtypes
        if t in ["int", "bigint", "double", "float"] and c != "close_t_plus_10"
    ]
    df = df.na.drop(subset=numeric_cols + ["close_t_plus_10"])
    return df, numeric_cols

# features to vector
def assemble_features(df, numeric_cols):
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    return assembler.transform(df).select("open_time", "features", "close_t_plus_10")

# split 
def split_data(df_ml, train_ratio=0.8):
    total = df_ml.count()
    train_limit = int(total * train_ratio)

    window = Window.orderBy("open_time")
    df_ml = df_ml.withColumn("rn", F.row_number().over(window))

    train_data = (
        df_ml.filter(F.col("rn") <= train_limit)
             .drop("rn", "open_time")
    )
    test_data = (
        df_ml.filter(F.col("rn") > train_limit)
             .drop("rn", "open_time")
    )

    return train_data, test_data

# train
def train_model(train_data):
    lr = LinearRegression(featuresCol="features", labelCol="close_t_plus_10")
    return lr.fit(train_data)

# metriques d'evaluation
def evaluate_model(model, test_data):
    predictions = model.transform(test_data)
    evaluator_rmse = RegressionEvaluator(
        labelCol="close_t_plus_10",
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="close_t_plus_10",
        predictionCol="prediction",
        metricName="mae"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    mae  = evaluator_mae.evaluate(predictions)

    return rmse, mae, predictions

def save_model(model, path):
    model.write().overwrite().save(path)


@dag(
    dag_id="airflow_taskflow_example",
    start_date=datetime(2026, 1, 21),
    schedule_interval="@daily",
    catchup=False,
)
def btc_ml_dag():

    @task()
    def start():
        import os
        # Créer les dossiers nécessaires s'ils n'existent pas
        os.makedirs(TEMP_DATA_PATH, exist_ok=True)
        os.makedirs(MODEL_PATH, exist_ok=True)
        return True

    @task()
    def load():
        spark = start_spark()
        df = spark.read.parquet("/opt/airflow/ml/data/silver/silver_dataset")
        # Sauvegarder le DataFrame et retourner le chemin
        temp_path = f"{TEMP_DATA_PATH}/loaded_data"
        df.write.mode("overwrite").parquet(temp_path)
        return temp_path

    @task()
    def new_features(df_path):
        spark = start_spark()
        df_silver = spark.read.parquet(df_path)
        
        df, numeric_cols = add_features(df_silver)
        
        # Sauvegarder le DataFrame
        temp_path = f"{TEMP_DATA_PATH}/features_data"
        df.write.mode("overwrite").parquet(temp_path)
        
        return {
            "df_path": temp_path,
            "numeric_cols": numeric_cols
        }

    @task()
    def features_vector(features_dict):
        spark = start_spark()
        df = spark.read.parquet(features_dict["df_path"])
        
        df_ml = assemble_features(df, features_dict["numeric_cols"])
        
        # Sauvegarder le DataFrame
        temp_path = f"{TEMP_DATA_PATH}/vector_data"
        df_ml.write.mode("overwrite").parquet(temp_path)
        
        return temp_path

    @task()
    def split(df_ml_path):
        spark = start_spark()
        df_ml = spark.read.parquet(df_ml_path)
        
        train_data, test_data = split_data(df_ml)
        
        # Sauvegarder les deux DataFrames
        train_path = f"{TEMP_DATA_PATH}/train_data"
        test_path = f"{TEMP_DATA_PATH}/test_data"
        
        train_data.write.mode("overwrite").parquet(train_path)
        test_data.write.mode("overwrite").parquet(test_path)
        
        return {
            "train_path": train_path,
            "test_path": test_path
        }

    @task()
    def training(split_result):
        spark = start_spark()
        train_data = spark.read.parquet(split_result["train_path"])
        
        model = train_model(train_data)
        
        # Sauvegarder le modèle temporairement
        temp_model_path = f"{TEMP_DATA_PATH}/temp_model"
        model.write().overwrite().save(temp_model_path)
        
        return {
            "model_path": temp_model_path,
            "test_path": split_result["test_path"]
        }

    @task()
    def metrics(training_result):
        spark = start_spark()
        
        # Charger le modèle et les données de test
        from pyspark.ml.regression import LinearRegressionModel
        model = LinearRegressionModel.load(training_result["model_path"])
        test_data = spark.read.parquet(training_result["test_path"])
        
        rmse, mae, predictions = evaluate_model(model, test_data)
        
        print(f"RMSE: {rmse}")
        print(f"MAE: {mae}")
        
        return {
            "rmse": rmse,
            "mae": mae,
            "model_path": training_result["model_path"]
        }

    @task()
    def save(metrics_result):
        spark = start_spark()  # Initialiser Spark
        from pyspark.ml.regression import LinearRegressionModel
        model = LinearRegressionModel.load(metrics_result["model_path"])
        save_model(model, f"{MODEL_PATH}/BTC_model")
        
        return {
            "rmse": metrics_result["rmse"],
            "mae": metrics_result["mae"],
            "model_saved": True
        }

    
    start()
    df_path = load()
    features = new_features(df_path)
    df_ml_path = features_vector(features)

    split_result = split(df_ml_path)
    training_result = training(split_result)

    metrics_result = metrics(training_result)
    save(metrics_result)

btc_ml_dag()