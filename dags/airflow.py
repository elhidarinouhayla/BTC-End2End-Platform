from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# start spark 
def start_spark(app_name="BTC_Prediction"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# load 
def load_data(spark, path):
    df = spark.read.parquet(path)
    return df

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
    return     assembler.transform(df).select("open_time", "features", "close_t_plus_10")


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
  
    return  lr.fit(train_data)

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
        return True

    @task()
    def load():
        spark = start_spark()
        return spark.read.parquet("/opt/airflow/ml/data/silver/silver_dataset")        

    @task()
    def new_features(df_silver):
        df, numeric_cols = add_features(df_silver)
        return {
            "df": df,
            "numeric_cols": numeric_cols
        }

    @task()
    def features_vector(features_dict):
        return assemble_features(
            features_dict["df"],
            features_dict["numeric_cols"]
        )


    @task()
    def split(df_ml):
        train_data, test_data = split_data(df_ml)
        return {
            "train": train_data,
            "test": test_data
        }

    @task()
    def training(train_data):
        return train_model(train_data)

    @task()
    def metrics(model, test_data):
        return evaluate_model(model, test_data)

    @task()
    def save(model):
        save_model(model, "/opt/airflow/ml/models/BTC_model")


    
    start()
    df_silver = load()
    features = new_features(df_silver)
    df_ml = features_vector(features)

    split_result = split(df_ml)
    model = training(split_result)

    metrics(model, split_result)
    save(model)

btc_ml_dag()
