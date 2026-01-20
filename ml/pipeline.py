from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

#start spark session
def start_spark(app_name="BTC_Prediction"):
    """Initialize Spark session and set log level to ERROR"""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

#load Silver dataset from a Parquet file
def load_data(spark, path):
    df = spark.read.parquet(path)
    return df

# Adding features
def add_features(df):
   
    window_spec = Window.orderBy("open_time")

    # T+10 target
    df = df.withColumn("close_t_plus_10", F.lead("close", 10).over(window_spec))
    df = df.na.drop(subset=["close_t_plus_10"])

    # 1-minute return
    df = df.withColumn("return_1m", (F.col("close") - F.lag("close",1).over(window_spec)) / F.lag("close",1).over(window_spec))

    # Moving avrg
    df = df.withColumn("MA_5", F.avg("close").over(window_spec.rowsBetween(-4,0)))
    df = df.withColumn("MA_10", F.avg("close").over(window_spec.rowsBetween(-9,0)))

    # taker ratio
    df = df.withColumn("taker_ratio", F.col("taker_buy_base_volume") / F.col("volume"))

    #keeping only num features 
    numeric_cols = [c for c, t in df.dtypes if t in ["int", "bigint", "double", "float"] and c != "close_t_plus_10"]
    df = df.na.drop(subset=numeric_cols + ["close_t_plus_10"])
    return df, numeric_cols

#asemble features into ML vector
def assemble_features(df, numeric_cols):
    """Combine numeric columns into 'features' vector for ML"""
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    df_ml = assembler.transform(df).select("features", "close_t_plus_10")
    return df_ml

#spliting data
def split_data(df_ml, train_ratio=0.8, seed=42):
    train_data, test_data = df_ml.randomSplit([train_ratio, 1-train_ratio], seed=seed)
    return train_data, test_data

#training
def train_model(train_data):
    lr = LinearRegression(featuresCol="features", labelCol="close_t_plus_10")
    model = lr.fit(train_data)
    return model

#Evaluation
def evaluate_model(model, test_data):
    predictions = model.transform(test_data)
    evaluator_rmse = RegressionEvaluator(labelCol="close_t_plus_10", predictionCol="prediction", metricName="rmse")
    evaluator_mae  = RegressionEvaluator(labelCol="close_t_plus_10", predictionCol="prediction", metricName="mae")
    evaluator_r2   = RegressionEvaluator(labelCol="close_t_plus_10", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae  = evaluator_mae.evaluate(predictions)
    r2   = evaluator_r2.evaluate(predictions)

    return rmse, mae, r2, predictions

def save_model(model, path):
    model.write().overwrite().save(path)

# test to check the pipeline
if __name__ == "__main__":
    spark = start_spark()
    df = load_data(spark, "ml/data/silver/silver_dataset")
    df, numeric_cols = add_features(df)
    df_ml = assemble_features(df, numeric_cols)
    train_data, test_data = split_data(df_ml)
    model = train_model(train_data)
    rmse, mae, r2, preds = evaluate_model(model, test_data)
    model_path = "ml/models/BTC_model"
    save_model(model, model_path)
    print("R2:", r2)
    print("RMSE:", rmse)
    print("MAE:", mae)
    preds.show(5)
