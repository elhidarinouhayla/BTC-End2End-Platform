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
    #keeping only num features 
    numeric_cols = [
        c for c, t in df.dtypes
        if t in ["int", "bigint", "double", "float"] and c != "close_t_plus_10"
    ]
    return df, numeric_cols

#asemble features into ML vector
def assemble_features(df, numeric_cols):
    """Combine numeric columns into 'features' vector for ML"""
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    df_ml = assembler.transform(df).select("open_time", "features", "close_t_plus_10")
    return df_ml

#spliting data
def split_data(df_ml, train_ratio=0.8):
    total = df_ml.count()
    train_limit = int(total * train_ratio)
    
    train_data = df_ml.limit(train_limit)
    test_data = df_ml.tail(total - train_limit)  # keeps order
    test_data = spark.createDataFrame(test_data, df_ml.schema)
    
    return train_data, test_data


#training
def train_model(train_data):
    lr = LinearRegression(featuresCol="features", labelCol="close_t_plus_10")
    model = lr.fit(train_data)
    return model

#Evaluation
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

# test to check the pipeline
if __name__ == "__main__":
    spark = start_spark()
    df = load_data(spark, "ml/data/silver/silver_dataset")
    df, numeric_cols = add_features(df)
    df_ml = assemble_features(df, numeric_cols)
    train_data, test_data = split_data(df_ml)
    model = train_model(train_data)
    rmse, mae, preds = evaluate_model(model, test_data)
    model_path = "ml/models/BTC_model"
    save_model(model, model_path)
    print("RMSE:", rmse)
    print("MAE:", mae)
    preds.show(5)
