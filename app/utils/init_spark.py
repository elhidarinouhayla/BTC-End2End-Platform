from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *




def init_spark():

    spark = SparkSession.builder \
        .appName("PostgresFinalTest") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    return spark