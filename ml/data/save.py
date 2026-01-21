from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession.builder \
    .appName("LoadSilverDataToPostgres") \
    .config("spark.jars", "/home/hp/spark_libs/postgresql-42.6.0.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

print(":white_check_mark: Spark session créée avec succès")


jdbc_url = "jdbc:postgresql://" \
"localhost:5432/silver_data"
connection_properties = {
    "user": "silver_user",
    "password": "silver_pass123",
    "driver": "org.postgresql.Driver"
}


folder_path = "/mnt/c/Users/hp/desktop/BTC-End2End-Platform/ml/data/silver/silver_dataset"

df = spark.read.parquet(folder_path)

print(f":bar_chart: Données chargées: {df.count()} lignes")
df.show(5)


table_name = "silver_table" 
df.write \
    .mode("overwrite") \
    .option("batchsize", "50000") \
    .option("numPartitions", "10") \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

print(f":white_check_mark: Données écrites dans PostgreSQL dans la table '{table_name}'")


df_read = spark.read \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

print(f":bar_chart: Nombre de lignes dans PostgreSQL: {df_read.count()}")
df_read.show(5)

spark.stop()
print(":white_check_mark: Terminé !")