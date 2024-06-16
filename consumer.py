from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Inicializar Spark
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Definir el esquema para los mensajes Kafka entrantes
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("Price", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Leer desde el tema o topico de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Procesar los datos
processed_df = df.withColumn("Revenue", col("Quantity") * col("Price"))

#  Salida de los datos procesados 
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que la consulta de streaming termine
query.awaitTermination()
