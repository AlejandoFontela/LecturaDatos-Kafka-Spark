import json
import datetime as dt
import time
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from concurrent.futures import ThreadPoolExecutor

# Inicialización del cliente Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
print(f'Inicializado el productor Kafka a las {dt.datetime.now()}')

# Establecer la ruta del archivo
file = "online_retail_II.csv"

# Inicialización del contador
counter = 0

# Función para procesar y enviar fragmentos (chunks)
def process_and_send_chunk(chunk):
    global counter  # Usar la variable contador global
    chunk = chunk.withColumn("InvoiceDate", chunk["InvoiceDate"].cast("timestamp"))

    for row in chunk.collect():
        key = str(counter).encode()  # Clave como string codificada en bytes
        data = json.dumps(row.asDict(), default=str).encode('utf-8')  # Datos convertidos a JSON y codificados en UTF-8

        producer.send(topic="transactions", key=key, value=data)  # Enviar al tema "transactions" de Kafka
        print(f'Registro enviado {counter} al tema a las {dt.datetime.now()}')

        counter += 1

# Iniciar temporizador
start_time = time.time()

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Leer el archivo CSV en un DataFrame de Spark
df = spark.read.csv(file, header=True, inferSchema=True, timestampFormat="yyyy-MM-dd HH:mm:ss")

# Usar ThreadPoolExecutor para procesamiento en paralelo
with ThreadPoolExecutor(max_workers=4) as executor:
    for chunk in df.rdd.randomSplit([0.25, 0.25, 0.25, 0.25]):  # Dividir en 4 fragmentos
        executor.submit(process_and_send_chunk, chunk)

# Detener sesión de Spark
spark.stop()

# Detener temporizador
end_time = time.time()

# Calcular e imprimir el tiempo total tomado
total_time = end_time - start_time
print(f'Todos los registros enviados a las {dt.datetime.now()}')
print(f'Tiempo total tomado: {total_time:.2f} segundos')
