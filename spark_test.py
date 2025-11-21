from pyspark.sql import SparkSession

# 1. Démarrer SparkSession
spark = SparkSession.builder \
    .appName("SimpleTest") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# 2. Afficher la version
print(f"Version Spark: {spark.version}")

# 3. Test basique
data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["Name", "Age"])

print("DataFrame créé:")
df.show()

# 4. Arrêter proprement
spark.stop()
print("✅ Session Spark arrêtée")