from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("lab1_hello_spark") \
    .master("spark://spark-master:7077") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

print("##############  SparkSession initialisée ####################")
print(f"Version: {spark.version}")

# Récupérer le SparkContext pour l'UI
sc = spark.sparkContext
print(f"UI disponible sur: http://localhost:4040")

# 2. Créer un DataFrame simple
data = [
    ("Alice", "IT", 5000, 28),
    ("Bob", "Marketing", 4500, 32),
    ("Charlie", "IT", 6000, 35),
    ("Diana", "HR", 4000, 29),
    ("Eve", "Marketing", 4800, 31)
]

df = spark.createDataFrame(data, ["Name", "Department", "Salary", "Age"])
print(" #################DataFrame créé ########################")

# 3. Afficher le schéma et les données
print("\n ****************** Schéma: ****************************")
df.printSchema()

print("\n ********************** Données: *************************")
df.show()

# 4. Agrégations simples
print("\n #################### Salaire moyen par département: ############################")
df.groupBy("Department") \
  .agg(avg("Salary").alias("AvgSalary"), count("*").alias("Count")) \
  .show()

# 5. Filtres simples
print("\n $$$$$$$$$$$$$$$$$$$ Employés avec salaire > 5000: $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
df.filter(df.Salary > 5000).show()

print("\n $$$$$$$$$$$$$$$$$$$  Employés IT: $$$$$$$$$$$$$$$$$$$$$$$$$$$")
df.filter(df.Department == "IT").show()
input()
#time.sleep(300)
# 6. Arrêt propre
spark.stop()
print("\n ################### Session Spark arrêtée ######################")