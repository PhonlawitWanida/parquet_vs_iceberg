from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ===============================
# Spark Session + Iceberg Config
# ===============================
spark = SparkSession.builder \
    .appName("Parquet_vs_Iceberg_Fixed") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/warehouse") \
    .getOrCreate()

# ===============================
# Path
# ===============================
CSV_PATH = "/app/raw/healthcare_dataset.csv"
PARQUET_PATH = "/tmp/healthcare.parquet"

# ===============================
# Load CSV
# ===============================
df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)

print("\nTotal rows:", df.count())
df.show(5)

# ============================================================
# PARQUET
# ============================================================
print("\n" + "="*60)
print("PARQUET")
print("="*60)

# Step 1 — Write
print("\n[Parquet] Step 1 — Initial write")
df.write.mode("overwrite").parquet(PARQUET_PATH)

# Step 2 — Append
print("\n[Parquet] Step 2 — Append")
df.limit(100).write.mode("append").parquet(PARQUET_PATH)

# Step 3 — Delete (FIXED)
print("\n[Parquet] Step 3 — Delete (rewrite safely)")

df_parquet = spark.read.parquet(PARQUET_PATH)

df_deleted = df_parquet.filter(col("age") > 50)

# 🔥 FIX: break lineage ก่อน overwrite
df_deleted = df_deleted.cache()
df_deleted.count()

df_deleted.write.mode("overwrite").parquet(PARQUET_PATH)

print("Delete done (safe)")

# ============================================================
# ICEBERG
# ============================================================
print("\n" + "="*60)
print("ICEBERG")
print("="*60)

print("\n[Iceberg] Step 1 — Create table")

spark.sql("DROP TABLE IF EXISTS local.db.healthcare_iceberg")

df.writeTo("local.db.healthcare_iceberg") \
    .using("iceberg") \
    .create()

print("Create done")

print("\n[Iceberg] Step 2 — Append")

df.limit(100).writeTo("local.db.healthcare_iceberg").append()

print("Append done")

print("\n[Iceberg] Step 3 — Delete")

spark.sql("""
DELETE FROM local.db.healthcare_iceberg
WHERE age <= 50
""")

print("Delete done")

print("\n[Iceberg] Final count:")
spark.sql("SELECT COUNT(*) FROM local.db.healthcare_iceberg").show()

spark.stop()