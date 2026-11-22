from common import session
from pyspark.sql import functions as F

s = session("preparation_velos")

# Bronze : copie Parquet de la source.
raw = s.read.option("header", True).csv("s3a://bronze/sources/hour.csv")
raw.write.mode("overwrite").parquet("s3a://bronze/velos/locations")
b = s.read.parquet("s3a://bronze/velos/locations")
n_source = b.count()

ints = ["instant", "hr", "season", "yr", "mnth", "holiday",
        "weekday", "workingday", "weathersit", "cnt"]
reals = ["temp", "atemp", "hum", "windspeed"]

# Conversion des types, puis écriture : le plan repart de colonnes simples.
(b.select(F.to_date("dteday").alias("date"),
          *[F.col(c).cast("int").alias(c) for c in ints],
          *[F.col(c).cast("double").alias(c) for c in reals])
  .write.mode("overwrite").parquet("s3a://bronze/velos/typees"))
x = s.read.parquet("s3a://bronze/velos/typees")

# Condition de validité sur colonnes simples.
ok = F.col("date").isNotNull() & (F.col("cnt") >= 0) & F.col("hr").between(0, 23)
for c in ints + reals:
    ok = ok & F.col(c).isNotNull()
for c in reals:
    ok = ok & F.col(c).between(0, 1)
ok = F.coalesce(ok, F.lit(False))

# Rejets.
x.filter(~ok).write.mode("overwrite").parquet("s3a://bronze/velos/rejets")
n_rejets = s.read.parquet("s3a://bronze/velos/rejets").count()

# Valides sans doublons parfaits, écrits puis relus (calculés une seule fois).
(x.filter(ok).dropDuplicates()
  .write.mode("overwrite").parquet("s3a://bronze/velos/valides"))
v = s.read.parquet("s3a://bronze/velos/valides")
n_valides = v.count()

# Un identifiant ne doit plus porter plusieurs lignes différentes.
assert v.select("instant").distinct().count() == n_valides, "conflit sur instant"

s.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
v.createOrReplaceTempView("arrivees")
s.sql("""CREATE TABLE IF NOT EXISTS lake.silver.locations
         USING iceberg AS SELECT * FROM arrivees WHERE 1 = 0""")
s.sql("""MERGE INTO lake.silver.locations t USING arrivees a
         ON t.instant = a.instant
         WHEN NOT MATCHED THEN INSERT *""")

print("source", n_source, "rejets", n_rejets, "valides", n_valides)
print("silver", s.table("lake.silver.locations").count())
s.sql("SELECT snapshot_id, operation FROM lake.silver.locations.snapshots").show()
s.stop()