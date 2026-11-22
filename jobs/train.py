from common import session
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import (LinearRegression,
    DecisionTreeRegressor, RandomForestRegressor)
from pyspark.ml.evaluation import RegressionEvaluator

s = session("demande_velos")

# Snapshot Silver utilisé par l'expérience.
sid = s.sql("""SELECT snapshot_id FROM lake.silver.locations.history
               ORDER BY made_current_at DESC LIMIT 1""").first()[0]
print("Snapshot Silver utilise :", sid)

# Relire exactement ce snapshot pour les trois algorithmes.
df = s.sql(f"SELECT * FROM lake.silver.locations VERSION AS OF {sid}")

# 2011 pour l'apprentissage, 2012 pour le test.
train = df.filter("date < DATE '2012-01-01'")
test = df.filter("date >= DATE '2012-01-01'")
n_train, n_test = train.count(), test.count()
assert n_train > 0 and n_test > 0
print("Lignes train :", n_train, "| lignes test :", n_test)

# Variables catégorielles (casual et registered exclus : fuite de la cible).
cats = ["hr", "season", "mnth", "holiday", "weekday",
        "workingday", "weathersit"]
idx = [c + "_idx" for c in cats]
vec = [c + "_vec" for c in cats]

indexer = StringIndexer(inputCols=cats, outputCols=idx, handleInvalid="keep")
encoder = OneHotEncoder(inputCols=idx, outputCols=vec, handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=vec + ["temp", "atemp", "hum", "windspeed"],
    outputCol="features")

algos = {
    "lineaire": LinearRegression(labelCol="cnt", maxIter=100,
        regParam=0.1, elasticNetParam=0.0, standardization=True),
    "arbre": DecisionTreeRegressor(labelCol="cnt", maxDepth=8, seed=42),
    "foret": RandomForestRegressor(labelCol="cnt",
        numTrees=40, maxDepth=8, seed=42)
}

predictions = {}
for name, algo in algos.items():
    pipeline = Pipeline(stages=[indexer, encoder, assembler, algo])
    model = pipeline.fit(train)
    predictions[name] = model.transform(test).cache()
    model.write().overwrite().save("s3a://lakehouse/modeles/" + name)
    print("Modele enregistre :", name)

# ---------- Évaluation et publication Gold ----------
avg = train.groupBy("hr", "workingday").agg(F.avg("cnt").alias("base"))
fallback = train.agg(F.avg("cnt")).first()[0]
baseline = (test.join(avg, ["hr", "workingday"], "left")
            .withColumn("prediction", F.coalesce("base", F.lit(fallback))))

s.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")

frames = {"reference": baseline, **predictions}
metrics = []
all_predictions = None
for name, frame in frames.items():
    values = []
    for metric in ["mae", "rmse"]:
        ev = RegressionEvaluator(labelCol="cnt",
            predictionCol="prediction", metricName=metric)
        values.append(ev.evaluate(frame))
    metrics.append((name, *values, int(sid)))
    out = (frame.select("instant", "date", "hr", "cnt", "prediction")
           .withColumn("modele", F.lit(name))
           .withColumn("snapshot_source", F.lit(sid)))
    all_predictions = out if all_predictions is None \
        else all_predictions.unionByName(out)

schema = "modele string, mae double, rmse double, snapshot long"
scores = s.createDataFrame(metrics, schema)
scores.orderBy("mae").show(truncate=False)

scores.writeTo("lake.gold.metriques").using("iceberg").createOrReplace()
all_predictions.writeTo("lake.gold.predictions").using("iceberg").createOrReplace()

all_predictions.orderBy(
    F.abs(F.col("cnt") - F.col("prediction")).desc()).show(12)

for frame in predictions.values():
    frame.unpersist()
s.stop()