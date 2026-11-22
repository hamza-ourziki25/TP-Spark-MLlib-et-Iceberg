import json
from pyspark.sql import SparkSession

def session(name):
    with open("/tmp/tp-velos/conf/tp.json", encoding="utf-8") as f:
        c = json.load(f)
    opts = {
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.sql.constraintPropagation.enabled": "false",
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lake.catalog-impl":
            "org.apache.iceberg.jdbc.JdbcCatalog",
        "spark.sql.catalog.lake.uri": c["mysql_uri"],
        "spark.sql.catalog.lake.jdbc.user": c["mysql_user"],
        "spark.sql.catalog.lake.jdbc.password": c["mysql_password"],
        "spark.sql.catalog.lake.warehouse": "s3a://lakehouse/",
        "spark.sql.catalog.lake.io-impl":
            "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.hadoop.fs.s3a.endpoint": c["minio_endpoint"],
        "spark.hadoop.fs.s3a.access.key": c["minio_access_key"],
        "spark.hadoop.fs.s3a.secret.key": c["minio_secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    }
    b = SparkSession.builder.appName(name)
    for key, value in opts.items():
        b = b.config(key, value)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark