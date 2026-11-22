param([string]$Job = "check.py", [string]$Master = "local[2]")
docker exec hadoop-master mkdir -p /tmp/tp-velos/jobs /tmp/tp-velos/conf
docker cp jobs/. hadoop-master:/tmp/tp-velos/jobs/
docker cp conf/tp.json hadoop-master:/tmp/tp-velos/conf/tp.json
$deps = @(
 "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
 "com.mysql:mysql-connector-j:8.4.0",
 "org.apache.hadoop:hadoop-aws:3.3.4",
 "com.amazonaws:aws-java-sdk-bundle:1.12.262"
) -join ","
$argsSpark = @(
 "--master", $Master,
 "--deploy-mode", "client",
 "--conf", "spark.driver.host=hadoop-master",
 "--conf", "spark.driver.bindAddress=0.0.0.0",
 "--conf", "spark.jars.ivy=/tmp/ivy-tp-velos",
 "--conf", "spark.driver.memory=2g",
 "--conf", "spark.executor.memory=1g",
 "--conf", "spark.cores.max=2",
 "--conf", "spark.ui.enabled=false",
 "--packages", $deps,
 "/tmp/tp-velos/jobs/$Job"
)
docker exec -w /tmp/tp-velos hadoop-master bash -lc `
 'exec "$SPARK_HOME/bin/spark-submit" "$@"' _ @argsSpark