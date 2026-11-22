import sys
from pyspark.sql import SparkSession
# Démarrer une session avec le master défini par spark-submit.
s = SparkSession.builder.appName("versions").getOrCreate()
# Accéder aux bibliothèques Java réellement chargées par Spark.
jvm = s.sparkContext._jvm
print("Spark :", s.version)
print("Hadoop charge :", jvm.org.apache.hadoop.util.VersionInfo.getVersion())
print("Scala :", jvm.scala.util.Properties.versionNumberString())
print("Java :", jvm.java.lang.System.getProperty("java.version"))
print("Python driver :", sys.version)
# Cette fonction est exécutée par les processus Python des exécutants.
def python_worker(_):
 import sys
 return sys.version
# Déclencher des tâches distribuées pour relever leurs versions Python.
print("Python executeurs :", s.sparkContext.parallelize(range(4), 4).map(python_worker).distinct().collect())
# Fermer la session et libérer ses ressources.
s.stop()
