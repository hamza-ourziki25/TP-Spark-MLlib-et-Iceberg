from common import session
# Ouvrir Spark avec la configuration commune.
s = session("verification")
# Créer l’espace de noms Silver s’il est absent.
s.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
# Tester une écriture transactionnelle dans une table réservée au contrôle.
s.sql("""CREATE OR REPLACE TABLE lake.silver.test_connexion
 USING iceberg AS SELECT 1 AS valeur""")
# Relire la table : le résultat attendu est la valeur 1.
s.table("lake.silver.test_connexion").show()
print("Master utilise :", s.sparkContext.master)
# Fermer la session après le contrôle.
s.stop()
