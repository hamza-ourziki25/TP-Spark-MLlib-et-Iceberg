# TP3 : Prévoir la demande de vélos avec Spark MLlib et Iceberg

## 1. Objectif

Anticiper le nombre de locations de vélos par heure (colonne `cnt`) à partir du calendrier et de la météo.
Le projet construit une chaîne de données complète sur un environnement Docker existant :

```
hour.csv -> Spark -> Bronze (Parquet, MinIO) -> Silver (Iceberg, MinIO) -> MLlib -> Gold (Iceberg, MinIO)
```

- **Bronze** : copie Parquet reproductible des données reçues (écrasée à chaque lancement).
- **Silver** : observations nettoyées et dédoublonnées, table Iceberg `lake.silver.locations`.
- **Gold** : métriques et prédictions, tables Iceberg `lake.gold.metriques` et `lake.gold.predictions`.

Quatre méthodes sont comparées sur 2012 : une référence simple (moyenne 2011 par heure et type de jour), une régression linéaire, un arbre de décision et une forêt aléatoire.

## 2. Source des données

Jeu de données **Bike Sharing** de l'UCI (Capital Bikeshare, 2011-2012), fichier `hour.csv` (17 379 observations horaires).
Téléchargement : https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset

Le fichier n'est pas versionné (dossier `data/` ignoré). Il est téléversé dans MinIO sous `bronze/sources/hour.csv` et lu par Spark via `s3a://bronze/sources/hour.csv`.

## 3. Versions

Image Docker : `elmendili/bigdata-hadoop:first` (non modifiée).

| Composant | Version |
|---|---|
| Hadoop / HDFS | 3.3.6 |
| Spark | 3.5.1 (client Hadoop 3.3.4) |
| Scala | 2.12.18 |
| Java | 8 (1.8.0_402) |
| Python | 3.10.12 |
| Apache Iceberg | 1.6.1 (`iceberg-spark-runtime-3.5_2.12`) |
| Pilote MySQL | `mysql-connector-j` 8.4.0 |
| MySQL | 8.0.40 |

Iceberg 1.6.1 est la dernière version compatible avec Java 8 (les versions 1.7.0 et suivantes l'ont abandonné).

## 4. Services et réseau

Tous les services sont sur le réseau Docker `hadoop`.

| Rôle | Conteneur | Adresse utilisée dans la configuration |
|---|---|---|
| Spark master + driver | `hadoop-master` | `spark://hadoop-master:7077` |
| Worker 1 | `hadoop-slave1` | - |
| Worker 2 | `hadoop-slave2` | - |
| Stockage objet | `tp2-minio` | `http://minio:9000` (alias réseau) |
| Catalogue JDBC | `tp2-mysql` | `jdbc:mysql://mysql:3306/iceberg_catalog` (alias réseau) |

Rôle des composants :

- **MinIO** : stocke les fichiers Parquet, les métadonnées Iceberg et les modèles (buckets `bronze` et `lakehouse`).
- **MySQL** : héberge le catalogue JDBC Iceberg (base `iceberg_catalog`), pas les observations.
- **Iceberg** : organise les tables et leurs versions (snapshots).
- **Spark** : exécute les traitements SQL et distribue les tâches sur les deux workers.
- **MLlib** (`pyspark.ml`) : apprend les modèles de régression.

Vérification du réseau :

```powershell
docker network inspect hadoop
```

## 5. Structure du dépôt

```
.
├── README.md
├── .gitignore
├── lancer.ps1              # lanceur spark-submit (copie les scripts dans hadoop-master)
├── conf/
│   └── tp.example.json     # exemple de configuration SANS secrets
├── jobs/
│   ├── common.py           # session Spark + catalogue Iceberg JDBC + accès MinIO
│   ├── versions.py         # vérification des versions dans le cluster
│   ├── check.py            # test d'écriture/lecture Iceberg
│   ├── prepare.py          # Bronze -> Silver (qualité, rejets, MERGE)
│   ├── train.py            # entraînement, évaluation, publication Gold
│   └── analyse.py          # comparaison des demandes moyennes 2011 / 2012
└── rapport/
    ├── reponses.md         # réponses aux questions
    └── captures.pdf        # captures d'écran de toutes les étapes
```

Non versionnés (voir `.gitignore`) : `conf/tp.json` (secrets), `data/`, `models/`, JAR, caches, journaux.

## 6. Configuration

1. Copier `conf/tp.example.json` vers `conf/tp.json`.
2. Remplacer les valeurs factices par celles de l'environnement :

```json
{
  "minio_endpoint": "http://minio:9000",
  "minio_access_key": "REMPLACER_CLE",
  "minio_secret_key": "REMPLACER_SECRET",
  "mysql_uri": "jdbc:mysql://mysql:3306/iceberg_catalog",
  "mysql_user": "iceberg_tp",
  "mysql_password": "REMPLACER_MOT_DE_PASSE"
}
```

Le fichier réel `conf/tp.json` reste local et n'est jamais déposé sur GitLab.

## 7. Reproduction pas à pas

Toutes les commandes sont exécutées depuis le dossier du TP dans PowerShell.

### 7.1 Préparer le dossier de travail

```powershell
New-Item -ItemType Directory -Force data,jobs,conf,models,rapport
```

### 7.2 Vérifier l'installation

```powershell
docker ps
docker exec hadoop-master bash -lc '$SPARK_HOME/bin/spark-submit --version'
.\lancer.ps1 versions.py
```

Dans l'interface du master Spark, vérifier que les deux workers sont actifs.

### 7.3 Préparer MinIO et MySQL

- Créer les buckets `bronze` et `lakehouse` dans la console MinIO.
- Téléverser `hour.csv` sous `bronze/sources/hour.csv`.
- Créer la base et le compte du catalogue :

```sql
CREATE DATABASE IF NOT EXISTS iceberg_catalog;
CREATE USER IF NOT EXISTS 'iceberg_tp'@'%' IDENTIFIED BY 'MOT_DE_PASSE_TP';
GRANT ALL PRIVILEGES ON iceberg_catalog.* TO 'iceberg_tp'@'%';
```

### 7.4 Lanceur

`lancer.ps1` copie les scripts et la configuration dans `hadoop-master:/tmp/tp-velos`, puis exécute `spark-submit` en mode client avec les dépendances supplémentaires :

- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1`
- `com.mysql:mysql-connector-j:8.4.0`

Le connecteur Hadoop AWS déjà présent dans l'image est conservé. Le lanceur exige un accès Internet depuis le master pour télécharger les JAR (cache Ivy dans `/tmp/ivy-tp-velos`).

### 7.5 Exécuter la chaîne

```powershell
.\lancer.ps1 check.py      # test de connexion : doit afficher la valeur 1
.\lancer.ps1 prepare.py    # Bronze -> Silver
.\lancer.ps1 train.py      # entraînement, évaluation, Gold
.\lancer.ps1 analyse.py    # demande moyenne 2011 vs 2012
```

## 8. Méthode

### 8.1 Qualité des données (`prepare.py`)

- Conversion explicite des types (entiers, décimaux, date).
- Contrôles : `cnt >= 0`, `hr` entre 0 et 23, mesures météo normalisées entre 0 et 1, aucune valeur nulle.
- Les lignes invalides sont isolées dans `s3a://bronze/velos/rejets`.
- Doublons strictement identiques supprimés ; un `assert` bloque si un `instant` porte encore plusieurs lignes différentes.
- `MERGE INTO ... WHEN NOT MATCHED THEN INSERT *` : un second lancement n'ajoute aucune ligne (idempotence du contenu). Les corrections de lignes existantes ne sont pas gérées (voir les réponses aux questions).

### 8.2 Modèles (`train.py`)

- **Cible** : `cnt` (régression).
- **Variables** : `hr`, `season`, `mnth`, `holiday`, `weekday`, `workingday`, `weathersit` (catégorielles, encodées par `StringIndexer` + `OneHotEncoder`), et `temp`, `atemp`, `hum`, `windspeed`.
- **Exclues** : `casual` et `registered` (leur somme vaut exactement `cnt` : fuite de la cible), `instant` et la date brute.
- **Pipeline** : `StringIndexer` -> `OneHotEncoder` -> `VectorAssembler` -> algorithme. Les catégories sont apprises sur `train` uniquement.
- **Séparation chronologique** : apprentissage sur 2011, test sur 2012 (plus proche d'une vraie prévision qu'un tirage aléatoire).
- **Reproductibilité** : le `snapshot_id` Silver utilisé est relevé (`lake.silver.locations.history`) et la table est relue avec `VERSION AS OF`. Il est stocké dans les tables Gold.

| Algorithme | Paramètres fixes |
|---|---|
| Régression linéaire | `maxIter=100`, `regParam=0.1`, `elasticNetParam=0.0`, `standardization=True` |
| Arbre de décision | `maxDepth=8`, `seed=42` |
| Forêt aléatoire | `numTrees=40`, `maxDepth=8`, `seed=42` |

Les pipelines complets sont enregistrés dans `s3a://lakehouse/modeles/{lineaire,arbre,foret}`.

### 8.3 Évaluation

- **Référence** (pas un algorithme appris) : moyenne 2011 pour chaque couple (heure, jour travaillé) ; moyenne générale 2011 si le couple est absent.
- **MAE** : moyenne des erreurs absolues, en locations par heure.
- **RMSE** : racine de la moyenne des erreurs au carré ; pénalise davantage les grosses erreurs.
- Les paramètres ne sont pas ajustés d'après les scores de 2012, qui restent une évaluation finale.

Tables Gold publiées : `lake.gold.metriques` (4 lignes, une par méthode) et `lake.gold.predictions` (une ligne par observation de test et par méthode, colonne `modele`).

## 9. Résultats obtenus

Snapshot Silver utilisé : `5695620952869767607`
Lignes d'apprentissage (2011) : 8 645 | lignes de test (2012) : 8 734

| Méthode | MAE | RMSE |
|---|---|---|
| Référence | 108,52 | 157,05 |
| Régression linéaire | 110,17 | 158,02 |
| Forêt aléatoire | 120,64 | 161,90 |
| Arbre de décision | 123,38 | 158,75 |

Constats :

- Aucun des trois modèles ne bat la référence simple sur 2012.
- Les plus grosses erreurs concernent les heures de pointe (8 h, 17 h, 18 h, 19 h) des jours travaillés, systématiquement sous-estimées.
- Explication probable (à confirmer avec `analyse.py`) : la demande a augmenté entre 2011 et 2012, alors que les modèles n'ont appris que sur 2011 et ne disposent pas de variable d'année ou de tendance. Les arbres ne peuvent pas prédire au-delà des valeurs vues à l'apprentissage.

Ces résultats sont une évaluation rétrospective ; ils ne garantissent pas la performance future.

## 10. Limites

- Les mesures météo observées jouent le rôle de prévisions parfaites ; en exploitation, il faudrait les prévisions disponibles avant l'heure à prédire.
- Bronze est écrasé à chaque lancement : ce n'est pas une archive de lots.
- `instant` est propre à ce fichier historique ; une application réelle utiliserait une clé métier (date et heure) ou un identifiant d'événement.
- Le `MERGE` n'applique pas de corrections sur les lignes existantes.
- Aucune recherche d'hyperparamètres ni validation chronologique interne à 2011 n'a été faite (non demandé).
- Le modèle prédit la demande du réseau entier, pas par station : il ne permet pas de répartir les vélos station par station.
- Les prédictions linéaires peuvent être négatives, alors qu'une demande ne peut pas l'être.

## 11. Dépannage

| Erreur | Cause probable |
|---|---|
| `UnknownHostException` | nom de conteneur ou réseau `hadoop` incorrect |
| `ClassNotFoundException` | JAR manquant (dépendances non téléchargées) |
| `UnsupportedClassVersionError` | incompatibilité Java (rester sur Iceberg 1.6.1) |
| `AccessDenied` | clés MinIO ou droits sur les buckets |
| `fatal: detected dubious ownership` (Git) | `git config --global --add safe.directory E:/TP3` |

L'avertissement du catalogue JDBC invitant à définir `jdbc.schema-version=V1` est informatif et n'empêche pas l'exécution.

## 12. Livraison

- Lien du dépôt GitLab : `https://gitlab.com/VOTRE_COMPTE/tp-velos-nom-prenom`
- SHA du commit remis : à relever avec `git rev-parse HEAD`
- Auteur : NOM Prénom