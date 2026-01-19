# BTC-End2End-Platform

Plateforme end-to-end de prédiction du prix du Bitcoin utilisant le Machine Learning et une architecture distribuée. Le système récupère les données de marché en temps réel depuis l'API Binance, les transforme en indicateurs techniques exploitables, et prédit le prix du Bitcoin à horizon T+10 minutes via une API REST sécurisée


## Objectifs du Projet

Ce projet développé pour Quant-AI vise à créer un système industriel capable de :

⚡ Traiter des flux de données massifs en temps réel avec faible latence

📈 Fournir des prédictions financières à court terme (T+10 min)

🔐 Sécuriser l'accès aux prédictions via authentification JWT

🔄 Automatiser l'ensemble de la chaîne de traitement de données

📊 Gérer l'historique croissant grâce au calcul distribué

##  Architecture

Architecture Medallion (Bronze/Silver)

```shell
API Binance → Zone Bronze → Zone Silver → ML Model → API REST
   (Raw)      (Stockage)   (Features)   (Prédictions)  (Service)
```

- Zone Bronze : Données brutes OHLC et volumes depuis Binance

- Zone Silver : Données nettoyées, typées et enrichies d'indicateurs techniques

- Service Layer : Modèle de régression et API de consultation


## Organisation de l'Équipe

### Data Engineer – Lead Pipeline
Ingestion des données Binance (OHLC, volumes)

Mise en place du stockage Bronze/Silver

Traitement distribué avec PySpark

Orchestration des pipelines via Airflow

### Machine Learning Engineer – Lead Modèle

Feature engineering pour séries temporelles

Construction de la variable cible (prix T+10)

Entraînement et évaluation du modèle de régression

Sérialisation et monitoring des performances

### Backend & Security Engineer – Lead API


Développement API REST avec FastAPI

Implémentation de l'authentification JWT

Exposition des prédictions et endpoints analytiques

Gestion des logs et sécurité

### Stack Technique

 => Technologies Principales

- Langage : Python

- Big Data : PySpark (traitement distribué)

- Orchestration : Apache Airflow

- Base de données : PostgreSQL

- API : FastAPI

- Containerisation : Docker

- ML : Scikit-learn / PySpark MLlib

- Sécurité : JWT (JSON Web Tokens)

=> Compétences Transversales

Git (versioning)
Méthodologie Agile (Kanban)
JSON (manipulation de données)
SQL (requêtes analytiques)


## Source de Données

### API Binance - Format des Données

```shell
[
    [
        1499040000000,         // Kline open time
        "0.01634790",          // Open price
        "0.80000000",          // High price
        "0.01575800",          // Low price
        "0.01577100",          // Close price
        "148976.11427815",     // Volume
        1499644799999,         // Kline Close time
        "2434.19055334",       // Quote asset volume
        308,                   // Number of trades
        "1756.87402397",       // Taker buy base asset volume
        "28.46694368",         // Taker buy quote asset volume
        "0"                    // Unused field
    ]
]
```

## Modèle de Machine Learning

- Objectif

Prédire le prix de clôture du Bitcoin à T+10 minutes

- Variable Cible

```shell
# Utilisation de la fonction lead() en PySpark
from pyspark.sql.window import Window
from pyspark.sql import functions as F

window = Window.orderBy("open_time")
df = df.withColumn("close_t_plus_10", F.lead("close", 10).over(window))
```

### Features (Variables d'entrée)

1. Variations de Prix (Returns)

Calcul de la variation relative du prix de clôture :
```shell
return(t) = (close(t) - close(t-1)) / close(t-1)
```

2. Moyennes Mobiles

Lissage des variations à court terme :

   - MA_5 : Moyenne des prix de clôture sur 5 minutes
   - MA_10 : Moyenne des prix de clôture sur 10 minutes

```shell

# Exemple avec PySpark
window_5 = Window.orderBy("open_time").rowsBetween(-4, 0)
df = df.withColumn("MA_5", F.avg("close").over(window_5))
```

3. Volume et Intensité de Trading

Proportion de BTC acheté par les "takers" :
```shell
taker_ratio(t) = taker_buy_base_volume / volume
```

### Métriques d'Évaluation

   - RMSE (Root Mean Square Error)
   - MAE (Mean Absolute Error)

## Pipeline de Données

- Ingestion : Récupération des données Binance toutes les minutes

- Stockage Bronze : Sauvegarde des données brutes

- Transformation : Calcul des features avec PySpark

- Stockage Silver : Données enrichies et nettoyées

- Entraînement : Mise à jour périodique du modèle ML

- Prédiction : Génération des prédictions T+10

 - Exposition : API REST pour consultation

## Sécurité

Authentification JWT : Protection de l'accès aux prédictions

Contrôle d'accès : Gestion des permissions utilisateurs

Logs : Traçabilité des requêtes et accès

Protection de la propriété intellectuelle : Sécurisation des modèles

## Ressources

- Documentation API Binance
- Apache Airflow Documentation
- PySpark Documentation
- FastAPI Documentation

##  Démarrage Rapide

```shell
# Cloner le repository
git clone [https://github.com/elhidarinouhayla/BTC-End2End-Platform.git]

# Installer les dépendances
pip install -r requirements.txt

# Configurer les variables d'environnement
cp .env.example .env

# Lancer les conteneurs Docker
docker-compose up -d

# Initialiser Airflow
airflow db init

# Démarrer l'API
uvicorn app.main:app --reload
```

