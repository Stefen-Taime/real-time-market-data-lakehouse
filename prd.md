# PRD — Real-Time Market Data Lakehouse

## 1. Contexte du projet

Nous voulons construire une plateforme de données de marché en temps réel sur Databricks Free Edition, en simulant un cas d’usage proche d’un environnement entreprise.

Le projet doit permettre d’ingérer :
- des données historiques de marché (batch),
- un flux réel en temps réel (live stream),
- puis de transformer, gouverner, stocker et exposer ces données pour l’analyse.

L’architecture s’appuie sur :
- **Delta Lake** comme format de table principal,
- **Unity Catalog** comme couche de gouvernance et d’organisation des données,
- une approche **Bronze / Silver / Gold** pour structurer le cycle de vie des données.

## 2. Problème à résoudre

Les systèmes data modernes doivent gérer deux besoins en parallèle :
1. exploiter l’historique pour le contexte analytique,
2. ingérer des événements temps réel pour suivre l’état du marché.

Sans architecture claire :
- les données batch et live restent séparées,
- la qualité est difficile à contrôler,
- le catalogue des données est flou,
- les pipelines deviennent difficiles à maintenir et à faire évoluer.

Nous voulons donc résoudre les points suivants :
- unifier les données historiques et temps réel dans une même plateforme,
- centraliser les données dans un **catalogue gouverné**,
- fiabiliser les transformations avec des tables **Delta**,
- produire des tables prêtes pour l’analyse,
- démontrer un cycle de vie complet : conception, tests, déploiement, maintenance.

## 3. Solution proposée

La solution retenue est un **Real-Time Market Data Lakehouse** basé sur :
- **Unity Catalog** pour organiser les objets de données,
- **Delta Lake** pour stocker les tables du lakehouse,
- une architecture **Medallion** avec les couches Bronze, Silver et Gold.

### Sources de données
- **Source 1 — Batch historique** : données de marché historiques (klines/candlesticks, éventuellement trades)
- **Source 2 — Flux live** : données de marché en temps réel via un vrai stream public

### Principe
- les données brutes arrivent dans une zone d’ingestion gouvernée,
- elles sont stockées dans des tables **Bronze en Delta**,
- elles sont nettoyées, harmonisées et validées dans des tables **Silver en Delta**,
- elles sont agrégées dans des tables **Gold en Delta** pour les usages analytiques et dashboards.

## 4. Architecture de la solution

### Unity Catalog
Unity Catalog sert de couche de gouvernance et d’organisation.  
Les objets du projet sont organisés dans un namespace de type :

`catalog.schema.table`

Exemple :
- `market_data.bronze.trades_live`
- `market_data.silver.trades`
- `market_data.gold.volume_1m`

Unity Catalog permet de :
- structurer les données par domaine et par couche,
- centraliser la gouvernance,
- faciliter la découverte des tables,
- garder une structure exploitable pour un projet de type entreprise.

### Delta Lake
Toutes les tables du projet sont stockées en **Delta** :
- transactions ACID,
- schéma contrôlé,
- meilleure fiabilité des écritures batch et incrémentales,
- support naturel des traitements lakehouse sur Databricks.

### Bronze
Stockage brut des données sources :
- historique marché brut
- événements live bruts
- métadonnées d’ingestion

Exemples :
- `market_data.bronze.klines_history`
- `market_data.bronze.trades_live_raw`

### Silver
Transformation et standardisation :
- normalisation des timestamps
- cast des types
- déduplication
- contrôles de qualité
- harmonisation batch + live

Exemples :
- `market_data.silver.klines_1m`
- `market_data.silver.trades`

### Gold
Tables analytiques prêtes à l’usage :
- dernier prix par symbole
- volume par intervalle de temps
- volatilité courte
- top movers
- métriques de qualité des données

Exemples :
- `market_data.gold.latest_price`
- `market_data.gold.volume_1m`
- `market_data.gold.volatility_5m`

## 5. Ce que le projet cherche à démontrer

Ce projet cherche à démontrer qu’il est possible de mettre en place une architecture data sérieuse, même dans un environnement limité, en couvrant :

- l’ingestion multi-sources,
- le traitement batch et quasi temps réel,
- le stockage fiable avec **Delta Lake**,
- l’organisation et la gouvernance via **Unity Catalog**,
- la modélisation lakehouse Bronze / Silver / Gold,
- les scripts ETL/ELT,
- les contrôles qualité,
- l’orchestration des traitements,
- les tests automatisés,
- le déploiement via CI/CD,
- la documentation et la maintenance.

## 6. Résultat attendu

À la fin du projet, nous devons disposer :
- d’un pipeline batch fonctionnel,
- d’un pipeline live fonctionnel,
- d’un catalogue de données clair dans **Unity Catalog**,
- de tables **Delta** exploitables en Bronze / Silver / Gold,
- d’indicateurs de marché consultables,
- d’une structure projet propre, gouvernée et maintenable.

Ce projet servira de base technique pour simuler un vrai système data d’entreprise dans Databricks, avec une architecture claire, modulaire et évolutive.