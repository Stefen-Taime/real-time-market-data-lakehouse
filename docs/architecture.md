# Architecture

## Vue d'ensemble

Le projet suit une architecture Medallion sur Databricks:

- Bronze : donnees brutes Binance historiques et live
- Silver : donnees normalisees, dedupliquees et harmonisees
- Gold : metriques analytiques et couches de consultation
- Audit : qualite des donnees et etat de traitement incremental

## Orchestration retenue

La voie d'orchestration canonique est `Lakeflow Jobs` via le bundle Databricks:

- backfill historique
- ingestion live
- transformation Silver
- build Gold
- controles qualite
- reporting observabilite

Les fichiers `resources/pipelines/*.yml` restent des points d'extension pour une evolution future vers des pipelines manages, mais ils ne sont pas deployes sur Free Edition.

## Gouvernance

Le repo supporte deux modes:

- mode `path` pour la robustesse sur Free Edition
- mode `register_tables=true` pour valider autant que possible un namespace gouverne

Sur le workspace actuel, la gouvernance Unity Catalog reste partielle: les jobs tournent et les ecritures peuvent reussir, mais la visibilite des objets cote warehouse/app n'est pas encore equivalente a un vrai workspace enterprise.
