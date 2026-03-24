# Architecture

## Vue d'ensemble

Le projet suit une architecture lakehouse en trois couches :

- Bronze : donnees brutes issues des flux et historiques Binance
- Silver : donnees standardisees, nettoyees et enrichies
- Gold : metriques pre-calculees pour le monitoring et l'analytique

## Composants

- ingestion temps reel
- chargement historique
- transformations batch/streaming
- controles qualite
- publication de tableaux de bord et alertes
