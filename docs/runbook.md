# Runbook

## Operations standard

1. verifier l'etat des jobs Databricks
2. verifier la fraicheur des tables Bronze, Silver et Gold
3. inspecter les alertes de qualite et d'ingestion
4. relancer un job si l'echec est transitoire

## Verifications minimales

- checkpoints streaming
- latence d'ingestion
- nombre de doublons
- taux de valeurs nulles sur les colonnes critiques
