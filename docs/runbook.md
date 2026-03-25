# Runbook

## Operations standard

1. verifier le dernier run de `market_data_medallion_pipeline_${bundle.target}`
2. verifier la fraicheur Bronze, Silver, Gold et Audit
3. verifier les violations de qualite sur `quality_check_runs`
4. verifier l'avancement watermark sur `processing_state`
5. relancer seulement la brique en echec si l'incident est transitoire

## Verifications minimales

- latence d'ingestion live inferieure au SLO attendu
- nombre de doublons nul sur les cles metier Silver
- taux de valeurs nulles dans les seuils configures
- succes du job `data_quality_checks_${bundle.target}`
- succes du job `market_observability_report_${bundle.target}`

## Relances recommandees

- incident Bronze historique: relancer `backfill_history`
- incident live Binance: relancer `ingest_live_market`
- incident Silver: relancer `transform_silver`
- incident Gold: relancer `build_gold_metrics`
- incident qualite: corriger puis relancer `data_quality_checks`
