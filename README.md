# Real-Time Market Data Lakehouse

Scaffold initial pour une plateforme de donnees de marche en temps reel construite autour d'une architecture Bronze / Silver / Gold, avec deploiement GitHub Actions vers Databricks.

## Structure

- `src/` contient la logique d'ingestion, de transformation, de qualite et de production des tables Gold
- `resources/` contient les definitions de jobs, pipelines, dashboards et alertes
- `docs/` centralise la documentation d'architecture, d'exploitation et de deploiement
- `sql/` regroupe les scripts DDL, controles qualite et requetes analytiques
- `notebooks/06_quality_audit_report.py` permet de consulter rapidement l'historique des controles qualite
- Les jobs Lakeflow sous `resources/jobs/` sont la voie d'orchestration canonique sur ce repo; les stubs `resources/pipelines/` restent des points d'extension et ne sont pas deployes sur Free Edition

## Demarrage

1. Installer les dependances avec `pip install -r requirements.txt`
2. Completer les fichiers de configuration dans `config/`
3. Adapter les ressources Databricks avant validation et deploiement

## CI/CD GitHub

- Le workflow [ci.yml](/Users/stefen/real-time-market-data-lakehouse/.github/workflows/ci.yml) execute compilation Python, tests unitaires/integration, puis un `databricks bundle validate -t dev` si les secrets GitHub Databricks sont presents
- Le workflow [ci.yml](/Users/stefen/real-time-market-data-lakehouse/.github/workflows/ci.yml) valide maintenant les targets `dev`, `staging` et `prod` quand les secrets Databricks sont presents
- Le workflow [cd.yml](/Users/stefen/real-time-market-data-lakehouse/.github/workflows/cd.yml) deploie automatiquement le bundle vers `dev` sur chaque push vers `main`, puis peut etre lance manuellement avec le target `staging` ou `prod`
- Les workflows GitHub sont configures en auth par token, ce qui est le mode le plus compatible avec Databricks Free Edition
- Secrets GitHub requis:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`
- Si tu migres plus tard vers un workspace Databricks non Free Edition, tu pourras remplacer cette auth par OIDC avec service principal
- Le CD GitHub cible volontairement `dev` pour l instant, car c est le seul target bundle reellement opere et valide sur ce workspace Free Edition
- `staging` est maintenant deployable manuellement depuis GitHub Actions; `prod` est prepare au niveau bundle, mais reste sur le meme workspace tant qu aucun environnement Databricks dedie n est ajoute

## Environnements Databricks

- `config/dev.yml` conserve un mode `path` Delta sous `dbfs:/tmp/...` et reste le mode le plus robuste sur Free Edition
- `config/dev_governed.yml` active `register_tables=true` en mode best-effort pour la Free Edition
- `config/dev_app.yml` publie les datasets app-facing dans un volume Unity Catalog pour contourner la limitation SQL sur le DBFS public
- `config/staging*.yml` et `config/prod*.yml` permettent maintenant de separer les noms de tables, paths et couches app-facing par environnement
- Sur le workspace valide a ce jour, les jobs Databricks tournent bien avec `register_tables=true`, mais les objets ne remontent pas via `databricks tables list` dans `workspace.default`
- En pratique, cela signifie que la logique applicative supporte bien les tables enregistrees, mais que la gouvernance Unity Catalog reste partielle sur cette edition
- Pour satisfaire completement le PRD cote gouvernance catalogue, il faudra un workspace Databricks avec support Unity Catalog pleinement exploitable

## Bootstrap Workspace

- Le script [setup_workspace.sh](/Users/stefen/real-time-market-data-lakehouse/scripts/setup_workspace.sh) n'est plus un placeholder: il valide le target bundle, charge le profil gouverne correspondant et cree schemas + volume Unity Catalog de facon idempotente quand `register_tables=true`
- Exemples:
  - `./scripts/setup_workspace.sh dev`
  - `./scripts/setup_workspace.sh staging`
  - `./scripts/setup_workspace.sh prod`
- Sur Free Edition, le script n'essaie pas de forcer un catalog custom quand la strategie retenue reste `workspace.default`

## Consultation Qualite

- Le notebook `notebooks/05_data_quality_checks.py` ecrit l'audit des controles dans `dbfs:/tmp/real-time-market-data-lakehouse/dev/audit/quality_check_runs` quand `register_tables=false`
- Le notebook `notebooks/06_quality_audit_report.py` lit cet audit et affiche les runs recents, les datasets en echec et la tendance de sante par dataset
- Si tu passes plus tard en mode `register_tables=true`, les requetes SQL sous `sql/analytics/quality_*.sql` pourront cibler `audit.quality_check_runs`

## Observabilite Marche

- Le notebook `notebooks/07_market_observability_report.py` centralise un report Free Edition sur les tables Gold, l'audit qualite et `processing_state`
- Le job `market_observability_report_${bundle.target}` permet de rejouer ce reporting a la demande depuis le bundle Databricks
- Le notebook `notebooks/10_market_observability_dashboard.py` est pense pour Databricks Notebook Dashboards, avec cellules separees pour KPI, fraicheur, latest price, volume, volatilite, top movers, qualite et watermarks
- Le job `market_observability_dashboard_refresh_${bundle.target}` permet de relancer ce notebook de dashboard avec les bons parametres
- Pour creer le dashboard dans Databricks: ouvrir le notebook `10`, executer `Run all`, puis utiliser `Add to notebook dashboard` sur les sorties que tu veux epingler
- Les requetes SQL sous `sql/analytics/` couvrent maintenant les prix recents, le volume 1m, la volatilite 5m, le statut qualite recent et l'etat des watermarks
- Le notebook `notebooks/08_publish_app_serving_layer.py` republie les datasets app-facing vers `/Volumes/workspace/default/market_data_app/dev`
- Cote ecriture Spark, cette publication utilise `dbfs:/Volumes/workspace/default/market_data_app/dev`, puis l app SQL lit le meme contenu via `/Volumes/workspace/default/market_data_app/dev`
- En plus du volume, le publisher publie aussi un fallback SQL direct dans `workspace.default.app_*` pour rester exploitable sur Free Edition si le path volume n est pas expose proprement au warehouse
- L app Streamlit `apps/market_observability_streamlit/app.py` utilise Plotly et le Statement Execution API via `databricks-sdk`
- Le bundle expose aussi l app `market_observability_app` pour un deploiement Databricks Apps
