# Deployment Guide

## Prerequis

- acces au workspace Databricks cible
- variables et secrets renseignes
- configuration d'environnement completee
- dependances Python du repo installees
- CLI Databricks authentifiee

## Etapes

1. choisir le target `dev`, `staging` ou `prod`
2. executer `./scripts/setup_workspace.sh <target>` si le mode gouverne doit etre prepare
3. valider le bundle avec `./scripts/validate_bundle.sh <target>`
4. deployer avec `./scripts/deploy_bundle.sh <target>`
5. lancer un smoke job ou la pipeline medallion
6. verifier les jobs qualite et reporting apres deploiement

## Notes de plateforme

- sur Free Edition, `dev` reste le mode le plus robuste
- `staging` et `prod` sont prepares au niveau config/bundle mais restent sur le meme workspace tant que tu n'ajoutes pas un environnement Databricks distinct
