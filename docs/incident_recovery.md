# Incident Recovery

## Scenarios cibles

- interruption du flux live
- corruption ou schema inattendu dans Bronze
- echec de transformation Silver
- publication Gold incomplete

## Strategie de reprise

1. isoler le composant en erreur
2. estimer la fenetre de donnees impactee
3. relancer le backfill sur la plage concernee
4. rejouer les controles qualite
5. confirmer le retour a un etat nominal
