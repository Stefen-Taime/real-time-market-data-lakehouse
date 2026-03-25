# Incident Recovery

## Scenarios cibles

- interruption du flux live
- corruption ou schema inattendu dans Bronze
- echec de transformation Silver
- publication Gold incomplete
- divergence entre etat watermark et donnees effectivement materialisees

## Strategie de reprise

1. isoler le composant en erreur
2. estimer la fenetre de donnees impactee
3. relancer le backfill sur la plage concernee
4. rejouer les controles qualite
5. confirmer le retour a un etat nominal

## Ordre de reprise recommande

1. Bronze
2. Silver
3. Gold
4. Audit qualite
5. Dashboard / reporting

## Point de vigilance

Les incidents de visibilite UC / warehouse observes sur Free Edition ne doivent pas etre traites comme des corruptions de donnees. Il faut d'abord distinguer un probleme de data plane d'un probleme de pipeline.
