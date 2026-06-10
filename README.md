# Projet Big Data - Enrichissement Multi-Source

## Sujet 4 : Comparaison BroadcastHashJoin vs SortMergeJoin

### Objectif
Analyser l'impact de la météo sur les trajets de taxi NYC en comparant BroadcastHashJoin et SortMergeJoin.

### Technologies
- Docker / Spark / Hadoop HDFS / Python

### Données
- Taxi NYC (Janvier 2010) : 14,863,778 trajets
- Météo (2010) : 365 jours

### Résultats
| Algorithme | Temps | Shuffle |
|------------|-------|---------|
| BroadcastHashJoin | 8.52 sec | Non |
| SortMergeJoin | 32.55 sec | Oui |

### Impact de la pluie
| Météo | Trajets | Distance | Pourboire |
|-------|---------|----------|-----------|
| Sec | 3,908,885 | 2.60 miles | 0.70 USD |
| Pluvieux | 10,954,893 | 2.64 miles | 0.66 USD |

### Conclusion
BroadcastHashJoin est **3.82x plus rapide**. La pluie augmente les trajets mais diminue le pourboire.

### Auteur
[Nom] - [Date]
