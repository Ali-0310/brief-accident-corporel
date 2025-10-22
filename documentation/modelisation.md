# Documentation : Modélisation de la Base de Données Accidents Corporels

## 📋 Table des Matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture Bronze/Silver/Gold](#architecture-bronzesilbergold)
3. [Couche Silver : Modèle en Flocon](#couche-silver--modèle-en-flocon)
4. [Couche Gold : Constellation vs Étoile](#couche-gold--constellation-vs-étoile)
5. [Relations entre Tables de Faits](#relations-entre-tables-de-faits)

---

## 🎯 Vue d'ensemble

Ce projet implémente une **architecture medallion en 3 couches** pour l'analyse des accidents corporels de la circulation routière en France (données BAAC 2005-2020).

### Objectifs métier

- Identifier les **zones géographiques à risque**
- Analyser les **conditions de survenue** (météo, luminosité, type de route, heure)
- Détecter les **tendances temporelles** et anomalies statistiques

### Principes de modélisation

- **Bronze** : Données brutes issues des fichiers CSV sans transformation
- **Silver** : Modèle normalisé 3NF pour garantir l'intégrité des données
- **Gold** : Modèle dimensionnel optimisé pour les requêtes analytiques

---

## 🏗️ Architecture Bronze/Silver/Gold

### Couche Bronze

**Rôle** : Zone de staging pour les données brutes.

**Contenu** :
- 1 table unique : `raw_accidents`
- Format : Toutes les colonnes du CSV en type TEXT
- Objectif : Traçabilité totale, possibilité de rejouer les transformations

### Couche Silver

**Rôle** : Données nettoyées et normalisées, prêtes pour les analyses transactionnelles.

**Contenu** :
- 4 tables normalisées (3NF)
- Relations strictes avec Foreign Keys
- Validation des types et contraintes CHECK

### Couche Gold

**Rôle** : Modèle dimensionnel optimisé pour les requêtes analytiques et dashboards.

**Contenu** :
- 5 tables de dimensions
- 3 tables de faits
- Métriques pré-calculées

---

## 🔷 Couche Silver : Modèle en Flocon

### Qu'est-ce qu'un modèle en flocon ?

Le **modèle en flocon (Snowflake Schema)** est une structure de base de données **normalisée** où les tables sont organisées hiérarchiquement avec des relations parent-enfant strictes.

### Tables Silver

#### Table 1 : `accidents` (table centrale)

**Grain** : 1 ligne = 1 accident corporel

**Colonnes principales** :
- `num_acc` (clé primaire) : Identifiant unique de l'accident
- `date_accident`, `heure`, `minute` : Temporalité
- `com_code`, `departement_code` : Localisation
- `luminosite`, `conditions_atmospheriques` : Conditions environnementales
- `latitude`, `longitude` : Coordonnées GPS

**Rôle** : Point d'entrée central, contient les informations générales de l'accident.

#### Table 2 : `lieux` (relation 1:1)

**Grain** : 1 ligne = 1 accident (caractéristiques du lieu)

**Colonnes principales** :
- `num_acc` (clé primaire et étrangère vers `accidents`)
- `categorie_route` : Type de voie (autoroute, nationale, départementale...)
- `profil_route` : Topographie (plat, pente, sommet de côte...)
- `trace_plan` : Géométrie (rectiligne, courbe, S)
- `etat_surface` : Condition de la chaussée (normale, mouillée, verglacée...)

**Rôle** : Détails techniques sur l'infrastructure routière.

#### Table 3 : `vehicules` (relation N:1)

**Grain** : 1 ligne = 1 véhicule impliqué dans un accident

**Colonnes principales** :
- `(num_acc, num_veh)` : Clé primaire composite
- `categorie_vehicule` : Type (VL, moto, PL, vélo...)
- `manoeuvre` : Action au moment du choc (tourner, dépasser, stationnement...)

**Relation** : Plusieurs véhicules peuvent être impliqués dans un même accident (moyenne : 2 véhicules/accident).

**Foreign Key** : `num_acc` → `accidents(num_acc)` avec `ON DELETE CASCADE`

#### Table 4 : `usagers` (relation N:1)

**Grain** : 1 ligne = 1 personne impliquée (conducteur, passager, piéton)

**Colonnes principales** :
- `id_usager` (clé primaire auto-incrémentée)
- `(num_acc, num_veh)` : Clé étrangère vers `vehicules`
- `gravite` : 1=Indemne, 2=Tué, 3=Hospitalisé, 4=Blessé léger
- `categorie_usager` : 1=Conducteur, 2=Passager, 3=Piéton
- `age`, `sexe` : Profil démographique

**Relation** : Plusieurs usagers par véhicule (conducteur + passagers).

**Foreign Key** : `(num_acc, num_veh)` → `vehicules(num_acc, num_veh)` avec `ON DELETE CASCADE`

### Caractéristiques du modèle Silver

**✅ Avantages :**
- **Intégrité référentielle garantie** : Les Foreign Keys empêchent les orphelins
- **Pas de redondance** : Chaque information stockée une seule fois
- **Cohérence ACID** : Transactions fiables
- **Facilité de mise à jour** : Modifications propagées automatiquement

**⚠️ Inconvénients pour l'analytique :**
- **Requêtes complexes** : Nécessite 3-4 JOINs pour analyses simples
- **Performance limitée** : Sur gros volumes, les JOINs ralentissent les requêtes
- **Calculs répétitifs** : COUNT, SUM, AVG recalculés à chaque requête

**Exemple de requête Silver (3 JOINs nécessaires) :**
```sql
SELECT 
    a.departement_code,
    COUNT(*) as nb_accidents,
    SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) as nb_tues
FROM accidents a
JOIN vehicules v ON a.num_acc = v.num_acc
JOIN usagers u ON v.num_acc = u.num_acc AND v.num_veh = u.num_veh
GROUP BY a.departement_code;
-- Temps d'exécution sur 1M accidents : ~5-10 secondes
``` 

### Pourquoi Silver est en Snowflake ?

Le modèle Silver est en **flocon** car :

1. **Structure hiérarchique** : accident → véhicule → usager (3 niveaux)
2. **Foreign Keys strictes** : Relations parent-enfant clairement définies
3. **Normalisation 3NF** : Aucune donnée dupliquée
4. **Intégrité transactionnelle** : CASCADE DELETE maintient la cohérence

---

## ⭐ Couche Gold : Constellation vs Étoile

### Modèle en Étoile (Star Schema)

**Structure :**
- 1 table de faits centrale
- N dimensions autour

**Caractéristiques :**
- ✅ Simple à comprendre
- ✅ Requêtes rapides (1 seul JOIN par dimension)
- ❌ **Problème pour ce projet** : Impossible de représenter plusieurs véhicules/usagers par accident

**Exemple du problème :**

Un accident avec 2 véhicules et 3 usagers devrait donner :
- 1 ligne ? → On perd le détail véhicule/usager
- 6 lignes dupliquées ? → Risque de double-comptage

### Modèle en Constellation (Galaxy Schema) ✅ CHOISI

**Structure :**
- **Plusieurs tables de faits** à grains différents
- Dimensions **partagées** entre les faits

**Avantages pour ce projet :**
- ✅ **Respect des grains** : 1 fait par niveau d'analyse
- ✅ **Pas de duplication** : Chaque métrique dans la bonne table
- ✅ **Analyses flexibles** : Drill-down et roll-up possibles
- ✅ **Pas de double-comptage** : Agrégations correctes garanties

---

## 📊 Tables de la Couche Gold

### Dimensions (5 tables de référence)

#### 1. `dim_date` - Calendrier

**Grain** : 1 ligne = 1 jour

**Colonnes clés :**
- `date_id` (PK) : Format YYYYMMDD (ex: 20150315)
- `annee`, `mois`, `jour`, `jour_semaine`
- `est_weekend`, `est_jour_ferie`
- `saison` : Hiver, Printemps, Été, Automne

**Rôle** : Permet analyses temporelles sans calculs (extraction jour de la semaine, trimestre, etc.)

#### 2. `dim_geographie` - Hiérarchie géographique

**Grain** : 1 ligne = 1 commune

**Colonnes clés :**
- `geo_id` (PK auto-incrémenté)
- `com_code` (UNIQUE) : Code INSEE commune
- `com_name`, `departement_code`, `region_code`
- `population`, `densite_population`

**Rôle** : Hiérarchie commune → département → région pour analyses territoriales

#### 3. `dim_conditions` - Conditions environnementales

**Grain** : 1 ligne = 1 combinaison (luminosité × météo)

**Colonnes clés :**
- `condition_id` (PK)
- `luminosite_code`, `luminosite_libelle` : Jour, nuit, crépuscule...
- `atm_code`, `atm_libelle` : Pluie, neige, brouillard...
- `est_nuit`, `est_intemperie` : Flags booléens

**Rôle** : Analyser l'impact des conditions climatiques et lumineuses

#### 4. `dim_route` - Caractéristiques de la voie

**Grain** : 1 ligne = 1 combinaison (catégorie × profil × tracé × surface)

**Colonnes clés :**
- `route_id` (PK)
- `categorie_route_libelle` : Autoroute, nationale, départementale...
- `profil_route_libelle` : Plat, pente, sommet de côte...
- `etat_surface_libelle` : Normal, mouillé, verglacé...

**Rôle** : Identifier les configurations routières dangereuses

#### 5. `dim_vehicule` - Types de véhicules

**Grain** : 1 ligne = 1 catégorie BAAC

**Colonnes clés :**
- `vehicule_id` (PK)
- `categorie_code` (UNIQUE) : Code BAAC (1=Vélo, 7=VL, 33=Moto>125cm3...)
- `type_vehicule` : 2-roues, VL, PL, TC
- `niveau_protection` : 1=Faible (moto), 2=Moyen (voiture), 3=Élevé (PL/bus)

**Rôle** : Analyser la vulnérabilité selon le type de véhicule

---

### Tables de Faits (3 grains différents)

#### Fait 1 : `fait_accidents`

**Grain** : 1 ligne = 1 accident corporel

**Clés étrangères (vers dimensions) :**
- `date_id` → `dim_date`
- `geo_id` → `dim_geographie`
- `condition_id` → `dim_conditions`
- `route_id` → `dim_route`

**Métriques pré-calculées :**
- `nb_vehicules` : Nombre de véhicules impliqués
- `nb_usagers_total` : Total de personnes impliquées
- `nb_tues_total`, `nb_blesses_hosp_total`, `nb_blesses_legers_total`
- `score_gravite_total` : Pondération (tué=100, hosp=10, léger=1)

**Flags analytiques :**
- `est_accident_mortel` : Au moins 1 tué
- `est_accident_grave` : Au moins 1 tué OU hospitalisé
- `est_weekend`, `est_nuit`

**Rôle** : Vue agrégée au niveau accident pour analyses macro (tendances, zones à risque)

#### Fait 2 : `fait_vehicules`

**Grain** : 1 ligne = 1 véhicule impliqué

**Clés :**
- `vehicule_id` (PK)
- `num_acc` : Clé métier pour jointure
- `vehicule_type_id` → `dim_vehicule`

**Métriques :**
- `nb_occupants` : Nombre de personnes dans ce véhicule
- `nb_tues_vehicule`, `nb_blesses_vehicule`

**Caractéristiques :**
- `manoeuvre`, `point_choc`, `obstacle_fixe`

**Rôle** : Analyses par type de véhicule (ex: taux de mortalité des motos)

#### Fait 3 : `fait_usagers`

**Grain** : 1 ligne = 1 personne impliquée

**Clés :**
- `usager_id` (PK)
- `vehicule_fk` → `fait_vehicules` (FK formelle)
- `accident_fk` : Relation logique (pas de FK)
- `num_acc` : Clé métier

**Métriques individuelles :**
- `gravite` : 1=Indemne, 2=Tué, 3=Hospitalisé, 4=Blessé léger
- `age`, `sexe`, `categorie_usager`
- `score_gravite_usager`

**Flags calculés :**
- `est_conducteur`, `est_pieton`, `est_tue`, `est_victime`

**Rôle** : Analyses démographiques et comportementales fines

---

## 🔗 Relations entre Tables de Faits

### Pourquoi pas de FK visible entre `fait_usagers` et `fait_accidents` ?

#### Constat

Dans le schéma, vous observez :
- ✅ `fait_usagers.vehicule_fk` → `fait_vehicules.vehicule_id` (FK formelle)
- ❌ `fait_usagers.accident_fk` → Pas de FK vers `fait_accidents` (relation logique seulement)

#### Explication

**Raison 1 : Grain différent (problème fondamental)**

- `fait_accidents` : 1 ligne par accident
- `fait_usagers` : Plusieurs lignes par accident (N:M)

Une FK formelle impliquerait une relation 1:N stricte, ce qui est faux ici.

**Raison 2 : Redondance déjà couverte**

La hiérarchie est respectée par :

```
fait_usagers.vehicule_fk → fait_vehicules.vehicule_id
fait_vehicules.num_acc → fait_accidents.num_acc (clé métier)
```

Ajouter une FK directe `usager → accident` serait redondant et créerait des contraintes doubles.

**Raison 3 : Performance et flexibilité**

Sans FK formelle :
- ✅ Chargement parallèle des 3 faits possible (pas de dépendance d'ordre)
- ✅ Pas de locks sur `fait_accidents` lors d'insertions massives dans `fait_usagers`
- ✅ Suppression/archivage indépendant par table

**Raison 4 : Pattern du modèle en constellation**

Dans un **Galaxy Schema**, les faits sont liés **logiquement via les dimensions partagées** plutôt que par des FK directes :

```
fait_usagers.num_acc = fait_accidents.num_acc (jointure métier)
fait_usagers.accident_fk (colonne dénormalisée pour performance)
```

---

### Pourquoi `accident_fk` existe quand même ?

**Rôle de `accident_fk` dans `fait_usagers` :**

1. **Dénormalisation pour performance** : Évite de passer par `fait_vehicules` pour remonter à l'accident
2. **Simplification des requêtes** : JOIN direct usager → accident sans intermédiaire
3. **Pas de contrainte** : C'est une simple colonne INTEGER, pas une FK PostgreSQL

**Exemple d'utilisation :**

```sql
-- Requête simplifiée grâce à accident_fk
SELECT 
    a.departement_code,
    COUNT(DISTINCT u.usager_id) as nb_victimes
FROM fait_usagers u
JOIN fait_accidents a ON u.accident_fk = a.accident_id
WHERE u.gravite > 1
GROUP BY a.departement_code;

-- Sans accident_fk, il faudrait :
SELECT 
    a.departement_code,
    COUNT(DISTINCT u.usager_id) as nb_victimes
FROM fait_usagers u
JOIN fait_vehicules v ON u.vehicule_fk = v.vehicule_id
JOIN fait_accidents a ON v.num_acc = a.num_acc
WHERE u.gravite > 1
GROUP BY a.departement_code;
-- 1 JOIN de plus = perte de performance
```

---

### Garantir l'intégrité sans FK

**Validation dans l'ETL Python :**

```python
# Lors du chargement fait_usagers
valid_accident_ids = set(df_fait_accidents['accident_id'])
df_usagers = df_usagers[df_usagers['accident_fk'].isin(valid_accident_ids)]
# → Garantit que tous les accident_fk existent
```

**Vue de contrôle qualité :**

```sql
-- Détecter les usagers orphelins (ne devrait jamais arriver)
SELECT u.usager_id, u.num_acc
FROM fait_usagers u
LEFT JOIN fait_accidents a ON u.accident_fk = a.accident_id
WHERE a.accident_id IS NULL;
```

---

## 📝 Résumé : Pourquoi ce Modèle ?

### Silver (Snowflake) :
- **Objectif** : Intégrité transactionnelle et traçabilité
- **Avantage** : Pas de redondance, FK strictes
- **Usage** : ETL, validation, requêtes OLTP

### Gold (Constellation) :
- **Objectif** : Performance analytique maximale
- **Avantage** : Métriques pré-calculées, analyses multi-niveaux
- **Usage** : Dashboards, BI, requêtes OLAP

### Relation logique entre faits :
- **Sans FK formelle** : Flexibilité, performance, chargement parallèle
- **Avec clés métier** : `num_acc` assure la cohérence
- **Dénormalisation contrôlée** : `accident_fk` pour optimiser les requêtes

---

## 📚 Références

- **Modèle Medallion** : Databricks Architecture Pattern
- **Snowflake vs Star** : Kimball, The Data Warehouse Toolkit
- **Galaxy Schema** : Inmon, Building the Data Warehouse
- **Données BAAC** : ONISR - Observatoire National Interministériel de la Sécurité Routière