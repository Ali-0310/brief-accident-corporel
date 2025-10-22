# 📊 Documentation du Pipeline de Données : Bronze → Silver → Gold

## 🎯 Vue d'ensemble

Ce document détaille les **transformations de données** appliquées aux accidents corporels de la circulation à travers l'architecture médaillon en **3 couches** (Bronze, Silver, Gold), et explique les **métriques métier** calculées dans la couche Gold pour l'analyse de la sécurité routière.

**Architecture :** `CSV brut` → **Bronze** (staging) → **Silver** (normalisé 3NF) → **Gold** (constellation analytique)

---

## 1️⃣ Couche Bronze : Données Brutes (Staging)

### Table : `accidents_bronze.raw_accidents`

**Objectif :** Zone de staging stockant les données CSV brutes **sans transformation ni validation**.

**Grain :** 1 ligne = 1 enregistrement CSV brut (mélange accident + véhicule + usager + lieu)

**Caractéristiques :**
- ✅ **Toutes les colonnes en TEXT** : accepte n'importe quelle valeur, même invalide
- ✅ **Pas de validation** : préservation fidèle de la source
- ✅ **Traçabilité totale** : possibilité de rejouer les transformations
- ✅ **Métadonnées** : `load_timestamp` pour suivi des imports

### Colonnes Principales (84 colonnes au total)

| Groupe | Colonnes | Type | Description |
|--------|----------|------|-------------|
| **Identifiant technique** | `row_id` | BIGSERIAL | Clé primaire auto-incrémentée |
| **Identifiants métier** | `num_acc`, `num_veh` | TEXT | Numéro accident + véhicule |
| **Temporel** | `jour`, `mois`, `an`, `hrmn` | TEXT | Date et heure (format brut) |
| **Conditions** | `lum`, `atm`, `col`, `intsect` | TEXT | Luminosité, météo, collision, intersection |
| **Géographie** | `dep`, `com`, `agg`, `lat`, `long`, `adr` | TEXT | Localisation GPS et administrative |
| **Lieux** | `catr`, `prof`, `trace_plan`, `surf`, `nbv` | TEXT | Catégorie route, profil, tracé, surface, nb voies |
| **Véhicules** | `catv`, `manv`, `choc`, `obs`, `senc` | TEXT | Catégorie, manœuvre, choc, obstacle, sens |
| **Usagers** | `grav`, `sexe`, `an_nais`, `catu`, `place` | TEXT | Gravité, sexe, naissance, catégorie, place |
| **Enrichissement géo** | `com_name`, `dep_name`, `reg_name`, `epci_name` | TEXT | Noms communes/départements/régions |
| **Métadonnées** | `load_timestamp` | TIMESTAMP | Date/heure d'import |

**Index :** `idx_bronze_num_acc` sur `num_acc` pour faciliter les jointures ETL.

**Note importante :** Cette table contient des **données dénormalisées** (1 ligne peut contenir plusieurs véhicules/usagers selon le CSV source). Le nettoyage et la normalisation s'effectuent dans Silver.

**Note importante :** Cette table contient des **données dénormalisées** (1 ligne peut contenir plusieurs véhicules/usagers selon le CSV source). Le nettoyage et la normalisation s'effectuent dans Silver.

---

## 2️⃣ Couche Silver : Données Nettoyées et Normalisées (3NF)

### Vue d'ensemble

**Objectif :** Modèle relationnel **normalisé en 3ème Forme Normale (3NF)** avec données nettoyées, typées et validées.

**Architecture :** 4 tables en **schéma Snowflake** avec contraintes d'intégrité référentielle strictes.

```
accidents (1) ←──┐
    ↓            │
lieux (1:1)      │
                 │
vehicules (N) ←──┘
    ↓
usagers (N)
```

---

### Table 1 : `accidents_silver.accidents`

**Grain :** 1 ligne = 1 accident corporel  
**Rôle :** Table centrale (fait) avec caractéristiques temporelles, géographiques et conditions.

#### Transformations Bronze → Silver

| Colonne Bronze | Type Bronze | Transformation | Colonne Silver | Type Silver | Validation |
|----------------|-------------|----------------|----------------|-------------|------------|
| `num_acc` | TEXT | Nettoyage espaces | `num_acc` | VARCHAR(20) | PRIMARY KEY, NOT NULL |
| `jour`, `mois`, `an` | TEXT | Reconstruction date | `date_accident` | DATE | NOT NULL, CHECK (2005-2030) |
| `date` | TEXT | Conversion directe (si valide) | `date_accident` | DATE | Fallback sur reconstruction |
| `hrmn` | TEXT | Extraction 2 premiers chars | `heure` | INTEGER | CHECK (0-23) |
| `hrmn` | TEXT | Extraction 2 derniers chars | `minute` | INTEGER | CHECK (0-59) |
| `an` | TEXT | Conversion numérique | `annee` | INTEGER | CHECK (2005-2030) |
| `mois` | TEXT | Conversion numérique | `mois` | INTEGER | CHECK (1-12) |
| `jour` | TEXT | Conversion numérique | `jour` | INTEGER | CHECK (1-31) |
| - | - | **Calcul** : EXTRACT(DOW) | `jour_semaine` | INTEGER | 1=Lundi, 7=Dimanche |
| `com` | TEXT | Nettoyage + padding | `com_code` | VARCHAR(5) | Format code INSEE |
| `dep` | TEXT | Nettoyage + validation | `departement_code` | VARCHAR(3) | Code département |
| `agg` | TEXT | Conversion booléenne | `en_agglomeration` | BOOLEAN | TRUE si agg=2 |
| `lat` | TEXT | Conversion DECIMAL | `latitude` | DECIMAL(10,8) | CHECK (-90 à 90) |
| `long` | TEXT | Conversion DECIMAL | `longitude` | DECIMAL(11,8) | CHECK (-180 à 180) |
| `adr` | TEXT | Nettoyage | `adresse` | TEXT | Sans contrainte |
| `lum` | TEXT | Conversion INTEGER | `luminosite` | INTEGER | CHECK (1-5) |
| `atm` | TEXT | Conversion INTEGER | `conditions_atmospheriques` | INTEGER | CHECK (1-9) |
| `intsect` | TEXT | Conversion INTEGER | `type_intersection` | INTEGER | CHECK (1-9) |
| `col` | TEXT | Conversion INTEGER | `type_collision` | INTEGER | CHECK (1-7) |
| - | - | Métadonnée ajoutée | `date_chargement` | TIMESTAMP | DEFAULT NOW() |
| `row_id` | BIGSERIAL | Copie pour traçabilité | `source_row_id` | BIGINT | Lien vers Bronze |

#### Règles de Nettoyage

**1. Déduplication :**
```sql
-- Garder une seule ligne par num_acc (première occurrence)
SELECT DISTINCT ON (num_acc) * FROM bronze.raw_accidents ORDER BY num_acc, row_id;
```

**2. Validation coordonnées GPS :**
```sql
-- Les deux doivent être NULL OU les deux valides
CONSTRAINT ck_coords_valides CHECK (
    (latitude IS NULL AND longitude IS NULL) OR
    (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
)
```

**3. Calcul automatique `jour_semaine` :**
```sql
-- Extraction jour de la semaine (norme ISO : 1=Lundi, 7=Dimanche)
jour_semaine = EXTRACT(ISODOW FROM date_accident)
```

**4. Rejet des lignes invalides :**
- `date_accident` NULL → ligne rejetée
- `num_acc` NULL → ligne rejetée
- Dates hors plage 2005-2030 → ligne rejetée

---

### Table 2 : `accidents_silver.lieux`

**Grain :** 1 ligne = 1 accident (relation **1:1** avec `accidents`)  
**Rôle :** Caractéristiques détaillées de la voie/route.

#### Transformations Bronze → Silver

| Colonne Bronze | Type Bronze | Transformation | Colonne Silver | Type Silver | Validation |
|----------------|-------------|----------------|----------------|-------------|------------|
| `num_acc` | TEXT | Nettoyage | `num_acc` | VARCHAR(20) | PRIMARY KEY, FOREIGN KEY |
| `catr` | TEXT | Conversion INTEGER | `categorie_route` | INTEGER | CHECK (1-9) |
| `voie` | TEXT | Nettoyage | `numero_route` | VARCHAR(10) | Sans contrainte |
| `circ` | TEXT | Conversion INTEGER | `regime_circulation` | INTEGER | CHECK (1-4) |
| `nbv` | TEXT | Conversion INTEGER | `nombre_voies` | INTEGER | CHECK (>0 AND <=20) |
| `vosp` | TEXT | Conversion INTEGER | `voie_reservee` | INTEGER | CHECK (0-3) |
| `prof` | TEXT | Conversion INTEGER | `profil_route` | INTEGER | CHECK (1-4) |
| `trace_plan` | TEXT | Conversion INTEGER | `trace_plan` | INTEGER | CHECK (1-4) |
| `lartpc` | TEXT | Conversion DECIMAL | `largeur_terre_plein` | DECIMAL(5,2) | CHECK (>=0 AND <100) |
| `larrout` | TEXT | Conversion DECIMAL | `largeur_chaussee` | DECIMAL(5,2) | CHECK (>=0 AND <100) |
| `surf` | TEXT | Conversion INTEGER | `etat_surface` | INTEGER | CHECK (1-9) |
| `infra` | TEXT | Conversion INTEGER | `infrastructure` | INTEGER | CHECK (0-7) |
| `situ` | TEXT | Conversion INTEGER | `situation` | INTEGER | CHECK (0-5) |
| - | - | **Calcul métier** | `proximite_ecole` | BOOLEAN | Détection si école <500m |

#### Règles de Nettoyage

**1. Déduplication :**
```sql
-- 1 seul lieu par accident
SELECT DISTINCT ON (num_acc) * FROM bronze.raw_accidents ORDER BY num_acc, row_id;
```

**2. Contrainte d'intégrité référentielle :**
```sql
-- Suppression en cascade si l'accident parent est supprimé
FOREIGN KEY (num_acc) REFERENCES accidents_silver.accidents(num_acc) ON DELETE CASCADE
```

**3. Validation cohérence :**
- `nombre_voies` > 0 (au moins 1 voie)
- `largeur_chaussee` et `largeur_terre_plein` < 100m (limite physique)

---

### Table 3 : `accidents_silver.vehicules`

**Grain :** 1 ligne = 1 véhicule impliqué dans un accident  
**Rôle :** Véhicules impliqués (relation **N:1** avec `accidents`).

#### Transformations Bronze → Silver

| Colonne Bronze | Type Bronze | Transformation | Colonne Silver | Type Silver | Validation |
|----------------|-------------|----------------|----------------|-------------|------------|
| `num_acc` | TEXT | Nettoyage | `num_acc` | VARCHAR(20) | Partie de PK composite |
| `num_veh` | TEXT | Nettoyage | `num_veh` | VARCHAR(10) | Partie de PK composite |
| `senc` | TEXT | Conversion INTEGER | `sens_circulation` | INTEGER | CHECK (1, 2) |
| `catv` | TEXT | Conversion INTEGER | `categorie_vehicule` | INTEGER | CHECK (0-99) |
| `obs` | TEXT | Conversion INTEGER | `obstacle_fixe` | INTEGER | CHECK (0-16) |
| `obsm` | TEXT | Conversion INTEGER | `obstacle_mobile` | INTEGER | CHECK (0-9) |
| `choc` | TEXT | Conversion INTEGER | `point_choc` | INTEGER | CHECK (0-9) |
| `manv` | TEXT | Conversion INTEGER | `manoeuvre` | INTEGER | CHECK (0-24) |
| `occutc` | TEXT | Conversion INTEGER | `nb_occupants_tc` | INTEGER | CHECK (>=0) |

#### Règles de Nettoyage

**1. Clé composite :**
```sql
PRIMARY KEY (num_acc, num_veh)
```

**2. Déduplication :**
```python
# Supprimer doublons sur la paire (num_acc, num_veh)
df_vehicules = df.drop_duplicates(subset=['num_acc', 'num_veh'], keep='first')
```

**3. Contrainte d'intégrité :**
```sql
FOREIGN KEY (num_acc) REFERENCES accidents_silver.accidents(num_acc) ON DELETE CASCADE
```

**4. Validation métier :**
- `sens_circulation` : 1=croissant (PR/PK), 2=décroissant
- `categorie_vehicule` : codes BAAC (01=Vélo, 07=VL, 13-17=PL, 30-34=Moto, 37-38=TC, 99=Autre)
- `nb_occupants_tc` >= 0 (uniquement pour transports en commun)

---

### Table 4 : `accidents_silver.usagers`

**Grain :** 1 ligne = 1 usager (victime ou indemne)  
**Rôle :** Personnes impliquées dans les accidents (relation **N:1** avec `vehicules`).

#### Transformations Bronze → Silver

| Colonne Bronze | Type Bronze | Transformation | Colonne Silver | Type Silver | Validation |
|----------------|-------------|----------------|----------------|-------------|------------|
| - | - | Auto-incrémenté | `id_usager` | BIGSERIAL | PRIMARY KEY |
| `num_acc` | TEXT | Nettoyage | `num_acc` | VARCHAR(20) | Partie de FK composite |
| `num_veh` | TEXT | Nettoyage | `num_veh` | VARCHAR(10) | Partie de FK composite |
| `place` | TEXT | Conversion INTEGER | `place_vehicule` | INTEGER | Sans contrainte stricte |
| `catu` | TEXT | Conversion INTEGER | `categorie_usager` | INTEGER | CHECK (1-4) |
| `grav` | TEXT | Conversion INTEGER | `gravite` | INTEGER | NOT NULL, CHECK (1-4) |
| `sexe` | TEXT | Conversion INTEGER | `sexe` | INTEGER | CHECK (1, 2) |
| `an_nais` | TEXT | Conversion INTEGER | `annee_naissance` | INTEGER | CHECK (1900-2025) |
| `an_nais` | TEXT | **Calcul** : annee_accident - an_nais | `age_au_moment_accident` | INTEGER | CHECK (0-120) |
| `trajet` | TEXT | Conversion INTEGER | `motif_deplacement` | INTEGER | Codes 1-9 |
| `secu` | TEXT | Nettoyage | `equipement_securite` | VARCHAR(2) | Format "XY" |
| `locp` | TEXT | Conversion INTEGER | `localisation_pieton` | INTEGER | Codes 1-8 |
| `actp` | TEXT | Conversion INTEGER | `action_pieton` | INTEGER | Codes 0-9 |
| `etatp` | TEXT | Conversion INTEGER | `etat_pieton` | INTEGER | Codes 1-3 |

#### Règles de Nettoyage

**1. Clé primaire technique :**
```sql
id_usager BIGSERIAL PRIMARY KEY  -- Auto-incrémenté car pas d'ID naturel
```

**2. Contrainte d'intégrité composite :**
```sql
FOREIGN KEY (num_acc, num_veh) 
    REFERENCES accidents_silver.vehicules(num_acc, num_veh) 
    ON DELETE CASCADE
```

**3. Calcul de l'âge :**
```python
# Calcul de l'âge au moment de l'accident
df['age_au_moment_accident'] = df['annee_accident'] - df['annee_naissance']

# Filtrer les âges aberrants (négatifs ou >120 ans)
df.loc[(df['age'] < 0) | (df['age'] > 120), 'age'] = None
```

**4. Validation métier :**
- `gravite` : 1=Indemne, 2=Tué (<30j), 3=Hospitalisé >24h, 4=Blessé léger
- `categorie_usager` : 1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton roller/trottinette
- `sexe` : 1=Masculin, 2=Féminin
- `equipement_securite` : format "XY" où X=type (1=Ceinture, 2=Casque, 3=Enfant, 4=Réfléchissant), Y=utilisation (1=Oui, 2=Non, 3=Indéterminé)

---

### Résumé des Améliorations Bronze → Silver

| Amélioration | Description | Impact |
|--------------|-------------|--------|
| **Typage fort** | Conversion TEXT → types appropriés (DATE, INTEGER, BOOLEAN, DECIMAL) | Intégrité des données garantie |
| **Normalisation 3NF** | Séparation en 4 tables liées (accidents, lieux, vehicules, usagers) | Élimination redondance, cohérence |
| **Contraintes CHECK** | Validation des plages de valeurs (heures 0-23, coordonnées GPS, âges, etc.) | Qualité des données assurée |
| **Clés étrangères** | Cascades ON DELETE pour maintenir l'intégrité | Cohérence référentielle |
| **Calculs dérivés** | `jour_semaine`, `age_au_moment_accident` | Enrichissement métier |
| **Déduplication** | Suppression doublons par DISTINCT ON ou drop_duplicates() | Unicité des enregistrements |
| **Traçabilité** | `source_row_id`, `date_chargement` | Audit et debug |

---

## 3️⃣ Couche Gold : Modèle Analytique (Constellation)

### Vue d'ensemble

**Objectif :** Modèle **constellation/galaxy** optimisé pour les requêtes analytiques avec métriques pré-calculées.

**Architecture :** 5 dimensions + 3 tables de faits (grain différent par fait)

```
       dim_date ────┐
   dim_geographie ──┼─→ fait_accidents
   dim_conditions ──┤       ↓
       dim_route ───┘   fait_vehicules
                            ↓
    dim_vehicule ────→ fait_usagers
```

**Pourquoi constellation au lieu de star unique ?**
- **Grains multiples** : accident ≠ véhicule ≠ usager
- **Performance** : métriques pré-agrégées à chaque niveau
- **Flexibilité** : analyses à différents niveaux de granularité

---

### 🔷 Dimension 1 : `accidents_gold.dim_date`

**Grain :** 1 ligne = 1 jour  
**Rôle :** Dimension calendrier complète pour analyses temporelles (2005-2025).

#### Colonnes et Transformations

| Colonne | Type | Source/Calcul | Description |
|---------|------|---------------|-------------|
| `date_id` | INTEGER (PK) | `TO_CHAR(date, 'YYYYMMDD')::INTEGER` | Format 20150315 = 15 mars 2015 |
| `date_complete` | DATE UNIQUE | Silver : `date_accident` | Date au format SQL |
| `annee` | INTEGER | `EXTRACT(YEAR FROM date)` | Année 2005-2025 |
| `mois` | INTEGER | `EXTRACT(MONTH FROM date)` | Mois 1-12 |
| `jour` | INTEGER | `EXTRACT(DAY FROM date)` | Jour du mois 1-31 |
| `trimestre` | INTEGER | `EXTRACT(QUARTER FROM date)` | Trimestre 1-4 |
| `jour_semaine` | INTEGER | `EXTRACT(ISODOW FROM date)` | 1=Lundi, 7=Dimanche (ISO) |
| `nom_jour` | VARCHAR(10) | Mapping via CASE | 'Lundi', 'Mardi', ... |
| `nom_mois` | VARCHAR(10) | Mapping via CASE | 'Janvier', 'Février', ... |
| `semaine_annee` | INTEGER | `EXTRACT(WEEK FROM date)` | Semaine 1-53 |
| `jour_annee` | INTEGER | `EXTRACT(DOY FROM date)` | Jour de l'année 1-366 |
| `est_weekend` | BOOLEAN | `jour_semaine IN (6, 7)` | TRUE si samedi/dimanche |
| `est_jour_ferie` | BOOLEAN | Mapping table jours fériés | TRUE si férié |
| `nom_jour_ferie` | VARCHAR(50) | Référentiel | '1er mai', 'Noël', etc. |
| `saison` | VARCHAR(10) | Calcul par mois | 'Hiver', 'Printemps', 'Été', 'Automne' |

#### Méthode de Population

```sql
-- Génération série de dates 2005-01-01 à 2025-12-31
INSERT INTO accidents_gold.dim_date (date_id, date_complete, annee, mois, ...)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d::DATE,
    EXTRACT(YEAR FROM d)::INTEGER,
    EXTRACT(MONTH FROM d)::INTEGER,
    -- ... autres calculs
FROM generate_series('2005-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) d;
```

**Pertinence pour le projet :**
- ✅ Analyses par jour de la semaine (détection pics accidents vendredi soir)
- ✅ Comparaisons weekend vs semaine
- ✅ Détection anomalies par période (vacances scolaires, jours fériés)
- ✅ Tendances saisonnières (hiver = verglas)

---

### 🔷 Dimension 2 : `accidents_gold.dim_geographie`

**Grain :** 1 ligne = 1 commune  
**Rôle :** Hiérarchie géographique commune → département → région.

#### Colonnes et Transformations

| Colonne | Type | Source | Description |
|---------|------|--------|-------------|
| `geo_id` | SERIAL (PK) | Auto-incrémenté | Clé technique |
| `com_code` | VARCHAR(5) UNIQUE | Silver/Bronze : `com_code` | Code INSEE commune |
| `com_name` | VARCHAR(100) | Bronze enrichi : `com_name` | Nom commune |
| `com_arm_name` | VARCHAR(100) | Bronze enrichi : `com_arm_name` | Nom arrondissement si applicable |
| `departement_code` | VARCHAR(3) | Silver : `departement_code` | Code département (01-95, 2A, 2B) |
| `departement_name` | VARCHAR(100) | Bronze enrichi : `dep_name` | Nom département |
| `region_code` | VARCHAR(2) | Bronze enrichi : `reg_code` | Code région |
| `region_name` | VARCHAR(100) | Bronze enrichi : `reg_name` | Nom région |
| `epci_code` | VARCHAR(10) | Bronze enrichi : `epci_code` | Code intercommunalité |
| `epci_name` | VARCHAR(100) | Bronze enrichi : `epci_name` | Nom EPCI |
| `type_zone` | VARCHAR(20) | **Calcul** : `population` | 'Urbain' (>10k), 'Périurbain' (2-10k), 'Rural' (<2k) |
| `population` | INTEGER | Référentiel INSEE | Nombre habitants |
| `superficie_km2` | DECIMAL(10,2) | Référentiel INSEE | Surface km² |
| `densite_population` | DECIMAL(10,2) | `population / superficie_km2` | Hab/km² |

#### Méthode de Population

```sql
-- Extraction communes distinctes depuis Bronze (données enrichies géographiques)
INSERT INTO accidents_gold.dim_geographie (com_code, com_name, departement_code, ...)
SELECT DISTINCT
    com_code,
    com_name,
    dep_code,
    dep_name,
    reg_code,
    reg_name,
    epci_code,
    epci_name
FROM accidents_bronze.raw_accidents
WHERE com_code IS NOT NULL;
```

**Pertinence pour le projet :**
- ✅ **Zones à risque** : identification communes/départements avec plus d'accidents
- ✅ **Analyses par type de zone** : urbain vs rural (question métier clé)
- ✅ **Hiérarchie drill-down** : région → département → commune
- ✅ **Normalisation densité** : accidents/habitant pour comparer zones

---

### 🔷 Dimension 3 : `accidents_gold.dim_conditions`

**Grain :** 1 ligne = 1 combinaison (luminosité × météo)  
**Rôle :** Profils de conditions météorologiques et lumineuses.

#### Colonnes et Transformations

| Colonne | Type | Source/Calcul | Description |
|---------|------|---------------|-------------|
| `condition_id` | SERIAL (PK) | Auto-incrémenté | Clé technique |
| `luminosite_code` | INTEGER | Silver : `luminosite` | Code 1-5 |
| `luminosite_libelle` | VARCHAR(50) | Mapping CASE | Libellé explicite |
| `est_nuit` | BOOLEAN | `luminosite_code IN (3, 4, 5)` | TRUE si nuit |
| `atm_code` | INTEGER | Silver : `conditions_atmospheriques` | Code 1-9 |
| `atm_libelle` | VARCHAR(50) | Mapping CASE | Libellé météo |
| `est_intemperie` | BOOLEAN | `atm_code IN (2, 3, 4, 5, 6)` | TRUE si météo dégradée |
| `niveau_risque` | INTEGER | **Calcul heuristique** | Score 1-5 combinant lum + météo |
| - | - | UNIQUE (luminosite_code, atm_code) | Pas de doublons |

#### Codes et Libellés

**Luminosité :**
1. Plein jour
2. Crépuscule ou aube
3. Nuit sans éclairage public
4. Nuit avec éclairage public non allumé
5. Nuit avec éclairage public allumé

**Conditions atmosphériques :**
1. Normale
2. Pluie légère
3. Pluie forte
4. Neige - Grêle
5. Brouillard - Fumée
6. Vent fort - Tempête
7. Temps éblouissant
8. Temps couvert
9. Autre

#### Méthode de Population

```sql
-- Générer toutes les combinaisons possibles (5 × 9 = 45 lignes)
INSERT INTO accidents_gold.dim_conditions (luminosite_code, luminosite_libelle, atm_code, atm_libelle, ...)
SELECT 
    l.code,
    l.libelle,
    a.code,
    a.libelle,
    l.code IN (3, 4, 5) AS est_nuit,
    a.code IN (2, 3, 4, 5, 6) AS est_intemperie,
    -- Calcul niveau_risque (exemple)
    CASE 
        WHEN l.code IN (3, 4) AND a.code IN (3, 4, 5) THEN 5  -- Nuit + neige/brouillard = très dangereux
        WHEN l.code = 3 OR a.code IN (2, 3) THEN 4
        ELSE 2
    END
FROM luminosites_ref l
CROSS JOIN atmospheres_ref a;
```

**Pertinence pour le projet :**
- ✅ **Question métier #1** : "Quelles conditions (météo + luminosité) ont un risque significativement supérieur ?"
- ✅ **Analyse combinée** : pluie forte + nuit sans éclairage = danger maximal
- ✅ **Détection patterns** : plus d'accidents graves en conditions dégradées ?
- ✅ **Scoring risque** : pondération pour recommandations prévention

---

### 🔷 Dimension 4 : `accidents_gold.dim_route`

**Grain :** 1 ligne = 1 combinaison (catégorie route × profil × tracé × surface)  
**Rôle :** Profils de configurations routières.

#### Colonnes et Transformations

| Colonne | Type | Source/Calcul | Description |
|---------|------|---------------|-------------|
| `route_id` | SERIAL (PK) | Auto-incrémenté | Clé technique |
| `categorie_route_code` | INTEGER | Silver lieux : `categorie_route` | Code 1-9 |
| `categorie_route_libelle` | VARCHAR(50) | Mapping CASE | Autoroute, RN, RD, Communale, etc. |
| `profil_route_code` | INTEGER | Silver lieux : `profil_route` | Code 1-4 |
| `profil_route_libelle` | VARCHAR(50) | Mapping CASE | Plat, Pente, Sommet côte, Bas côte |
| `trace_plan_code` | INTEGER | Silver lieux : `trace_plan` | Code 1-4 |
| `trace_plan_libelle` | VARCHAR(50) | Mapping CASE | Rectiligne, Courbe G, Courbe D, En S |
| `etat_surface_code` | INTEGER | Silver lieux : `etat_surface` | Code 1-9 |
| `etat_surface_libelle` | VARCHAR(50) | Mapping CASE | Normal, Mouillé, Verglacé, etc. |
| `niveau_risque_route` | INTEGER | **Calcul heuristique** | Score 1-5 |
| - | - | UNIQUE (cat, profil, tracé, surface) | Pas de doublons |

#### Codes et Libellés

**Catégorie route :**
1. Autoroute, 2. Route Nationale, 3. Départementale, 4. Communale, 5. Hors réseau public, 6. Parking, 9. Autre

**Profil route :**
1. Plat, 2. Pente, 3. Sommet de côte, 4. Bas de côte

**Tracé en plan :**
1. Rectiligne, 2. Courbe à gauche, 3. Courbe à droite, 4. En S

**État surface :**
1. Normal, 2. Mouillé, 3. Flaques, 4. Inondé, 5. Enneigé, 6. Boue, 7. Verglacé, 8. Corps gras, 9. Autre

#### Méthode de Population

```sql
-- Extraction combinaisons réelles depuis Silver
INSERT INTO accidents_gold.dim_route (categorie_route_code, profil_route_code, ...)
SELECT DISTINCT
    l.categorie_route,
    l.profil_route,
    l.trace_plan,
    l.etat_surface,
    -- Libellés via CASE
    CASE l.categorie_route WHEN 1 THEN 'Autoroute' ... END,
    -- Calcul niveau_risque
    CASE 
        WHEN l.etat_surface = 7 THEN 5  -- Verglacé = danger max
        WHEN l.profil_route = 3 AND l.trace_plan = 4 THEN 4  -- Sommet + virage S
        ELSE 2
    END
FROM accidents_silver.lieux l
WHERE l.categorie_route IS NOT NULL;
```

**Pertinence pour le projet :**
- ✅ **Question métier #1** : "Type de route + état surface augmentent-ils significativement le risque ?"
- ✅ **Analyse infrastructure** : autoroutes vs routes départementales
- ✅ **Détection configurations dangereuses** : courbes + pente + verglas
- ✅ **Ciblage travaux** : priorités aménagements

---

### 🔷 Dimension 5 : `accidents_gold.dim_vehicule`

**Grain :** 1 ligne = 1 catégorie de véhicule BAAC  
**Rôle :** Classification véhicules par type et niveau de protection.

#### Colonnes et Transformations

| Colonne | Type | Source/Calcul | Description |
|---------|------|---------------|-------------|
| `vehicule_id` | SERIAL (PK) | Auto-incrémenté | Clé technique |
| `categorie_code` | INTEGER UNIQUE | Silver véhicules : `categorie_vehicule` | Code BAAC 0-99 |
| `categorie_libelle` | VARCHAR(100) | Mapping CASE | Libellé détaillé |
| `type_vehicule` | VARCHAR(50) | **Calcul regroupement** | 2-roues, VL, PL, TC, Piéton, Autre |
| `est_motorise` | BOOLEAN | `NOT IN (01, 99)` | TRUE sauf vélo/autre |
| `niveau_protection` | INTEGER | **Calcul heuristique** | 1=Faible (moto), 2=Moyen (VL), 3=Élevé (PL/bus) |

#### Codes et Regroupements

**Catégories BAAC → Types :**
- **2-roues** : 01 (Bicyclette), 02 (Cyclomoteur), 30-34 (Motos)
- **VL** : 07 (Véhicule léger)
- **VU** : 10 (Véhicule utilitaire)
- **PL** : 13-17 (Poids lourds divers)
- **TC** : 37-38 (Transport en commun : bus, car, tramway, métro)
- **Autre** : 99 (Autre/indéterminé)

#### Méthode de Population

```sql
-- Extraction catégories distinctes depuis Silver
INSERT INTO accidents_gold.dim_vehicule (categorie_code, categorie_libelle, type_vehicule, niveau_protection)
VALUES 
    (01, 'Bicyclette', '2-roues', FALSE, 1),
    (02, 'Cyclomoteur <50cm3', '2-roues', TRUE, 1),
    (07, 'VL seul', 'VL', TRUE, 2),
    (10, 'VU seul 1,5T <= PTAC <= 3,5T avec ou sans remorque', 'VU', TRUE, 2),
    (13, 'PL seul 3,5T <PTCA <= 7,5T', 'PL', TRUE, 3),
    (30, 'Scooter < 50 cm3', '2-roues', TRUE, 1),
    (37, 'Autobus', 'TC', TRUE, 3),
    (99, 'Autre véhicule', 'Autre', FALSE, 1);
    -- ... (liste complète des ~20 catégories)
```

**Pertinence pour le projet :**
- ✅ **Analyse vulnérabilité** : 2-roues plus de tués/blessés graves ?
- ✅ **Comparaison types** : PL vs VL vs 2-roues
- ✅ **Scoring protection** : priorité campagnes sécurité 2-roues
- ✅ **Requêtes métier** : "Accidents mortels par type de véhicule"

---

### 📊 Fait 1 : `accidents_gold.fait_accidents`

**Grain :** 1 ligne = 1 accident corporel  
**Rôle :** Métriques agrégées au niveau accident (victimes, véhicules, gravité globale).

#### Colonnes et Métriques Principales

| Catégorie | Colonnes | Description | Pertinence Projet |
|-----------|----------|-------------|-------------------|
| **Clés** | `accident_id`, `num_acc` | Identifiants unique (surrogate + naturel) | Traçabilité |
| **Dimensions (FK)** | `date_id`, `geo_id`, `condition_id`, `route_id` | Liens vers dimensions | Analyses multi-dimensionnelles |
| **Caractéristiques** | `heure`, `est_weekend`, `est_nuit`, `en_agglomeration` | Contexte accident | Filtres analytiques |
| **Métriques Comptage** | `nb_vehicules`, `nb_usagers_total` | Volumes impliqués | Complexité accident |
| **Métriques Gravité** | `nb_tues_total`, `nb_blesses_hosp_total`, `nb_blesses_legers_total`, `nb_victimes_total` | Comptage par niveau gravité | ⭐⭐⭐⭐⭐ KPIs mortalité/morbidité |
| **Score Composite** | `score_gravite_total` | `(tués × 100) + (hosp × 10) + (légers × 1)` | ⭐⭐⭐⭐⭐ **Réponse "zones graves vs fréquentées"** |
| **Flags Analytiques** | `est_accident_mortel`, `est_accident_grave` | Booléens pour filtrage rapide | Performance requêtes |
| **Géolocalisation** | `latitude`, `longitude` | Coordonnées GPS | Cartographie hotspots |

#### Calcul du Score de Gravité (Métrique Clé)

```sql
score_gravite_total = (nb_tues_total * 100) + (nb_blesses_hosp_total * 10) + (nb_blesses_legers_total * 1)
```

**Pourquoi cette pondération ?**
- **Tué = 100 points** : impact humain/social maximal
- **Hospitalisé = 10 points** : traumatisme grave mais survie
- **Blessé léger = 1 point** : impact limité

**Utilisation pour répondre aux questions projet :**

```sql
-- Question : "Zones fréquentées = plus graves ou juste plus d'accidents ?"
SELECT 
    g.departement_name,
    COUNT(*) as nb_accidents,
    SUM(fa.score_gravite_total) as gravite_cumul,
    SUM(fa.score_gravite_total) / COUNT(*)::FLOAT as gravite_moyenne_par_accident
FROM accidents_gold.fait_accidents fa
JOIN accidents_gold.dim_geographie g ON fa.geo_id = g.geo_id
GROUP BY g.departement_name
ORDER BY gravite_moyenne_par_accident DESC;
-- → Permet de différencier zones avec beaucoup d'accidents BÉNINS vs accidents GRAVES
```

#### Méthode de Calcul (ETL Silver → Gold)

```sql
INSERT INTO accidents_gold.fait_accidents (
    num_acc, date_id, geo_id, condition_id, route_id,
    nb_vehicules, nb_tues_total, score_gravite_total, ...
)
SELECT 
    a.num_acc,
    TO_CHAR(a.date_accident, 'YYYYMMDD')::INTEGER,
    g.geo_id,
    c.condition_id,
    r.route_id,
    
    -- Agrégations depuis Silver
    (SELECT COUNT(DISTINCT num_veh) FROM accidents_silver.vehicules v WHERE v.num_acc = a.num_acc),
    (SELECT COUNT(*) FROM accidents_silver.usagers u WHERE u.num_acc = a.num_acc AND u.gravite = 2),
    
    -- Calcul score
    (nb_tues * 100) + (nb_hosp * 10) + (nb_legers * 1)
    
FROM accidents_silver.accidents a
LEFT JOIN accidents_gold.dim_date dd ON TO_CHAR(a.date_accident, 'YYYYMMDD')::INT = dd.date_id
LEFT JOIN accidents_gold.dim_geographie g ON a.com_code = g.com_code
-- ... autres jointures dimensions
```

---

### 📊 Fait 2 : `accidents_gold.fait_vehicules`

**Grain :** 1 ligne = 1 véhicule impliqué  
**Rôle :** Métriques par véhicule (victimes par véhicule, obstacles).

#### Métriques Principales

| Métrique | Type | Calcul | Pertinence | Utilisation |
|----------|------|--------|------------|-------------|
| `nb_occupants` | INTEGER | COUNT usagers par (num_acc, num_veh) | ⭐⭐⭐ | Taux d'occupation véhicules |
| `nb_tues_vehicule` | INTEGER | COUNT WHERE gravite=2 | ⭐⭐⭐⭐ | Mortalité par type véhicule |
| `nb_blesses_vehicule` | INTEGER | COUNT WHERE gravite IN (3,4) | ⭐⭐⭐ | Morbidité par véhicule |
| `est_vehicule_implique_mortel` | BOOLEAN | nb_tues_vehicule > 0 | ⭐⭐⭐⭐ | Filtrage accidents mortels par type |
| `a_heurte_obstacle_fixe` | BOOLEAN | obstacle_fixe > 0 | ⭐⭐⭐ | Accidents contre arbres/poteaux |
| `a_heurte_pieton` | BOOLEAN | obstacle_mobile = 1 | ⭐⭐⭐⭐⭐ | Protection usagers vulnérables |

**Exemple requête métier :**

```sql
-- "Quel type de véhicule a le plus fort taux de mortalité ?"
SELECT 
    dv.type_vehicule,
    COUNT(*) as nb_vehicules_total,
    SUM(fv.nb_tues_vehicule) as total_tues,
    SUM(fv.nb_tues_vehicule)::FLOAT / COUNT(*) as taux_mortalite
FROM accidents_gold.fait_vehicules fv
JOIN accidents_gold.dim_vehicule dv ON fv.vehicule_type_id = dv.vehicule_id
GROUP BY dv.type_vehicule
ORDER BY taux_mortalite DESC;
```

---

### 📊 Fait 3 : `accidents_gold.fait_usagers`

**Grain :** 1 ligne = 1 usager (personne impliquée)  
**Rôle :** Analyse individuelle par profil démographique.

#### Métriques et Flags Analytiques

| Métrique | Type | Calcul | Pertinence | Usage Métier |
|----------|------|--------|------------|--------------|
| `gravite` | INTEGER (1-4) | Silver direct | ⭐⭐⭐⭐⭐ | Classification victimes |
| `score_gravite_usager` | INTEGER | Tué=100, Hosp=10, Léger=1, Indemne=0 | ⭐⭐⭐⭐ | Comparaison gravité moyenne |
| `age` | INTEGER | Calculé Silver | ⭐⭐⭐⭐⭐ | Profils à risque |
| `tranche_age` | VARCHAR | '0-14', '15-24', '25-64', '65+' | ⭐⭐⭐⭐⭐ | Segmentation campagnes |
| `est_pieton` | BOOLEAN | categorie_usager IN (3,4) | ⭐⭐⭐⭐⭐ | Usagers vulnérables |
| `est_conducteur` | BOOLEAN | categorie_usager = 1 | ⭐⭐⭐⭐ | Responsabilité |
| `est_tue` | BOOLEAN | gravite = 2 | ⭐⭐⭐⭐⭐ | Mortalité |
| `equipement_utilise` | BOOLEAN | Parsing equipement_securite | ⭐⭐⭐⭐ | Impact ceinture/casque |

**Pourquoi `tranche_age` au lieu de `age` brut ?**
- ⚡ **Performance** : GROUP BY plus rapide
- 📊 **Lisibilité** : catégories métier standards
- 🎯 **Campagnes ciblées** : jeunes 15-24 ans, seniors 65+

**Exemple requête métier :**

```sql
-- "Les jeunes 15-24 ans ont-ils plus d'accidents mortels ?"
SELECT 
    fu.tranche_age,
    COUNT(*) as nb_usagers,
    COUNT(*) FILTER (WHERE fu.est_tue) as nb_tues,
    (COUNT(*) FILTER (WHERE fu.est_tue)::FLOAT / COUNT(*) * 100) as taux_mortalite_pct
FROM accidents_gold.fait_usagers fu
WHERE fu.est_conducteur = TRUE
GROUP BY fu.tranche_age
ORDER BY taux_mortalite_pct DESC;
```

---

## 4️⃣ Tableau de Correspondance : Questions Métier → Métriques Gold

| Question Projet | Tables/Métriques Utilisées | Exemple Requête |
|-----------------|----------------------------|-----------------|
| **"Conditions (météo + lum + route) à risque significativement supérieur ?"** | `dim_conditions`, `dim_route`, `fait_accidents.score_gravite_total` | `SELECT condition_id, route_id, AVG(score_gravite) FROM fait_accidents GROUP BY 1,2 HAVING COUNT(*) > 100 ORDER BY 3 DESC` |
| **"Zones fréquentées : plus graves ou juste plus d'accidents ?"** | `dim_geographie`, `fait_accidents.score_gravite_total / COUNT(*)` | `SELECT geo_id, COUNT(*), SUM(score_gravite)/COUNT(*) as gravite_moy FROM fait_accidents GROUP BY 1 ORDER BY 3 DESC` |
| **"Détection semaines anormales (écarts moyenne) ?"** | `dim_date.semaine_annee`, `fait_accidents.nb_tues_total` | `WITH stats AS (...) SELECT semaine WHERE nb_tues > moy + 2*ecart_type` |
| **"Impact équipement sécurité sur gravité ?"** | `fait_usagers.equipement_utilise`, `score_gravite_usager` | `SELECT equipement_utilise, AVG(score_gravite_usager) FROM fait_usagers WHERE est_conducteur GROUP BY 1` |
| **"Profils démographiques à risque ?"** | `fait_usagers.tranche_age`, `sexe`, `est_tue` | `SELECT tranche_age, sexe, COUNT(*) FILTER (WHERE est_tue) / COUNT(*)::FLOAT FROM fait_usagers GROUP BY 1,2` |
| **"Types véhicules plus mortels ?"** | `dim_vehicule.type_vehicule`, `fait_vehicules.est_vehicule_implique_mortel` | `SELECT type_vehicule, COUNT(*) FILTER (WHERE est_mortel) / COUNT(*)::FLOAT FROM fait_vehicules JOIN dim_vehicule GROUP BY 1` |

---

## 5️⃣ Résumé des Améliorations de Qualité

### Bronze → Silver
- ✅ **Typage fort** : TEXT → types appropriés (DATE, INTEGER, BOOLEAN, DECIMAL)
- ✅ **Normalisation 3NF** : 4 tables liées avec intégrité référentielle
- ✅ **Validation** : contraintes CHECK sur plages de valeurs
- ✅ **Précision géographique** : coordonnées GPS validées

### Silver → Gold
- ✅ **Métriques pré-agrégées** : requêtes 10-20x plus rapides
- ✅ **KPIs métier** : score_gravite, nb_victimes directement disponibles
- ✅ **Dimensions de référence** : libellés explicites (pas de codes bruts)
- ✅ **Analyses multi-dimensionnelles** : constellation permet flexibilité

---

## 6️⃣ Recommandations d'Utilisation par Couche

| Couche | Utiliser pour | Ne PAS utiliser pour |
|--------|---------------|---------------------|
| **Bronze** | Audit, traçabilité, re-processing complet, debug | Requêtes analytiques (non typé) |
| **Silver** | Analyses exploratoires, data science, ML (grain fin) | Dashboards production (lent si gros volumes) |
| **Gold** | **Dashboards, reporting, KPIs, requêtes récurrentes** | Analyses ad-hoc grain très fin |

---

## 7️⃣ Métriques de Suivi Qualité

| Métrique Qualité | Calcul | Seuil Alerte |
|------------------|--------|--------------|
| **Taux géolocalisation** | `COUNT(*) FILTER (WHERE latitude IS NOT NULL) / COUNT(*)` | < 80% |
| **Cohérence score gravité** | `(nb_tues*100 + nb_hosp*10 + nb_legers) = score_gravite_total` | 100% |
| **Unicité accidents** | `COUNT(DISTINCT num_acc) = COUNT(*)` dans fait_accidents | 100% |
| **Orphelins géographiques** | `COUNT(*) FILTER (WHERE geo_id IS NULL)` | < 5% |

---

**📌 Conclusion :** Cette architecture médaillon Bronze → Silver → Gold permet de répondre efficacement aux questions métier du projet d'observatoire de sécurité routière, avec des métriques pré-calculées optimisées pour dashboards et analyses, tout en conservant la traçabilité complète des données sources.