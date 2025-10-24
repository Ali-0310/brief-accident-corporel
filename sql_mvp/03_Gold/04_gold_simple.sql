-- ========================================
-- GOLD : Modèle constellation COMPLET
-- Structures VIDES (SANS données de référence)
-- SANS partitionnement (pour MVP)
-- ========================================

-- ================================
-- DIMENSIONS (tables vides)
-- ================================

-- DIMENSION 1 : Date (calendrier)
CREATE TABLE accidents_gold.dim_date (
    date_id INTEGER PRIMARY KEY,
    date_complete DATE UNIQUE NOT NULL,
    annee INTEGER NOT NULL,
    mois INTEGER NOT NULL,
    jour INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    jour_semaine INTEGER NOT NULL,  -- 1=Lundi, 7=Dimanche
    nom_jour VARCHAR(10) NOT NULL,
    nom_mois VARCHAR(10) NOT NULL,
    semaine_annee INTEGER NOT NULL,
    jour_annee INTEGER NOT NULL,
    est_weekend BOOLEAN NOT NULL,
    est_jour_ferie BOOLEAN DEFAULT FALSE,
    nom_jour_ferie VARCHAR(50),
    saison VARCHAR(10) NOT NULL
);

COMMENT ON TABLE accidents_gold.dim_date IS 'Dimension calendrier : de 2005 à 2025. Grain: 1 jour';
COMMENT ON COLUMN accidents_gold.dim_date.date_id IS 'Clé technique au format YYYYMMDD (ex: 20150315 pour 15 mars 2015)';
COMMENT ON COLUMN accidents_gold.dim_date.jour_semaine IS '1=Lundi, 2=Mardi, 3=Mercredi, 4=Jeudi, 5=Vendredi, 6=Samedi, 7=Dimanche (norme ISO)';

-- DIMENSION 2 : Géographie
CREATE TABLE accidents_gold.dim_geographie (
    geo_id SERIAL PRIMARY KEY,
    com_code VARCHAR(5) UNIQUE NOT NULL,
    com_name VARCHAR(100),
    com_arm_name VARCHAR(100),
    departement_code VARCHAR(3) NOT NULL,
    departement_name VARCHAR(100),
    region_code VARCHAR(2),
    region_name VARCHAR(100),
    epci_code VARCHAR(10),
    epci_name VARCHAR(100),
    type_zone VARCHAR(20),
    population INTEGER,
    superficie_km2 DECIMAL(10, 2),
    densite_population DECIMAL(10, 2)
);

COMMENT ON TABLE accidents_gold.dim_geographie IS 'Dimension géographie : hiérarchie commune > département > région. Grain: 1 commune';
COMMENT ON COLUMN accidents_gold.dim_geographie.type_zone IS 'Classification: Urbain (>10000 hab), Périurbain (2000-10000), Rural (<2000)';

-- DIMENSION 3 : Conditions (météo + luminosité)
CREATE TABLE accidents_gold.dim_conditions (
    condition_id SERIAL PRIMARY KEY,
    luminosite_code INTEGER NOT NULL,
    luminosite_libelle VARCHAR(50) NOT NULL,
    est_nuit BOOLEAN NOT NULL,
    atm_code INTEGER NOT NULL,
    atm_libelle VARCHAR(50) NOT NULL,
    est_intemperie BOOLEAN NOT NULL,
    niveau_risque INTEGER,
    UNIQUE (luminosite_code, atm_code)
);

COMMENT ON TABLE accidents_gold.dim_conditions IS 'Dimension conditions : combinaison luminosité × météo. Grain: 1 combinaison';
COMMENT ON COLUMN accidents_gold.dim_conditions.luminosite_libelle IS 'Plein jour, Crépuscule/aube, Nuit sans éclairage, Nuit éclairage non allumé, Nuit éclairage allumé';
COMMENT ON COLUMN accidents_gold.dim_conditions.atm_libelle IS 'Normal, Pluie légère, Pluie forte, Neige/grêle, Brouillard, Vent fort, Éblouissant, Couvert, Autre';

-- DIMENSION 4 : Route
CREATE TABLE accidents_gold.dim_route (
    route_id SERIAL PRIMARY KEY,
    categorie_route_code INTEGER,
    categorie_route_libelle VARCHAR(50),
    profil_route_code INTEGER,
    profil_route_libelle VARCHAR(50),
    trace_plan_code INTEGER,
    trace_plan_libelle VARCHAR(50),
    etat_surface_code INTEGER,
    etat_surface_libelle VARCHAR(50),
    niveau_risque_route INTEGER,
    UNIQUE (categorie_route_code, profil_route_code, trace_plan_code, etat_surface_code)
);

COMMENT ON TABLE accidents_gold.dim_route IS 'Dimension route : combinaison catégorie × profil × tracé × surface. Grain: 1 combinaison';
COMMENT ON COLUMN accidents_gold.dim_route.categorie_route_libelle IS 'Autoroute, Route Nationale, Départementale, Communale, Hors réseau, Parking, Autre';
COMMENT ON COLUMN accidents_gold.dim_route.profil_route_libelle IS 'Plat, Pente, Sommet de côte, Bas de côte';
COMMENT ON COLUMN accidents_gold.dim_route.trace_plan_libelle IS 'Rectiligne, Courbe à gauche, Courbe à droite, En S';
COMMENT ON COLUMN accidents_gold.dim_route.etat_surface_libelle IS 'Normal, Mouillé, Flaques, Inondé, Enneigé, Boue, Verglacé, Corps gras, Autre';

-- DIMENSION 5 : Véhicule
CREATE TABLE accidents_gold.dim_vehicule (
    vehicule_id SERIAL PRIMARY KEY,
    categorie_code INTEGER UNIQUE,
    categorie_libelle VARCHAR(100) NOT NULL,
    type_vehicule VARCHAR(50),
    est_motorise BOOLEAN NOT NULL,
    niveau_protection INTEGER
);

COMMENT ON TABLE accidents_gold.dim_vehicule IS 'Dimension véhicule : typologie et niveau de protection. Grain: 1 catégorie BAAC';
COMMENT ON COLUMN accidents_gold.dim_vehicule.type_vehicule IS '2-roues, VL (Véhicule Léger), PL (Poids Lourd), TC (Transport Commun), Piéton, Autre';
COMMENT ON COLUMN accidents_gold.dim_vehicule.niveau_protection IS '1=Faible (vélo, moto), 2=Moyen (voiture), 3=Élevé (PL, bus)';

-- ================================
-- TABLES DE FAITS (vides)
-- ================================

-- FAIT 1 : Accidents (SANS partitionnement)
CREATE TABLE accidents_gold.fait_accidents (
    accident_id BIGSERIAL PRIMARY KEY,
    num_acc VARCHAR(20) UNIQUE NOT NULL,
    
    -- Clés étrangères vers dimensions
    date_id INTEGER NOT NULL REFERENCES accidents_gold.dim_date(date_id),
    geo_id INTEGER REFERENCES accidents_gold.dim_geographie(geo_id),
    condition_id INTEGER REFERENCES accidents_gold.dim_conditions(condition_id),
    route_id INTEGER REFERENCES accidents_gold.dim_route(route_id),
    
    -- Caractéristiques dénormalisées
    heure INTEGER CHECK (heure BETWEEN 0 AND 23),
    est_weekend BOOLEAN NOT NULL,
    est_nuit BOOLEAN NOT NULL,
    en_agglomeration BOOLEAN NOT NULL,
    est_intersection BOOLEAN NOT NULL,
    type_collision INTEGER,
    
    -- Métriques agrégées
    nb_vehicules INTEGER DEFAULT 0 CHECK (nb_vehicules >= 0),
    nb_usagers_total INTEGER DEFAULT 0 CHECK (nb_usagers_total >= 0),
    nb_tues_total INTEGER DEFAULT 0 CHECK (nb_tues_total >= 0),
    nb_blesses_hosp_total INTEGER DEFAULT 0 CHECK (nb_blesses_hosp_total >= 0),
    nb_blesses_legers_total INTEGER DEFAULT 0 CHECK (nb_blesses_legers_total >= 0),
    nb_indemnes_total INTEGER DEFAULT 0 CHECK (nb_indemnes_total >= 0),
    nb_victimes_total INTEGER DEFAULT 0 CHECK (nb_victimes_total >= 0),
    
    -- Score de gravité
    score_gravite_total INTEGER DEFAULT 0 CHECK (score_gravite_total >= 0),
    
    -- Flags analytiques
    est_accident_mortel BOOLEAN NOT NULL DEFAULT FALSE,
    est_accident_grave BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Géolocalisation
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Métadonnées
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE accidents_gold.fait_accidents IS 'Table de faits accidents : métriques agrégées au niveau accident. Grain: 1 accident';
COMMENT ON COLUMN accidents_gold.fait_accidents.score_gravite_total IS 'Score pondéré : tué=100 + hospitalisé=10 + blessé léger=1';
COMMENT ON COLUMN accidents_gold.fait_accidents.est_accident_grave IS 'TRUE si au moins 1 tué OU 1 hospitalisé';

-- FAIT 2 : Véhicules
CREATE TABLE accidents_gold.fait_vehicules (
    vehicule_id BIGSERIAL PRIMARY KEY,
    accident_fk BIGINT NOT NULL,  -- Relation logique vers fait_accidents
    num_acc VARCHAR(20) NOT NULL,
    num_veh VARCHAR(10) NOT NULL,
    
    -- Dimension véhicule
    vehicule_type_id INTEGER REFERENCES accidents_gold.dim_vehicule(vehicule_id),
    
    -- Caractéristiques véhicule
    sens_circulation INTEGER,
    obstacle_fixe INTEGER,
    obstacle_mobile INTEGER,
    point_choc INTEGER,
    manoeuvre INTEGER,
    
    -- Métriques agrégées (niveau véhicule)
    nb_occupants INTEGER DEFAULT 0,
    nb_tues_vehicule INTEGER DEFAULT 0,
    nb_blesses_vehicule INTEGER DEFAULT 0,
    nb_indemnes_vehicule INTEGER DEFAULT 0,
    
    -- Flags
    est_vehicule_implique_mortel BOOLEAN DEFAULT FALSE,
    a_heurte_obstacle_fixe BOOLEAN DEFAULT FALSE,
    a_heurte_pieton BOOLEAN DEFAULT FALSE,
    
    -- Métadonnées
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (num_acc, num_veh)
);

COMMENT ON TABLE accidents_gold.fait_vehicules IS 'Table de faits véhicules : véhicules impliqués dans les accidents. Grain: 1 véhicule';
COMMENT ON COLUMN accidents_gold.fait_vehicules.accident_fk IS 'Référence logique vers fait_accidents (sans FK formelle pour éviter contraintes)';

-- FAIT 3 : Usagers
CREATE TABLE accidents_gold.fait_usagers (
    usager_id BIGSERIAL PRIMARY KEY,
    vehicule_fk BIGINT NOT NULL REFERENCES accidents_gold.fait_vehicules(vehicule_id),
    accident_fk BIGINT NOT NULL,  -- Dénormalisé pour performance
    num_acc VARCHAR(20) NOT NULL,
    
    -- Position et catégorie
    place_vehicule INTEGER,
    categorie_usager INTEGER NOT NULL CHECK (categorie_usager BETWEEN 1 AND 4),
    
    -- Gravité (métrique clé)
    gravite INTEGER NOT NULL CHECK (gravite BETWEEN 1 AND 4),
    
    -- Profil démographique
    sexe INTEGER CHECK (sexe IN (1, 2)),
    age INTEGER CHECK (age BETWEEN 0 AND 120),
    tranche_age VARCHAR(10),
    
    -- Contexte
    motif_deplacement INTEGER,
    equipement_securite VARCHAR(2),
    equipement_present BOOLEAN,
    equipement_utilise BOOLEAN,
    
    -- Spécifique piétons
    localisation_pieton INTEGER,
    action_pieton INTEGER,
    etat_pieton INTEGER,
    
    -- Score individuel
    score_gravite_usager INTEGER DEFAULT 0,
    
    -- Flags analytiques (colonnes calculées)
    est_conducteur BOOLEAN,
    est_pieton BOOLEAN,
    est_victime BOOLEAN,
    est_tue BOOLEAN,
    est_blesse_grave BOOLEAN,
    
    -- Métadonnées
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE accidents_gold.fait_usagers IS 'Table de faits usagers : personnes impliquées dans les accidents. Grain: 1 usager';
COMMENT ON COLUMN accidents_gold.fait_usagers.gravite IS '1=Indemne, 2=Tué (sur coup ou <30j), 3=Hospitalisé >24h, 4=Blessé léger';
COMMENT ON COLUMN accidents_gold.fait_usagers.score_gravite_usager IS 'Métrique pondérée : tué=100, hosp=10, blessé=1, indemne=0';

-- ================================
-- VALIDATION
-- ================================

-- Vérifier toutes les tables créées
SELECT 
    schemaname,
    tablename,
    'Table vide' as statut
FROM pg_tables 
WHERE schemaname = 'accidents_gold'
ORDER BY tablename;

-- Compter les lignes (devrait être 0 partout)
SELECT 'dim_date' as table_name, COUNT(*) as nb_lignes FROM accidents_gold.dim_date
UNION ALL
SELECT 'dim_geographie', COUNT(*) FROM accidents_gold.dim_geographie
UNION ALL
SELECT 'dim_conditions', COUNT(*) FROM accidents_gold.dim_conditions
UNION ALL
SELECT 'dim_route', COUNT(*) FROM accidents_gold.dim_route
UNION ALL
SELECT 'dim_vehicule', COUNT(*) FROM accidents_gold.dim_vehicule
UNION ALL
SELECT 'fait_accidents', COUNT(*) FROM accidents_gold.fait_accidents
UNION ALL
SELECT 'fait_vehicules', COUNT(*) FROM accidents_gold.fait_vehicules
UNION ALL
SELECT 'fait_usagers', COUNT(*) FROM accidents_gold.fait_usagers;