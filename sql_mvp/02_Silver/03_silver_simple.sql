-- ========================================
-- SILVER : Tables normalisées (3NF)
-- ========================================

-- =======================================================================================
-- Table accidents
-- Grain: 1 ligne = 1 accident corporel
-- Description: Fait central avec caractéristiques validées
-- =======================================================================================
CREATE TABLE accidents_silver.accidents (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    num_acc VARCHAR(20) PRIMARY KEY,
    
    -- ================================
    -- TEMPOREL
    -- ================================
    date_accident DATE NOT NULL,
    heure INTEGER CHECK (heure BETWEEN 0 AND 23),
    minute INTEGER CHECK (minute BETWEEN 0 AND 59),
    annee INTEGER NOT NULL CHECK (annee BETWEEN 2005 AND 2030),
    mois INTEGER CHECK (mois BETWEEN 1 AND 12),
    jour INTEGER CHECK (jour BETWEEN 1 AND 31),
    jour_semaine INTEGER CHECK (jour_semaine BETWEEN 1 AND 7),  -- 1=Lundi, 7=Dimanche
    
    -- ================================
    -- GÉOGRAPHIE
    -- ================================
    com_code VARCHAR(5),
    departement_code VARCHAR(3),
    en_agglomeration BOOLEAN,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    adresse TEXT,
    
    -- ================================
    -- CONDITIONS
    -- ================================
    luminosite INTEGER CHECK (luminosite BETWEEN 1 AND 5),
    conditions_atmospheriques INTEGER CHECK (conditions_atmospheriques BETWEEN 1 AND 9),
    type_intersection INTEGER CHECK (type_intersection BETWEEN 1 AND 9),
    type_collision INTEGER CHECK (type_collision BETWEEN 1 AND 7),
    
    -- ================================
    -- MÉTADONNÉES
    -- ================================
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_row_id BIGINT,  -- Traçabilité vers Bronze
    
    -- ================================
    -- CONTRAINTES
    -- ================================
    CONSTRAINT ck_coords_valides CHECK (
        (latitude IS NULL AND longitude IS NULL) OR
        (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
    )
);

COMMENT ON TABLE accidents_silver.accidents IS 
'Table centrale des accidents : données nettoyées et validées. Grain: 1 accident';

COMMENT ON COLUMN accidents_silver.accidents.jour_semaine IS 
'1=Lundi, 2=Mardi, 3=Mercredi, 4=Jeudi, 5=Vendredi, 6=Samedi, 7=Dimanche';

COMMENT ON COLUMN accidents_silver.accidents.luminosite IS 
'1=Plein jour, 2=Crépuscule/aube, 3=Nuit sans éclairage, 4=Nuit éclairage non allumé, 5=Nuit éclairage allumé';

COMMENT ON COLUMN accidents_silver.accidents.source_row_id IS 
'FK vers accidents_bronze.raw_accidents.row_id pour traçabilité';


-- =======================================================================================
-- Table lieux
-- Grain: 1 ligne = 1 accident (relation 1:1)
-- Description: Caractéristiques détaillées de la voie
-- =======================================================================================
CREATE TABLE accidents_silver.lieux (
    -- ================================
    -- IDENTIFIANT (FK)
    -- ================================
    num_acc VARCHAR(20) PRIMARY KEY REFERENCES accidents_silver.accidents(num_acc) ON DELETE CASCADE,
    
    -- ================================
    -- VOIE
    -- ================================
    categorie_route INTEGER CHECK (categorie_route BETWEEN 1 AND 9),
    numero_route VARCHAR(10),
    regime_circulation INTEGER CHECK (regime_circulation BETWEEN 1 AND 4),
    nombre_voies INTEGER CHECK (nombre_voies > 0 AND nombre_voies <= 20),
    voie_reservee INTEGER CHECK (voie_reservee BETWEEN 0 AND 3),
    
    -- ================================
    -- PROFIL
    -- ================================
    profil_route INTEGER CHECK (profil_route BETWEEN 1 AND 4),
    trace_plan INTEGER CHECK (trace_plan BETWEEN 1 AND 4),
    
    -- ================================
    -- DIMENSIONS
    -- ================================
    largeur_terre_plein DECIMAL(5, 2) CHECK (largeur_terre_plein >= 0 AND largeur_terre_plein < 100),
    largeur_chaussee DECIMAL(5, 2) CHECK (largeur_chaussee >= 0 AND largeur_chaussee < 100),
    
    -- ================================
    -- ÉTAT ET INFRASTRUCTURE
    -- ================================
    etat_surface INTEGER CHECK (etat_surface BETWEEN 1 AND 9),
    infrastructure INTEGER CHECK (infrastructure BETWEEN 0 AND 7),
    situation INTEGER CHECK (situation BETWEEN 0 AND 5),
    proximite_ecole BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE accidents_silver.lieux IS 
'Caractéristiques de la voie où s''est produit l''accident. Grain: 1 accident (1:1 avec accidents)';

COMMENT ON COLUMN accidents_silver.lieux.categorie_route IS 
'1=Autoroute, 2=Route Nationale, 3=Départementale, 4=Communale, 5=Hors réseau public, 6=Parking, 9=Autre';

COMMENT ON COLUMN accidents_silver.lieux.etat_surface IS 
'1=Normal, 2=Mouillé, 3=Flaques, 4=Inondé, 5=Enneigé, 6=Boue, 7=Verglacé, 8=Corps gras, 9=Autre';

COMMENT ON COLUMN accidents_silver.lieux.proximite_ecole IS 
'TRUE si accident à proximité d''une école (< 500m)';

-- =======================================================================================
-- Table véhicules
-- Grain: 1 ligne = 1 véhicule impliqué dans un accident
-- Description: Caractéristiques des véhicules impliqués
-- =======================================================================================
CREATE TABLE accidents_silver.vehicules (
    -- ================================
    -- IDENTIFIANT (Clé composite)
    -- ================================
    num_acc VARCHAR(20) REFERENCES accidents_silver.accidents(num_acc) ON DELETE CASCADE,
    num_veh VARCHAR(10),
    
    -- ================================
    -- CARACTÉRISTIQUES VÉHICULE
    -- ================================
    sens_circulation INTEGER CHECK (sens_circulation IN (1, 2)),
    categorie_vehicule INTEGER CHECK (categorie_vehicule BETWEEN 0 AND 99),
    
    -- ================================
    -- OBSTACLES
    -- ================================
    obstacle_fixe INTEGER CHECK (obstacle_fixe BETWEEN 0 AND 16),
    obstacle_mobile INTEGER CHECK (obstacle_mobile BETWEEN 0 AND 9),
    
    -- ================================
    -- CHOC ET MANOEUVRE
    -- ================================
    point_choc INTEGER CHECK (point_choc BETWEEN 0 AND 9),
    manoeuvre INTEGER CHECK (manoeuvre BETWEEN 0 AND 24),
    
    -- ================================
    -- OCCUPATION
    -- ================================
    nb_occupants_tc INTEGER CHECK (nb_occupants_tc >= 0),  -- Transport en commun uniquement
    
    -- ================================
    -- CONTRAINTES
    -- ================================
    PRIMARY KEY (num_acc, num_veh)
);

COMMENT ON TABLE accidents_silver.vehicules IS 
'Véhicules impliqués dans les accidents. Grain: 1 véhicule. Relation N:1 avec accidents';

COMMENT ON COLUMN accidents_silver.vehicules.sens_circulation IS 
'1=Sens croissant (PR/PK), 2=Sens décroissant';

COMMENT ON COLUMN accidents_silver.vehicules.categorie_vehicule IS 
'01=Bicyclette, 02=Cyclomoteur, 07=VL, 10=VU, 13-17=PL, 30-34=Moto, 37-38=TC, 99=Autre';

COMMENT ON COLUMN accidents_silver.vehicules.obstacle_fixe IS 
'1=Véhicule stationné, 2=Arbre, 3-5=Glissière, 6=Bâtiment, 16=Sortie chaussée, etc.';

COMMENT ON COLUMN accidents_silver.vehicules.manoeuvre IS 
'1=Sans changement, 2-24=Diverses manœuvres (demi-tour, dépassement, stationnement, etc.)';

-- =======================================================================================
-- Table usagers
-- Grain: 1 ligne = 1 usager (victime ou indemne)
-- Description: Personnes impliquées dans les accidents
-- =======================================================================================

CREATE TABLE accidents_silver.usagers (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    id_usager BIGSERIAL PRIMARY KEY,
    
    -- ================================
    -- CLES ETRANGERES
    -- ================================
    num_acc VARCHAR(20),
    num_veh VARCHAR(10),
    
    -- ================================
    -- POSITION USAGER
    -- ================================
    place_vehicule INTEGER,  -- Position dans le véhicule (conducteur, passager avant/arrière, etc.)
    categorie_usager INTEGER CHECK (categorie_usager BETWEEN 1 AND 4),  -- 1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton roller/trottinette
    
    -- ================================
    -- GRAVITE ET PROFIL
    -- ================================
    gravite INTEGER NOT NULL CHECK (gravite BETWEEN 1 AND 4),  -- 1=Indemne, 2=Tué, 3=Hospitalisé, 4=Blessé léger
    sexe INTEGER CHECK (sexe IN (1, 2)),  -- 1=Masculin, 2=Féminin
    annee_naissance INTEGER CHECK (annee_naissance BETWEEN 1900 AND 2025),
    age_au_moment_accident INTEGER CHECK (age_au_moment_accident BETWEEN 0 AND 120),
    
    -- ================================
    -- CONTEXTE DEPLACEMENT
    -- ================================
    motif_deplacement INTEGER,  -- 1-9 : Domicile-travail, école, courses, loisirs, etc.
    equipement_securite VARCHAR(2),  -- 2 caractères : 1er=type équipement, 2e=utilisation
    
    -- ================================
    -- SPECIFIQUE PIETONS
    -- ================================
    localisation_pieton INTEGER,  -- 1-8 : Sur chaussée, passage piéton, trottoir, etc.
    action_pieton INTEGER,  -- 0-9 : Traversant, masqué, jouant, etc.
    etat_pieton INTEGER,  -- 1-3 : Seul, accompagné, en groupe
    
    -- ================================
    -- CONTRAINTES
    -- ================================
    FOREIGN KEY (num_acc, num_veh) 
        REFERENCES accidents_silver.vehicules(num_acc, num_veh) 
        ON DELETE CASCADE
);

COMMENT ON TABLE accidents_silver.usagers IS 
'Usagers (victimes et indemnes) impliqués dans les accidents (grain: 1 usager)';

COMMENT ON COLUMN accidents_silver.usagers.gravite IS 
'1=Indemne, 2=Tué (sur le coup ou dans les 30 jours), 3=Hospitalisé >24h, 4=Blessé léger';

COMMENT ON COLUMN accidents_silver.usagers.categorie_usager IS 
'1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton en roller/trottinette';

COMMENT ON COLUMN accidents_silver.usagers.equipement_securite IS 
'Format: XY où X=type (1=Ceinture, 2=Casque, 3=Dispositif enfant, 4=Réfléchissant, 9=Autre) et Y=utilisation (1=Oui, 2=Non, 3=Indéterminé)';