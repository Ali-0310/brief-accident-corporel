-- ========================================
-- COUCHE SILVER : Table accidents
-- Grain: 1 ligne = 1 accident corporel
-- Description: Fait central avec caractéristiques validées
-- ========================================

DROP TABLE IF EXISTS accidents_silver.accidents CASCADE;

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