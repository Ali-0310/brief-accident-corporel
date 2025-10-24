-- ========================================
-- COUCHE SILVER : Table accidents
-- Grain: 1 ligne = 1 accident corporel
-- Description: Fait central avec caractéristiques validées
-- ========================================

DROP TABLE IF EXISTS accidents CASCADE;

CREATE TABLE accidents (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    num_acc VARCHAR(20) PRIMARY KEY,
    
    -- ================================
    -- TEMPOREL
    -- ================================
--    date_accident DATE NOT NULL, ---------------------------   pas besoin redondant
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
    type_collision INTEGER CHECK (type_collision BETWEEN 1 AND 7)
)