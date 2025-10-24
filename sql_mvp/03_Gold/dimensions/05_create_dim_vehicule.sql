-- ========================================
-- DIMENSION : Véhicule
-- Grain: 1 ligne = 1 type de véhicule
-- Description: Typologie des véhicules (référentiel BAAC)
-- ========================================

DROP TABLE IF EXISTS accidents_gold.dim_vehicule CASCADE;

CREATE TABLE accidents_gold.dim_vehicule (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    vehicule_id SERIAL PRIMARY KEY,
    categorie_code INTEGER UNIQUE CHECK (categorie_code BETWEEN 0 AND 99),
    
    -- ================================
    -- LIBELLÉ
    -- ================================
    categorie_libelle VARCHAR(100) NOT NULL,
    
    -- ================================
    -- CLASSIFICATION
    -- ================================
    type_vehicule VARCHAR(50),  -- 2-roues, VL, PL, TC, Piéton, Autre
    est_motorise BOOLEAN NOT NULL,
    
    -- ================================
    -- NIVEAU DE PROTECTION
    -- ================================
    niveau_protection INTEGER CHECK (niveau_protection BETWEEN 1 AND 3)
    -- 1=Faible (2-roues), 2=Moyen (VL), 3=Élevé (PL/TC)
);

COMMENT ON TABLE accidents_gold.dim_vehicule IS 
'Dimension véhicule : typologie et niveau de protection. Grain: 1 catégorie BAAC';

COMMENT ON COLUMN accidents_gold.dim_vehicule.type_vehicule IS 
'2-roues, VL (Véhicule Léger), PL (Poids Lourd), TC (Transport Commun), Piéton, Autre';

COMMENT ON COLUMN accidents_gold.dim_vehicule.niveau_protection IS 
'1=Faible (vélo, moto), 2=Moyen (voiture), 3=Élevé (PL, bus)';

-- Index
CREATE INDEX idx_dim_vehicule_type ON accidents_gold.dim_vehicule(type_vehicule);
CREATE INDEX idx_dim_vehicule_protection ON accidents_gold.dim_vehicule(niveau_protection);

-- ================================
-- PEUPLEMENT INITIAL (référentiel BAAC)
-- ================================
INSERT INTO accidents_gold.dim_vehicule (categorie_code, categorie_libelle, type_vehicule, est_motorise, niveau_protection) VALUES
(1, 'Bicyclette', '2-roues', FALSE, 1),
(2, 'Cyclomoteur <50cm3', '2-roues', TRUE, 1),
(7, 'VL seul', 'VL', TRUE, 2),
(10, 'VU seul 1,5T <= PTAC <= 3,5T', 'VL', TRUE, 2),
(13, 'PL seul 3,5T <PTCA <= 7,5T', 'PL', TRUE, 3),
(14, 'PL seul > 7,5T', 'PL', TRUE, 3),
(15, 'PL > 3,5T + remorque', 'PL', TRUE, 3),
(16, 'Tracteur routier seul', 'PL', TRUE, 3),
(17, 'Tracteur routier + semi-remorque', 'PL', TRUE, 3),
(30, 'Scooter < 50 cm3', '2-roues', TRUE, 1),
(31, 'Motocyclette > 50 cm3 et <= 125 cm3', '2-roues', TRUE, 1),
(32, 'Scooter > 50 cm3 et <= 125 cm3', '2-roues', TRUE, 1),
(33, 'Motocyclette > 125 cm3', '2-roues', TRUE, 1),
(34, 'Scooter > 125 cm3', '2-roues', TRUE, 1),
(37, 'Autobus', 'TC', TRUE, 3),
(38, 'Autocar', 'TC', TRUE, 3),
(99, 'Autre véhicule', 'Autre', TRUE, 2);

-- Validation
SELECT 
    categorie_code,
    categorie_libelle,
    type_vehicule,
    niveau_protection
FROM accidents_gold.dim_vehicule
ORDER BY categorie_code;