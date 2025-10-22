-- ========================================
-- DIMENSION : Géographie
-- Grain: 1 ligne = 1 commune
-- Description: Hiérarchie géographique complète
-- ========================================

DROP TABLE IF EXISTS accidents_gold.dim_geographie CASCADE;

CREATE TABLE accidents_gold.dim_geographie (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    geo_id SERIAL PRIMARY KEY,
    com_code VARCHAR(5) UNIQUE NOT NULL,
    
    -- ================================
    -- COMMUNE
    -- ================================
    com_name VARCHAR(100),
    com_arm_name VARCHAR(100),  -- Arrondissement municipal
    
    -- ================================
    -- DÉPARTEMENT
    -- ================================
    departement_code VARCHAR(3) NOT NULL,
    departement_name VARCHAR(100),
    
    -- ================================
    -- RÉGION
    -- ================================
    region_code VARCHAR(2),
    region_name VARCHAR(100),
    
    -- ================================
    -- EPCI (Intercommunalité)
    -- ================================
    epci_code VARCHAR(10),
    epci_name VARCHAR(100),
    
    -- ================================
    -- ENRICHISSEMENTS (optionnels)
    -- ================================
    type_zone VARCHAR(20),  -- Urbain, Rural, Périurbain
    population INTEGER CHECK (population >= 0),
    superficie_km2 DECIMAL(10, 2) CHECK (superficie_km2 >= 0),
    densite_population DECIMAL(10, 2) CHECK (densite_population >= 0)
);

COMMENT ON TABLE accidents_gold.dim_geographie IS 
'Dimension géographie : hiérarchie commune > département > région. Grain: 1 commune';

COMMENT ON COLUMN accidents_gold.dim_geographie.type_zone IS 
'Classification: Urbain (>10000 hab), Périurbain (2000-10000), Rural (<2000)';

-- Index
CREATE INDEX idx_dim_geo_dept ON accidents_gold.dim_geographie(departement_code);
CREATE INDEX idx_dim_geo_region ON accidents_gold.dim_geographie(region_code);
CREATE INDEX idx_dim_geo_type ON accidents_gold.dim_geographie(type_zone) WHERE type_zone IS NOT NULL;