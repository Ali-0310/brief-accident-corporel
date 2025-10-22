-- ========================================
-- TABLE DE FAITS : Accidents
-- Grain: 1 ligne = 1 accident corporel
-- Description: Métriques agrégées au niveau accident
-- ========================================

DROP TABLE IF EXISTS accidents_gold.fait_accidents CASCADE;

CREATE TABLE accidents_gold.fait_accidents (
    -- ================================
    -- IDENTIFIANTS
    -- ================================
    accident_id BIGSERIAL,
    num_acc VARCHAR(20) NOT NULL,
    
    -- ================================
    -- CLÉS ÉTRANGÈRES DIMENSIONS
    -- ================================
    date_id INTEGER NOT NULL REFERENCES accidents_gold.dim_date(date_id),
    geo_id INTEGER REFERENCES accidents_gold.dim_geographie(geo_id),
    condition_id INTEGER REFERENCES accidents_gold.dim_conditions(condition_id),
    route_id INTEGER REFERENCES accidents_gold.dim_route(route_id),
    
    -- ================================
    -- CARACTÉRISTIQUES DÉNORMALISÉES
    -- (éviter JOINs répétitifs)
    -- ================================
    heure INTEGER CHECK (heure BETWEEN 0 AND 23),
    est_weekend BOOLEAN NOT NULL,
    est_nuit BOOLEAN NOT NULL,
    en_agglomeration BOOLEAN NOT NULL,
    est_intersection BOOLEAN NOT NULL,
    type_collision INTEGER,
    
    -- ================================
    -- MÉTRIQUES AGRÉGÉES
    -- (pré-calculées pour performance)
    -- ================================
    nb_vehicules INTEGER DEFAULT 0 CHECK (nb_vehicules >= 0),
    nb_usagers_total INTEGER DEFAULT 0 CHECK (nb_usagers_total >= 0),
    nb_tues_total INTEGER DEFAULT 0 CHECK (nb_tues_total >= 0),
    nb_blesses_hosp_total INTEGER DEFAULT 0 CHECK (nb_blesses_hosp_total >= 0),
    nb_blesses_legers_total INTEGER DEFAULT 0 CHECK (nb_blesses_legers_total >= 0),
    nb_indemnes_total INTEGER DEFAULT 0 CHECK (nb_indemnes_total >= 0),
    nb_victimes_total INTEGER DEFAULT 0 CHECK (nb_victimes_total >= 0),  -- tués + blessés
    
    -- ================================
    -- SCORE DE GRAVITÉ COMPOSITE
    -- ================================
    score_gravite_total INTEGER DEFAULT 0 CHECK (score_gravite_total >= 0),
    -- Pondération: tué=100, hospitalisé=10, blessé léger=1
    
    -- ================================
    -- FLAGS ANALYTIQUES (index bitmap)
    -- ================================
    est_accident_mortel BOOLEAN NOT NULL DEFAULT FALSE,
    est_accident_grave BOOLEAN NOT NULL DEFAULT FALSE,  -- >=1 tué ou hospitalisé
    
    -- ================================
    -- GÉOLOCALISATION
    -- ================================
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- ================================
    -- MÉTADONNÉES
    -- ================================
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ================================
    -- CLÉ PRIMAIRE COMPOSITE
    -- (pour partitionnement)
    -- ================================
    PRIMARY KEY (accident_id, date_id),
    
    -- UNIQUE doit inclure date_id pour compatibilité partitionnement
    UNIQUE (num_acc, date_id) 
) PARTITION BY RANGE (date_id);

COMMENT ON TABLE accidents_gold.fait_accidents IS 
'Table de faits accidents : métriques agrégées au niveau accident. Grain: 1 accident. Partitionnée par date pour performance.';

COMMENT ON COLUMN accidents_gold.fait_accidents.score_gravite_total IS 
'Score pondéré : tué=100 + hospitalisé=10 + blessé léger=1';

COMMENT ON COLUMN accidents_gold.fait_accidents.est_accident_grave IS 
'TRUE si au moins 1 tué OU 1 hospitalisé';

-- ================================
-- CRÉATION DES PARTITIONS PAR ANNÉE
-- (Améliore drastiquement les performances)
-- ================================

CREATE TABLE accidents_gold.fait_accidents_2005 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20050101) TO (20060101);

CREATE TABLE accidents_gold.fait_accidents_2006 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20060101) TO (20070101);

CREATE TABLE accidents_gold.fait_accidents_2007 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20070101) TO (20080101);

CREATE TABLE accidents_gold.fait_accidents_2008 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20080101) TO (20090101);

CREATE TABLE accidents_gold.fait_accidents_2009 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20090101) TO (20100101);

CREATE TABLE accidents_gold.fait_accidents_2010 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20100101) TO (20110101);

CREATE TABLE accidents_gold.fait_accidents_2011 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20110101) TO (20120101);

CREATE TABLE accidents_gold.fait_accidents_2012 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20120101) TO (20130101);

CREATE TABLE accidents_gold.fait_accidents_2013 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20130101) TO (20140101);

CREATE TABLE accidents_gold.fait_accidents_2014 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20140101) TO (20150101);

CREATE TABLE accidents_gold.fait_accidents_2015 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20150101) TO (20160101);

CREATE TABLE accidents_gold.fait_accidents_2016 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20160101) TO (20170101);

CREATE TABLE accidents_gold.fait_accidents_2017 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20170101) TO (20180101);

CREATE TABLE accidents_gold.fait_accidents_2018 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20180101) TO (20190101);

CREATE TABLE accidents_gold.fait_accidents_2019 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20190101) TO (20200101);

CREATE TABLE accidents_gold.fait_accidents_2020 
    PARTITION OF accidents_gold.fait_accidents 
    FOR VALUES FROM (20200101) TO (20210101);

-- Partition par défaut pour dates futures
CREATE TABLE accidents_gold.fait_accidents_default 
    PARTITION OF accidents_gold.fait_accidents 
    DEFAULT;

-- Validation
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as taille
FROM pg_tables 
WHERE schemaname = 'accidents_gold' 
  AND tablename LIKE 'fait_accidents%'
ORDER BY tablename;