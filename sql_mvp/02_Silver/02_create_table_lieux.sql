-- ========================================
-- COUCHE SILVER : Table lieux
-- Grain: 1 ligne = 1 accident (relation 1:1)
-- Description: Caractéristiques détaillées de la voie
-- ========================================

DROP TABLE IF EXISTS accidents_silver.lieux CASCADE;

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