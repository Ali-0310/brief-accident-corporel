-- ========================================
-- COUCHE SILVER : Table lieux
-- Grain: 1 ligne = 1 accident (relation 1:1)
-- Description: Caractéristiques détaillées de la voie
-- ========================================

DROP TABLE IF EXISTS lieux CASCADE;

CREATE TABLE lieux (
    -- ================================
    -- IDENTIFIANT (FK)
    -- ================================
    num_acc VARCHAR(20) PRIMARY KEY REFERENCES accidents(num_acc) ON DELETE CASCADE,
    
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