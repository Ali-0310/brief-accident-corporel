-- ========================================
-- COUCHE SILVER : Table véhicules
-- Grain: 1 ligne = 1 véhicule impliqué
-- Description: Véhicules impliqués dans les accidents
-- ========================================

DROP TABLE IF EXISTS accidents_silver.vehicules CASCADE;

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