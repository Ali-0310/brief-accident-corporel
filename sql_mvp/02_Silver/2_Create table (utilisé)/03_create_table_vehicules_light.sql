-- ========================================
-- COUCHE SILVER : Table véhicules
-- Grain: 1 ligne = 1 véhicule impliqué
-- Description: Véhicules impliqués dans les accidents
-- ========================================

DROP TABLE IF EXISTS vehicules CASCADE;

CREATE TABLE vehicules (
    -- ================================
    -- IDENTIFIANT (Clé composite)
    -- ================================
    num_acc VARCHAR(20) REFERENCES accidents(num_acc) ON DELETE CASCADE,
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
