-- ========================================
-- TABLE DE FAITS : Véhicules
-- Grain: 1 ligne = 1 véhicule impliqué dans un accident
-- Description: Véhicules avec leurs actions et caractéristiques
-- ========================================

DROP TABLE IF EXISTS accidents_gold.fait_vehicules CASCADE;

CREATE TABLE accidents_gold.fait_vehicules (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    vehicule_id BIGSERIAL PRIMARY KEY,
    
    -- ================================
    -- CLÉS ÉTRANGÈRES
    -- ================================
    accident_fk BIGINT NOT NULL,  -- Référence logique vers fait_accidents
    num_acc VARCHAR(20) NOT NULL,  -- Clé métier pour jointure
    num_veh VARCHAR(10) NOT NULL,
    
    -- ================================
    -- CLÉ DIMENSION VÉHICULE
    -- ================================
    vehicule_type_id INTEGER REFERENCES accidents_gold.dim_vehicule(vehicule_id),
    
    -- ================================
    -- CARACTÉRISTIQUES VÉHICULE
    -- ================================
    sens_circulation INTEGER CHECK (sens_circulation IN (1, 2, NULL)),
    -- 1=Sens croissant, 2=Sens décroissant
    
    -- ================================
    -- OBSTACLES
    -- ================================
    obstacle_fixe INTEGER CHECK (obstacle_fixe BETWEEN 0 AND 16),
    -- 0=Aucun, 1=Véhicule stationné, 2=Arbre, 3-5=Glissières, 6=Bâtiment, etc.
    
    obstacle_mobile INTEGER CHECK (obstacle_mobile BETWEEN 0 AND 9),
    -- 0=Aucun, 1=Piéton, 2=Véhicule, 4=Véhicule sur rail, 5-6=Animaux, 9=Autre
    
    -- ================================
    -- CHOC ET MANOEUVRE
    -- ================================
    point_choc INTEGER CHECK (point_choc BETWEEN 0 AND 9),
    -- 1=Avant, 2=Avant droit, 3=Avant gauche, 4=Arrière, 5-8=Côtés, 9=Chocs multiples
    
    manoeuvre INTEGER CHECK (manoeuvre BETWEEN 0 AND 24),
    -- 1=Sans changement, 2-24=Diverses manœuvres
    
    -- ================================
    -- MÉTRIQUES AGRÉGÉES (niveau véhicule)
    -- ================================
    nb_occupants INTEGER DEFAULT 0 CHECK (nb_occupants >= 0),
    nb_tues_vehicule INTEGER DEFAULT 0 CHECK (nb_tues_vehicule >= 0),
    nb_blesses_vehicule INTEGER DEFAULT 0 CHECK (nb_blesses_vehicule >= 0),
    nb_indemnes_vehicule INTEGER DEFAULT 0 CHECK (nb_indemnes_vehicule >= 0),
    
    -- ================================
    -- FLAGS
    -- ================================
    est_vehicule_implique_mortel BOOLEAN DEFAULT FALSE,
    -- TRUE si au moins 1 tué dans ce véhicule
    
    a_heurte_obstacle_fixe BOOLEAN DEFAULT FALSE,
    a_heurte_pieton BOOLEAN DEFAULT FALSE,
    
    -- ================================
    -- MÉTADONNÉES
    -- ================================
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ================================
    -- CONTRAINTE D'UNICITÉ
    -- ================================
    UNIQUE (num_acc, num_veh)
);

COMMENT ON TABLE accidents_gold.fait_vehicules IS 
'Table de faits véhicules : véhicules impliqués dans les accidents. Grain: 1 véhicule';

COMMENT ON COLUMN accidents_gold.fait_vehicules.accident_fk IS 
'Référence logique vers fait_accidents (sans FK formelle pour éviter contraintes entre partitions)';

COMMENT ON COLUMN accidents_gold.fait_vehicules.vehicule_type_id IS 
'FK vers dim_vehicule : type de véhicule (VL, moto, PL, etc.)';

COMMENT ON COLUMN accidents_gold.fait_vehicules.manoeuvre IS 
'1=Aucune, 2=Même sens/file, 9=Insertion, 10=Demi-tour, 11-12=Changement file, 15-16=Tournant, 17-18=Dépassement, 19=Traversant, 20=Stationnement, 23=Arrêté, 24=En stationnement';

COMMENT ON COLUMN accidents_gold.fait_vehicules.nb_occupants IS 
'Nombre d''usagers dans ce véhicule (calculé depuis fait_usagers)';