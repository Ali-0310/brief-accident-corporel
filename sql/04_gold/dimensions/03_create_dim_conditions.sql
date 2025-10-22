-- ========================================
-- DIMENSION : Conditions (météo + luminosité)
-- Grain: 1 ligne = 1 combinaison météo×luminosité
-- Description: Conditions environnementales
-- ========================================

DROP TABLE IF EXISTS accidents_gold.dim_conditions CASCADE;

CREATE TABLE accidents_gold.dim_conditions (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    condition_id SERIAL PRIMARY KEY,
    
    -- ================================
    -- LUMINOSITÉ
    -- ================================
    luminosite_code INTEGER NOT NULL CHECK (luminosite_code BETWEEN 1 AND 5),
    luminosite_libelle VARCHAR(50) NOT NULL,
    est_nuit BOOLEAN NOT NULL,
    
    -- ================================
    -- MÉTÉO
    -- ================================
    atm_code INTEGER NOT NULL CHECK (atm_code BETWEEN 1 AND 9),
    atm_libelle VARCHAR(50) NOT NULL,
    est_intemperie BOOLEAN NOT NULL,
    
    -- ================================
    -- SCORE DE RISQUE
    -- ================================
    niveau_risque INTEGER CHECK (niveau_risque BETWEEN 1 AND 3),  -- 1=faible, 2=moyen, 3=élevé
    
    -- ================================
    -- CONTRAINTE UNICITÉ
    -- ================================
    UNIQUE (luminosite_code, atm_code)
);

COMMENT ON TABLE accidents_gold.dim_conditions IS 
'Dimension conditions : combinaison luminosité × météo. Grain: 1 combinaison';

COMMENT ON COLUMN accidents_gold.dim_conditions.luminosite_libelle IS 
'Plein jour, Crépuscule/aube, Nuit sans éclairage, Nuit éclairage non allumé, Nuit éclairage allumé';

COMMENT ON COLUMN accidents_gold.dim_conditions.atm_libelle IS 
'Normal, Pluie légère, Pluie forte, Neige/grêle, Brouillard, Vent fort, Éblouissant, Couvert, Autre';

-- Index
CREATE INDEX idx_dim_conditions_risque ON accidents_gold.dim_conditions(niveau_risque);
CREATE INDEX idx_dim_conditions_nuit ON accidents_gold.dim_conditions(est_nuit);
CREATE INDEX idx_dim_conditions_intemperie ON accidents_gold.dim_conditions(est_intemperie);