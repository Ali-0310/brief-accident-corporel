-- ========================================
-- DIMENSION : Calendrier (dim_date)
-- Grain: 1 ligne = 1 jour
-- Description: Dimension temporelle complète
-- ========================================

DROP TABLE IF EXISTS accidents_gold.dim_date CASCADE;

CREATE TABLE accidents_gold.dim_date (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    date_id INTEGER PRIMARY KEY,  -- Format: YYYYMMDD (ex: 20150315)
    date_complete DATE UNIQUE NOT NULL,
    
    -- ================================
    -- COMPOSANTS TEMPORELS
    -- ================================
    annee INTEGER NOT NULL,
    mois INTEGER NOT NULL CHECK (mois BETWEEN 1 AND 12),
    jour INTEGER NOT NULL CHECK (jour BETWEEN 1 AND 31),
    trimestre INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semestre INTEGER NOT NULL CHECK (semestre IN (1, 2)),
    
    -- ================================
    -- JOUR DE LA SEMAINE
    -- ================================
    jour_semaine INTEGER NOT NULL CHECK (jour_semaine BETWEEN 1 AND 7),  -- 1=Lundi
    nom_jour VARCHAR(10) NOT NULL,
    
    -- ================================
    -- MOIS
    -- ================================
    nom_mois VARCHAR(10) NOT NULL,
    
    -- ================================
    -- SEMAINE
    -- ================================
    semaine_annee INTEGER NOT NULL CHECK (semaine_annee BETWEEN 1 AND 53),
    jour_annee INTEGER NOT NULL CHECK (jour_annee BETWEEN 1 AND 366),
    
    -- ================================
    -- FLAGS BOOLÉENS
    -- ================================
    est_weekend BOOLEAN NOT NULL,
    est_jour_ferie BOOLEAN DEFAULT FALSE,
    nom_jour_ferie VARCHAR(50),
    
    -- ================================
    -- SAISON
    -- ================================
    saison VARCHAR(10) NOT NULL CHECK (saison IN ('Hiver', 'Printemps', 'Été', 'Automne')),
    
    -- ================================
    -- PÉRIODE SCOLAIRE (optionnel)
    -- ================================
    periode_vacances VARCHAR(50)
);

COMMENT ON TABLE accidents_gold.dim_date IS 
'Dimension calendrier : de 2005 à 2025. Grain: 1 jour';

COMMENT ON COLUMN accidents_gold.dim_date.date_id IS 
'Clé technique au format YYYYMMDD (ex: 20150315 pour 15 mars 2015)';

COMMENT ON COLUMN accidents_gold.dim_date.jour_semaine IS 
'1=Lundi, 2=Mardi, 3=Mercredi, 4=Jeudi, 5=Vendredi, 6=Samedi, 7=Dimanche (norme ISO)';

-- Index
CREATE INDEX idx_dim_date_annee_mois ON accidents_gold.dim_date(annee, mois);
CREATE INDEX idx_dim_date_semaine ON accidents_gold.dim_date(annee, semaine_annee);
CREATE INDEX idx_dim_date_weekend ON accidents_gold.dim_date(est_weekend);