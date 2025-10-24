-- ========================================
-- COUCHE SILVER : Index pour ETL et analyses
-- Objectif: Optimiser jointures et transformations Gold
-- ========================================

-- ================================
-- INDEX TABLE ACCIDENTS
-- ================================

-- Index temporels (ETL incrémental et filtres)
CREATE INDEX idx_silver_accidents_date 
ON accidents_silver.accidents(date_accident);

CREATE INDEX idx_silver_accidents_annee_mois 
ON accidents_silver.accidents(annee, mois);

-- Index géographiques (jointures dimensions)
CREATE INDEX idx_silver_accidents_commune 
ON accidents_silver.accidents(com_code) 
WHERE com_code IS NOT NULL;

CREATE INDEX idx_silver_accidents_dept 
ON accidents_silver.accidents(departement_code) 
WHERE departement_code IS NOT NULL;

-- Index coordonnées (analyse spatiale)
CREATE INDEX idx_silver_accidents_coords 
ON accidents_silver.accidents(latitude, longitude) 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Index conditions (agrégations)
CREATE INDEX idx_silver_accidents_conditions 
ON accidents_silver.accidents(luminosite, conditions_atmospheriques);

-- ================================
-- INDEX TABLE LIEUX
-- ================================

CREATE INDEX idx_silver_lieux_categorie 
ON accidents_silver.lieux(categorie_route) 
WHERE categorie_route IS NOT NULL;

CREATE INDEX idx_silver_lieux_surface 
ON accidents_silver.lieux(etat_surface) 
WHERE etat_surface IS NOT NULL;

-- ================================
-- INDEX TABLE VEHICULES
-- ================================

CREATE INDEX idx_silver_vehicules_num_acc 
ON accidents_silver.vehicules(num_acc);

CREATE INDEX idx_silver_vehicules_categorie 
ON accidents_silver.vehicules(categorie_vehicule) 
WHERE categorie_vehicule IS NOT NULL;

-- ================================
-- INDEX TABLE USAGERS
-- ================================

CREATE INDEX idx_silver_usagers_num_acc 
ON accidents_silver.usagers(num_acc);

CREATE INDEX idx_silver_usagers_gravite 
ON accidents_silver.usagers(gravite);

CREATE INDEX idx_silver_usagers_age 
ON accidents_silver.usagers(age_au_moment_accident) 
WHERE age_au_moment_accident IS NOT NULL;

CREATE INDEX idx_silver_usagers_categorie 
ON accidents_silver.usagers(categorie_usager) 
WHERE categorie_usager IS NOT NULL;

-- Statistiques
ANALYZE accidents_silver.accidents;
ANALYZE accidents_silver.lieux;
ANALYZE accidents_silver.vehicules;
ANALYZE accidents_silver.usagers;

-- Rapport
SELECT 
    schemaname,
    tablename,
    COUNT(*) as nb_index
FROM pg_indexes 
WHERE schemaname = 'accidents_silver'
GROUP BY schemaname, tablename
ORDER BY tablename;