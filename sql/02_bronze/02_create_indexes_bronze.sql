-- ========================================
-- COUCHE BRONZE : Index minimaux
-- Objectif: Traçabilité et déduplication
-- ========================================

-- Index sur identifiant accident (déduplication)
CREATE INDEX idx_bronze_num_acc 
ON accidents_bronze.raw_accidents(num_acc);

COMMENT ON INDEX accidents_bronze.idx_bronze_num_acc IS 
'Index pour identifier les doublons et tracer les accidents en Silver';

-- Index sur horodatage (requêtes incrémentales)
CREATE INDEX idx_bronze_load_ts 
ON accidents_bronze.raw_accidents(load_timestamp);

COMMENT ON INDEX accidents_bronze.idx_bronze_load_ts IS 
'Index pour charger uniquement les nouvelles données (ETL incrémental)';

-- Index sur année (partitionnement virtuel)
CREATE INDEX idx_bronze_annee 
ON accidents_bronze.raw_accidents(an) 
WHERE an IS NOT NULL;

COMMENT ON INDEX accidents_bronze.idx_bronze_annee IS 
'Index pour filtrer par année lors des transformations Silver';

-- Statistiques
ANALYZE accidents_bronze.raw_accidents;

-- Rapport
SELECT 
    'Index Bronze créés' as statut,
    COUNT(*) as nb_index
FROM pg_indexes 
WHERE schemaname = 'accidents_bronze' 
  AND tablename = 'raw_accidents';