-- ========================================
-- VUE MATÉRIALISÉE : Statistiques hebdomadaires
-- Objectif: Détecter les semaines anormales (écarts à la moyenne)
-- Refresh: Après chaque chargement ETL
-- ========================================

DROP MATERIALIZED VIEW IF EXISTS accidents_gold.mv_stats_hebdomadaires CASCADE;

CREATE MATERIALIZED VIEW accidents_gold.mv_stats_hebdomadaires AS
SELECT 
    -- ================================
    -- PÉRIODE
    -- ================================
    d.annee,
    d.semaine_annee,
    MIN(d.date_complete) as date_debut_semaine,
    MAX(d.date_complete) as date_fin_semaine,
    
    -- ================================
    -- COMPTAGES ACCIDENTS
    -- ================================
    COUNT(DISTINCT f.accident_id) as nb_accidents,
    COUNT(DISTINCT CASE WHEN f.est_accident_mortel THEN f.accident_id END) as nb_accidents_mortels,
    COUNT(DISTINCT CASE WHEN f.est_accident_grave THEN f.accident_id END) as nb_accidents_graves,
    
    -- ================================
    -- COMPTAGES VICTIMES
    -- ================================
    SUM(f.nb_tues_total) as nb_tues,
    SUM(f.nb_blesses_hosp_total) as nb_blesses_hospitalises,
    SUM(f.nb_blesses_legers_total) as nb_blesses_legers,
    SUM(f.nb_victimes_total) as nb_victimes_total,
    SUM(f.nb_indemnes_total) as nb_indemnes,
    
    -- ================================
    -- SCORE DE GRAVITÉ
    -- ================================
    AVG(f.score_gravite_total) as gravite_moyenne,
    STDDEV(f.score_gravite_total) as ecart_type_gravite,
    MIN(f.score_gravite_total) as gravite_min,
    MAX(f.score_gravite_total) as gravite_max,
    
    -- ================================
    -- RÉPARTITION TEMPORELLE
    -- ================================
    COUNT(DISTINCT CASE WHEN f.est_weekend THEN f.accident_id END) as nb_accidents_weekend,
    COUNT(DISTINCT CASE WHEN f.est_nuit THEN f.accident_id END) as nb_accidents_nuit,
    
    -- ================================
    -- RÉPARTITION GÉOGRAPHIQUE
    -- ================================
    COUNT(DISTINCT CASE WHEN f.en_agglomeration THEN f.accident_id END) as nb_accidents_agglomeration,
    COUNT(DISTINCT CASE WHEN NOT f.en_agglomeration THEN f.accident_id END) as nb_accidents_hors_agglomeration,
    
    -- ================================
    -- MOYENNES PAR ACCIDENT
    -- ================================
    AVG(f.nb_vehicules) as nb_vehicules_moyen_par_accident,
    AVG(f.nb_victimes_total) as nb_victimes_moyen_par_accident,
    
    -- ================================
    -- RATIO GRAVITÉ
    -- ================================
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / COUNT(DISTINCT f.accident_id)) * 100 
        ELSE 0 
    END as taux_mortalite_par_accident,
    
    CASE 
        WHEN SUM(f.nb_victimes_total) > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / SUM(f.nb_victimes_total)) * 100 
        ELSE 0 
    END as taux_mortalite_par_victime

FROM accidents_gold.fait_accidents f
JOIN accidents_gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.annee, d.semaine_annee
ORDER BY d.annee, d.semaine_annee;

-- ================================
-- INDEX POUR PERFORMANCES
-- ================================

CREATE UNIQUE INDEX idx_mv_stats_hebdo_pk 
ON accidents_gold.mv_stats_hebdomadaires (annee, semaine_annee);

CREATE INDEX idx_mv_stats_hebdo_nb_accidents 
ON accidents_gold.mv_stats_hebdomadaires (nb_accidents);

CREATE INDEX idx_mv_stats_hebdo_gravite 
ON accidents_gold.mv_stats_hebdomadaires (gravite_moyenne DESC);

CREATE INDEX idx_mv_stats_hebdo_tues 
ON accidents_gold.mv_stats_hebdomadaires (nb_tues DESC);

-- ================================
-- COMMENTAIRES
-- ================================

COMMENT ON MATERIALIZED VIEW accidents_gold.mv_stats_hebdomadaires IS 
'Statistiques hebdomadaires pré-agrégées pour détection d''anomalies temporelles. Refresh: CONCURRENTLY après chaque ETL.';

COMMENT ON COLUMN accidents_gold.mv_stats_hebdomadaires.gravite_moyenne IS 
'Score moyen de gravité (tué=100, hosp=10, blessé=1) pour comparaisons';

COMMENT ON COLUMN accidents_gold.mv_stats_hebdomadaires.taux_mortalite_par_accident IS 
'Pourcentage : (nb_tués / nb_accidents) * 100';

COMMENT ON COLUMN accidents_gold.mv_stats_hebdomadaires.taux_mortalite_par_victime IS 
'Pourcentage : (nb_tués / nb_victimes) * 100 - Indicateur de gravité';

-- ================================
-- STATISTIQUES
-- ================================

ANALYZE accidents_gold.mv_stats_hebdomadaires;

-- ================================
-- VALIDATION
-- ================================

SELECT 
    'Vue matérialisée créée' as statut,
    COUNT(*) as nb_semaines,
    MIN(annee) as annee_min,
    MAX(annee) as annee_max
FROM accidents_gold.mv_stats_hebdomadaires;