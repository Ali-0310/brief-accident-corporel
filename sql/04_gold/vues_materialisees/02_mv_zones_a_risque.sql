-- ========================================
-- VUE MATÉRIALISÉE : Zones à risque
-- Objectif: Identifier zones géographiques dangereuses
-- Refresh: Hebdomadaire ou après chargement données
-- ========================================

DROP MATERIALIZED VIEW IF EXISTS accidents_gold.mv_zones_a_risque CASCADE;

CREATE MATERIALIZED VIEW accidents_gold.mv_zones_a_risque AS
SELECT 
    -- ================================
    -- GÉOGRAPHIE
    -- ================================
    g.geo_id,
    g.com_code,
    g.com_name,
    g.com_arm_name,
    g.departement_code,
    g.departement_name,
    g.region_code,
    g.region_name,
    g.type_zone,
    g.population,
    g.densite_population,
    
    -- ================================
    -- COMPTAGES ACCIDENTS
    -- ================================
    COUNT(DISTINCT f.accident_id) as nb_accidents_total,
    COUNT(DISTINCT CASE WHEN f.est_accident_mortel THEN f.accident_id END) as nb_accidents_mortels,
    COUNT(DISTINCT CASE WHEN f.est_accident_grave THEN f.accident_id END) as nb_accidents_graves,
    
    -- ================================
    -- COMPTAGES VICTIMES
    -- ================================
    SUM(f.nb_tues_total) as nb_tues_total,
    SUM(f.nb_blesses_hosp_total) as nb_blesses_hosp_total,
    SUM(f.nb_blesses_legers_total) as nb_blesses_legers_total,
    SUM(f.nb_victimes_total) as nb_victimes_total,
    
    -- ================================
    -- GRAVITÉ
    -- ================================
    AVG(f.score_gravite_total) as gravite_moyenne,
    MAX(f.score_gravite_total) as gravite_max,
    
    -- ================================
    -- RÉPARTITION TEMPORELLE
    -- ================================
    COUNT(DISTINCT CASE WHEN f.est_weekend THEN f.accident_id END) as nb_accidents_weekend,
    COUNT(DISTINCT CASE WHEN f.est_nuit THEN f.accident_id END) as nb_accidents_nuit,
    
    -- ================================
    -- RÉPARTITION SPATIALE
    -- ================================
    COUNT(DISTINCT CASE WHEN f.en_agglomeration THEN f.accident_id END) as nb_accidents_agglomeration,
    COUNT(DISTINCT CASE WHEN NOT f.en_agglomeration THEN f.accident_id END) as nb_accidents_hors_agglomeration,
    
    -- ================================
    -- NORMALISATION PAR POPULATION
    -- (Indicateurs clés pour comparaison)
    -- ================================
    CASE 
        WHEN g.population > 0 
        THEN (COUNT(DISTINCT f.accident_id)::DECIMAL / g.population) * 10000 
        ELSE NULL 
    END as taux_accidents_pour_10k_hab,
    
    CASE 
        WHEN g.population > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / g.population) * 100000 
        ELSE NULL 
    END as taux_tues_pour_100k_hab,
    
    CASE 
        WHEN g.population > 0 
        THEN (SUM(f.nb_victimes_total)::DECIMAL / g.population) * 10000 
        ELSE NULL 
    END as taux_victimes_pour_10k_hab,
    
    -- ================================
    -- RATIO GRAVITÉ
    -- ================================
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (COUNT(DISTINCT CASE WHEN f.est_accident_grave THEN f.accident_id END)::DECIMAL / COUNT(DISTINCT f.accident_id)) * 100 
        ELSE 0 
    END as pct_accidents_graves,
    
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / COUNT(DISTINCT f.accident_id)) 
        ELSE 0 
    END as nb_tues_moyen_par_accident,
    
    -- ================================
    -- PÉRIODE D'ANALYSE
    -- ================================
    MIN(d.date_complete) as premiere_date,
    MAX(d.date_complete) as derniere_date,
    COUNT(DISTINCT d.annee) as nb_annees_observees

FROM accidents_gold.fait_accidents f
JOIN accidents_gold.dim_geographie g ON f.geo_id = g.geo_id
JOIN accidents_gold.dim_date d ON f.date_id = d.date_id
GROUP BY 
    g.geo_id, g.com_code, g.com_name, g.com_arm_name,
    g.departement_code, g.departement_name,
    g.region_code, g.region_name,
    g.type_zone, g.population, g.densite_population
HAVING COUNT(DISTINCT f.accident_id) >= 3;  -- Seuil minimum pour significativité statistique

-- ================================
-- INDEX POUR PERFORMANCES
-- ================================

CREATE UNIQUE INDEX idx_mv_zones_risque_pk 
ON accidents_gold.mv_zones_a_risque (geo_id);

CREATE INDEX idx_mv_zones_risque_dept 
ON accidents_gold.mv_zones_a_risque (departement_code);

CREATE INDEX idx_mv_zones_risque_region 
ON accidents_gold.mv_zones_a_risque (region_code);

CREATE INDEX idx_mv_zones_risque_nb_accidents 
ON accidents_gold.mv_zones_a_risque (nb_accidents_total DESC);

CREATE INDEX idx_mv_zones_risque_gravite 
ON accidents_gold.mv_zones_a_risque (gravite_moyenne DESC);

CREATE INDEX idx_mv_zones_risque_taux_norm 
ON accidents_gold.mv_zones_a_risque (taux_accidents_pour_10k_hab DESC NULLS LAST);

CREATE INDEX idx_mv_zones_risque_taux_tues 
ON accidents_gold.mv_zones_a_risque (taux_tues_pour_100k_hab DESC NULLS LAST);

CREATE INDEX idx_mv_zones_risque_pct_graves 
ON accidents_gold.mv_zones_a_risque (pct_accidents_graves DESC);

-- ================================
-- COMMENTAIRES
-- ================================

COMMENT ON MATERIALIZED VIEW accidents_gold.mv_zones_a_risque IS 
'Zones géographiques à risque avec normalisation par population. Seuil: >=3 accidents pour significativité.';

COMMENT ON COLUMN accidents_gold.mv_zones_a_risque.taux_accidents_pour_10k_hab IS 
'Nombre d''accidents pour 10 000 habitants - Permet comparaison entre communes';

COMMENT ON COLUMN accidents_gold.mv_zones_a_risque.taux_tues_pour_100k_hab IS 
'Nombre de tués pour 100 000 habitants - Indicateur mortalité routière normalisé';

COMMENT ON COLUMN accidents_gold.mv_zones_a_risque.pct_accidents_graves IS 
'Pourcentage d''accidents graves (tués ou hospitalisés) sur total accidents';

COMMENT ON COLUMN accidents_gold.mv_zones_a_risque.nb_tues_moyen_par_accident IS 
'Indicateur de létalité : moyenne de tués par accident';

-- ================================
-- STATISTIQUES
-- ================================

ANALYZE accidents_gold.mv_zones_a_risque;

-- ================================
-- VALIDATION
-- ================================

SELECT 
    'Vue matérialisée créée' as statut,
    COUNT(*) as nb_communes,
    COUNT(DISTINCT departement_code) as nb_departements,
    COUNT(DISTINCT region_code) as nb_regions,
    SUM(nb_accidents_total) as total_accidents,
    SUM(nb_tues_total) as total_tues
FROM accidents_gold.mv_zones_a_risque;