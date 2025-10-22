-- ========================================
-- VUE MATÉRIALISÉE : Profil de risque par conditions
-- Objectif: Identifier combinaisons météo + route à risque
-- Refresh: Hebdomadaire ou après chargement données
-- ========================================

DROP MATERIALIZED VIEW IF EXISTS accidents_gold.mv_profil_risque_conditions CASCADE;

CREATE MATERIALIZED VIEW accidents_gold.mv_profil_risque_conditions AS
SELECT 
    -- ================================
    -- DIMENSIONS CONDITIONS
    -- ================================
    c.condition_id,
    c.luminosite_code,
    c.luminosite_libelle,
    c.est_nuit,
    c.atm_code,
    c.atm_libelle,
    c.est_intemperie,
    c.niveau_risque as niveau_risque_conditions,
    
    -- ================================
    -- DIMENSIONS ROUTE
    -- ================================
    r.route_id,
    r.categorie_route_code,
    r.categorie_route_libelle,
    r.profil_route_code,
    r.profil_route_libelle,
    r.trace_plan_code,
    r.trace_plan_libelle,
    r.etat_surface_code,
    r.etat_surface_libelle,
    r.niveau_risque_route,
    
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
    SUM(f.nb_blesses_hosp_total) as nb_blesses_hosp,
    SUM(f.nb_blesses_legers_total) as nb_blesses_legers,
    SUM(f.nb_victimes_total) as nb_victimes,
    
    -- ================================
    -- GRAVITÉ
    -- ================================
    AVG(f.score_gravite_total) as gravite_moyenne,
    STDDEV(f.score_gravite_total) as ecart_type_gravite,
    MAX(f.score_gravite_total) as gravite_max,
    
    -- ================================
    -- RATIOS DE GRAVITÉ
    -- ================================
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (COUNT(DISTINCT CASE WHEN f.est_accident_grave THEN f.accident_id END)::DECIMAL / COUNT(DISTINCT f.accident_id)) * 100 
        ELSE 0 
    END as pct_accidents_graves,
    
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (COUNT(DISTINCT CASE WHEN f.est_accident_mortel THEN f.accident_id END)::DECIMAL / COUNT(DISTINCT f.accident_id)) * 100 
        ELSE 0 
    END as pct_accidents_mortels,
    
    CASE 
        WHEN SUM(f.nb_victimes_total) > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / SUM(f.nb_victimes_total)) * 100 
        ELSE 0 
    END as taux_mortalite_par_victime,
    
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) > 0 
        THEN (SUM(f.nb_tues_total)::DECIMAL / COUNT(DISTINCT f.accident_id)) 
        ELSE 0 
    END as nb_tues_moyen_par_accident,
    
    -- ================================
    -- SCORE DE RISQUE COMPOSITE
    -- ================================
    CASE 
        WHEN COUNT(DISTINCT f.accident_id) >= 10 THEN
            -- Pondération: 50% gravité moyenne + 30% % accidents graves + 20% taux mortalité
            (AVG(f.score_gravite_total) / 100.0) * 0.5 +
            (COUNT(DISTINCT CASE WHEN f.est_accident_grave THEN f.accident_id END)::DECIMAL / COUNT(DISTINCT f.accident_id)) * 0.3 +
            (SUM(f.nb_tues_total)::DECIMAL / NULLIF(SUM(f.nb_victimes_total), 0)) * 0.2
        ELSE NULL
    END as score_risque_composite,
    
    -- ================================
    -- RÉPARTITION TEMPORELLE
    -- ================================
    COUNT(DISTINCT CASE WHEN f.est_weekend THEN f.accident_id END) as nb_accidents_weekend,
    COUNT(DISTINCT CASE WHEN f.en_agglomeration THEN f.accident_id END) as nb_accidents_agglomeration

FROM accidents_gold.fait_accidents f
JOIN accidents_gold.dim_conditions c ON f.condition_id = c.condition_id
JOIN accidents_gold.dim_route r ON f.route_id = r.route_id
GROUP BY 
    c.condition_id, c.luminosite_code, c.luminosite_libelle, c.est_nuit,
    c.atm_code, c.atm_libelle, c.est_intemperie, c.niveau_risque,
    r.route_id, r.categorie_route_code, r.categorie_route_libelle,
    r.profil_route_code, r.profil_route_libelle,
    r.trace_plan_code, r.trace_plan_libelle,
    r.etat_surface_code, r.etat_surface_libelle,
    r.niveau_risque_route
HAVING COUNT(DISTINCT f.accident_id) >= 10;  -- Seuil minimum pour significativité

-- ================================
-- INDEX POUR PERFORMANCES
-- ================================

CREATE UNIQUE INDEX idx_mv_profil_risque_pk 
ON accidents_gold.mv_profil_risque_conditions (condition_id, route_id);

CREATE INDEX idx_mv_profil_risque_nb_accidents 
ON accidents_gold.mv_profil_risque_conditions (nb_accidents DESC);

CREATE INDEX idx_mv_profil_risque_gravite 
ON accidents_gold.mv_profil_risque_conditions (gravite_moyenne DESC);

CREATE INDEX idx_mv_profil_risque_pct_graves 
ON accidents_gold.mv_profil_risque_conditions (pct_accidents_graves DESC);

CREATE INDEX idx_mv_profil_risque_score 
ON accidents_gold.mv_profil_risque_conditions (score_risque_composite DESC NULLS LAST);

CREATE INDEX idx_mv_profil_risque_conditions 
ON accidents_gold.mv_profil_risque_conditions (est_nuit, est_intemperie);

CREATE INDEX idx_mv_profil_risque_surface 
ON accidents_gold.mv_profil_risque_conditions (etat_surface_code);

-- ================================
-- COMMENTAIRES
-- ================================

COMMENT ON MATERIALIZED VIEW accidents_gold.mv_profil_risque_conditions IS 
'Profil de risque par combinaison conditions (météo + luminosité) × route. Seuil: >=10 accidents.';

COMMENT ON COLUMN accidents_gold.mv_profil_risque_conditions.score_risque_composite IS 
'Score pondéré : 50% gravité + 30% % graves + 20% taux mortalité. NULL si <10 accidents (non significatif)';

COMMENT ON COLUMN accidents_gold.mv_profil_risque_conditions.pct_accidents_graves IS 
'Pourcentage d''accidents avec >=1 tué ou hospitalisé';

COMMENT ON COLUMN accidents_gold.mv_profil_risque_conditions.taux_mortalite_par_victime IS 
'(Tués / Total victimes) * 100 - Indicateur de létalité';

-- ================================
-- STATISTIQUES
-- ================================

ANALYZE accidents_gold.mv_profil_risque_conditions;

-- ================================
-- VALIDATION
-- ================================

SELECT 
    'Vue matérialisée créée' as statut,
    COUNT(*) as nb_combinaisons,
    SUM(nb_accidents) as total_accidents,
    SUM(nb_tues) as total_tues,
    AVG(gravite_moyenne) as gravite_moyenne_globale,
    MAX(score_risque_composite) as score_risque_max
FROM accidents_gold.mv_profil_risque_conditions;

-- Top 10 combinaisons les plus dangereuses
SELECT 
    luminosite_libelle,
    atm_libelle,
    categorie_route_libelle,
    etat_surface_libelle,
    nb_accidents,
    pct_accidents_graves,
    score_risque_composite
FROM accidents_gold.mv_profil_risque_conditions
WHERE score_risque_composite IS NOT NULL
ORDER BY score_risque_composite DESC
LIMIT 10;