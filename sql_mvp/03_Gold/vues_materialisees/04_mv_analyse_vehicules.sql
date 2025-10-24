-- ========================================
-- VUE MATÉRIALISÉE : Analyse par type de véhicule
-- Objectif: Statistiques de risque par catégorie de véhicule
-- Refresh: Hebdomadaire
-- ========================================

DROP MATERIALIZED VIEW IF EXISTS accidents_gold.mv_analyse_vehicules CASCADE;

CREATE MATERIALIZED VIEW accidents_gold.mv_analyse_vehicules AS
SELECT 
    -- ================================
    -- DIMENSION VÉHICULE
    -- ================================
    dv.vehicule_id,
    dv.categorie_code,
    dv.categorie_libelle,
    dv.type_vehicule,
    dv.est_motorise,
    dv.niveau_protection,
    
    -- ================================
    -- COMPTAGES VÉHICULES
    -- ================================
    COUNT(DISTINCT fv.vehicule_id) as nb_vehicules_impliques,
    COUNT(DISTINCT fv.accident_fk) as nb_accidents_impliques,
    
    -- ================================
    -- COMPTAGES VICTIMES
    -- ================================
    SUM(fv.nb_occupants) as nb_occupants_total,
    SUM(fv.nb_tues_vehicule) as nb_tues_total,
    SUM(fv.nb_blesses_vehicule) as nb_blesses_total,
    SUM(fv.nb_indemnes_vehicule) as nb_indemnes_total,
    
    -- ================================
    -- GRAVITÉ
    -- ================================
    COUNT(DISTINCT CASE WHEN fv.est_vehicule_implique_mortel THEN fv.vehicule_id END) as nb_vehicules_avec_tues,
    
    -- ================================
    -- RATIOS DE RISQUE
    -- ================================
    CASE 
        WHEN SUM(fv.nb_occupants) > 0 
        THEN (SUM(fv.nb_tues_vehicule)::DECIMAL / SUM(fv.nb_occupants)) * 100 
        ELSE 0 
    END as taux_mortalite_occupants,
    
    CASE 
        WHEN COUNT(DISTINCT fv.vehicule_id) > 0 
        THEN (SUM(fv.nb_tues_vehicule)::DECIMAL / COUNT(DISTINCT fv.vehicule_id)) 
        ELSE 0 
    END as nb_tues_moyen_par_vehicule,
    
    CASE 
        WHEN COUNT(DISTINCT fv.vehicule_id) > 0 
        THEN (COUNT(DISTINCT CASE WHEN fv.est_vehicule_implique_mortel THEN fv.vehicule_id END)::DECIMAL / COUNT(DISTINCT fv.vehicule_id)) * 100 
        ELSE 0 
    END as pct_vehicules_avec_tues,
    
    -- ================================
    -- OBSTACLES
    -- ================================
    COUNT(DISTINCT CASE WHEN fv.a_heurte_obstacle_fixe THEN fv.vehicule_id END) as nb_avec_obstacle_fixe,
    COUNT(DISTINCT CASE WHEN fv.a_heurte_pieton THEN fv.vehicule_id END) as nb_ayant_heurte_pieton,
    
    -- ================================
    -- MANŒUVRES FRÉQUENTES
    -- ================================
    MODE() WITHIN GROUP (ORDER BY fv.manoeuvre) as manoeuvre_la_plus_frequente

FROM accidents_gold.fait_vehicules fv
JOIN accidents_gold.dim_vehicule dv ON fv.vehicule_type_id = dv.vehicule_id
GROUP BY 
    dv.vehicule_id, dv.categorie_code, dv.categorie_libelle,
    dv.type_vehicule, dv.est_motorise, dv.niveau_protection
HAVING COUNT(DISTINCT fv.vehicule_id) >= 5;  -- Seuil minimum

-- ================================
-- INDEX
-- ================================

CREATE UNIQUE INDEX idx_mv_analyse_veh_pk 
ON accidents_gold.mv_analyse_vehicules (vehicule_id);

CREATE INDEX idx_mv_analyse_veh_type 
ON accidents_gold.mv_analyse_vehicules (type_vehicule);

CREATE INDEX idx_mv_analyse_veh_taux_mortalite 
ON accidents_gold.mv_analyse_vehicules (taux_mortalite_occupants DESC);

-- ================================
-- COMMENTAIRES
-- ================================

COMMENT ON MATERIALIZED VIEW accidents_gold.mv_analyse_vehicules IS 
'Statistiques de risque par type de véhicule. Seuil: >=5 véhicules.';

COMMENT ON COLUMN accidents_gold.mv_analyse_vehicules.taux_mortalite_occupants IS 
'(Tués / Total occupants) * 100 - Indicateur vulnérabilité type véhicule';

-- ================================
-- STATISTIQUES
-- ================================

ANALYZE accidents_gold.mv_analyse_vehicules;