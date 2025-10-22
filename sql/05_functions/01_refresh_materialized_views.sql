-- ========================================
-- FONCTION : Rafraîchir toutes les vues matérialisées
-- Usage: SELECT accidents_gold.refresh_all_materialized_views();
-- ========================================

CREATE OR REPLACE FUNCTION accidents_gold.refresh_all_materialized_views()
RETURNS TABLE (
    vue_name TEXT,
    statut TEXT,
    duree_secondes NUMERIC
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Vue 1: Stats hebdomadaires
    start_time := clock_timestamp();
    RAISE NOTICE 'Refresh mv_stats_hebdomadaires...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY accidents_gold.mv_stats_hebdomadaires;
    end_time := clock_timestamp();
    vue_name := 'mv_stats_hebdomadaires';
    statut := 'OK';
    duree_secondes := EXTRACT(EPOCH FROM (end_time - start_time));
    RETURN NEXT;
    
    -- Vue 2: Zones à risque
    start_time := clock_timestamp();
    RAISE NOTICE 'Refresh mv_zones_a_risque...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY accidents_gold.mv_zones_a_risque;
    end_time := clock_timestamp();
    vue_name := 'mv_zones_a_risque';
    statut := 'OK';
    duree_secondes := EXTRACT(EPOCH FROM (end_time - start_time));
    RETURN NEXT;
    
    -- Vue 3: Profil risque conditions
    start_time := clock_timestamp();
    RAISE NOTICE 'Refresh mv_profil_risque_conditions...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY accidents_gold.mv_profil_risque_conditions;
    end_time := clock_timestamp();
    vue_name := 'mv_profil_risque_conditions';
    statut := 'OK';
    duree_secondes := EXTRACT(EPOCH FROM (end_time - start_time));
    RETURN NEXT;
    
    -- Vue 4: Analyse véhicules
    start_time := clock_timestamp();
    RAISE NOTICE 'Refresh mv_analyse_vehicules...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY accidents_gold.mv_analyse_vehicules;
    end_time := clock_timestamp();
    vue_name := 'mv_analyse_vehicules';
    statut := 'OK';
    duree_secondes := EXTRACT(EPOCH FROM (end_time - start_time));
    RETURN NEXT;
    
    RAISE NOTICE 'Toutes les vues matérialisées ont été rafraîchies avec succès';
    
EXCEPTION
    WHEN OTHERS THEN
        statut := 'ERREUR: ' || SQLERRM;
        duree_secondes := NULL;
        RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION accidents_gold.refresh_all_materialized_views() IS 
'Rafraîchit toutes les vues matérialisées de la couche Gold avec CONCURRENTLY (sans lock)';

-- Test de la fonction
SELECT * FROM accidents_gold.refresh_all_materialized_views();