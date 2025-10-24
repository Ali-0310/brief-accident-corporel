-- ========================================
-- SCRIPT MAÎTRE : Création Architecture Complète
-- Exécution modulaire étape par étape
-- Usage: \i sql/99_master_schema_creation.sql
-- ========================================

-- ================================
-- CONFIGURATION
-- ================================
\set ON_ERROR_STOP on
\set ECHO all
\timing on

-- Message de bienvenue
\echo ''
\echo '========================================='
\echo 'CRÉATION ARCHITECTURE ACCIDENTS CORPORELS'
\echo 'Architecture: Bronze / Silver / Gold'
\echo '========================================='
\echo ''

-- ================================
-- ÉTAPE 0 : Vérification Prérequis
-- ================================
\echo '>>> ÉTAPE 0: Vérification prérequis'
SELECT version() as postgresql_version;
SELECT current_database() as database_actuelle;
SELECT current_user as utilisateur;

\echo ''
\echo 'Continuer avec ÉTAPE 1 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer les SCHÉMAS...' dummy

-- ================================
-- ÉTAPE 1 : CRÉATION DES SCHÉMAS
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 1: Création des schémas'
\echo '========================================='
\i sql/01_create_schemas.sql

\echo ''
\echo '>>> ÉTAPE 1 terminée. Vérification:'
SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'accidents_%' ORDER BY schema_name;

\echo ''
\echo 'Continuer avec ÉTAPE 2 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer la COUCHE BRONZE...' dummy

-- ================================
-- ÉTAPE 2 : COUCHE BRONZE
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 2: Création couche BRONZE (staging)'
\echo '========================================='

\echo '>>> 2.1 - Table raw_accidents'
\i sql/02_bronze/01_create_table_raw_accidents.sql

\echo '>>> 2.2 - Index Bronze'
\i sql/02_bronze/02_create_indexes_bronze.sql

\echo ''
\echo '>>> ÉTAPE 2 terminée. Vérification:'
SELECT 
    schemaname, 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as taille
FROM pg_tables 
WHERE schemaname = 'accidents_bronze'
ORDER BY tablename;

\echo ''
\echo 'Continuer avec ÉTAPE 3 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer la COUCHE SILVER...' dummy

-- ================================
-- ÉTAPE 3 : COUCHE SILVER
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 3: Création couche SILVER (nettoyée)'
\echo '========================================='

\echo '>>> 3.1 - Table accidents'
\i sql/03_silver/01_create_table_accidents.sql

\echo '>>> 3.2 - Table lieux'
\i sql/03_silver/02_create_table_lieux.sql

\echo '>>> 3.3 - Table vehicules'
\i sql/03_silver/03_create_table_vehicules.sql

\echo '>>> 3.4 - Table usagers'
\i sql/03_silver/04_create_table_usagers.sql

\echo '>>> 3.5 - Index Silver'
\i sql/03_silver/05_create_indexes_silver.sql

\echo ''
\echo '>>> ÉTAPE 3 terminée. Vérification:'
SELECT 
    schemaname, 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as taille
FROM pg_tables 
WHERE schemaname = 'accidents_silver'
ORDER BY tablename;

\echo ''
\echo 'Continuer avec ÉTAPE 4 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer les DIMENSIONS GOLD...' dummy

-- ================================
-- ÉTAPE 4 : COUCHE GOLD - DIMENSIONS
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 4: Création couche GOLD - Dimensions'
\echo '========================================='

\echo '>>> 4.1 - Dimension Date'
\i sql/04_gold/dimensions/01_create_dim_date.sql

\echo '>>> 4.2 - Dimension Géographie'
\i sql/04_gold/dimensions/02_create_dim_geographie.sql

\echo '>>> 4.3 - Dimension Conditions'
\i sql/04_gold/dimensions/03_create_dim_conditions.sql

\echo '>>> 4.4 - Dimension Route'
\i sql/04_gold/dimensions/04_create_dim_route.sql

\echo '>>> 4.5 - Dimension Véhicule'
\i sql/04_gold/dimensions/05_create_dim_vehicule.sql

\echo ''
\echo '>>> ÉTAPE 4 terminée. Vérification:'
SELECT 
    schemaname, 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as taille
FROM pg_tables 
WHERE schemaname = 'accidents_gold'
  AND tablename LIKE 'dim_%'
ORDER BY tablename;

\echo ''
\echo 'Continuer avec ÉTAPE 5 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer les TABLES DE FAITS...' dummy

-- ================================
-- ÉTAPE 5 : COUCHE GOLD - FAITS
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 5: Création couche GOLD - Tables de Faits'
\echo '========================================='

\echo '>>> 5.1 - Fait Accidents'
\i sql/04_gold/faits/01_create_fait_accidents.sql

\echo '>>> 5.2 - Fait Véhicules'
\i sql/04_gold/faits/02_create_fait_vehicules.sql

\echo '>>> 5.3 - Fait Usagers'
\i sql/04_gold/faits/03_create_fait_usagers.sql

\echo '>>> 5.4 - Index Faits'
\i sql/04_gold/faits/04_create_indexes_faits.sql

\echo ''
\echo '>>> ÉTAPE 5 terminée. Vérification:'
SELECT 
    schemaname, 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as taille
FROM pg_tables 
WHERE schemaname = 'accidents_gold'
  AND tablename LIKE 'fait_%'
ORDER BY tablename;

\echo ''
\echo 'Continuer avec ÉTAPE 6 ? (Ctrl+C pour annuler, ENTER pour continuer)'
\prompt 'Appuyez sur ENTER pour créer les VUES MATÉRIALISÉES...' dummy

-- ================================
-- ÉTAPE 6 : VUES MATÉRIALISÉES
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 6: Création Vues Matérialisées'
\echo '========================================='

\echo '>>> 6.1 - Stats Hebdomadaires'
\i sql/04_gold/vues_materialisees/01_mv_stats_hebdomadaires.sql

\echo '>>> 6.2 - Zones à Risque'
\i sql/04_gold/vues_materialisees/02_mv_zones_a_risque.sql

\echo '>>> 6.3 - Profil Risque Conditions'
\i sql/04_gold/vues_materialisees/03_mv_profil_risque_conditions.sql

\echo '>>> 6.4 - Analyse Véhicules'
\i sql/04_gold/vues_materialisees/04_mv_analyse_vehicules.sql

\echo '>>> 6.5 - Fonction Refresh'
\i sql/04_gold/vues_materialisees/05_refresh_all_views.sql

\echo ''
\echo '>>> ÉTAPE 6 terminée. Vérification:'
SELECT 
    schemaname,
    matviewname as nom_vue,
    ispopulated as est_peuplee,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as taille
FROM pg_matviews
WHERE schemaname = 'accidents_gold'
ORDER BY matviewname;

-- ================================
-- ÉTAPE 7 : RAPPORT FINAL
-- ================================
\echo ''
\echo '========================================='
\echo 'ÉTAPE 7: Rapport Final'
\echo '========================================='

\echo ''
\echo '>>> SCHÉMAS CRÉÉS:'
SELECT schema_name, 
       (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = schema_name) as nb_tables
FROM information_schema.schemata 
WHERE schema_name LIKE 'accidents_%'
ORDER BY schema_name;

\echo ''
\echo '>>> TABLES PAR SCHÉMA:'
SELECT 
    schemaname,
    COUNT(*) as nb_tables,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||tablename))) as taille_totale
FROM pg_tables 
WHERE schemaname LIKE 'accidents_%'
GROUP BY schemaname
ORDER BY schemaname;

\echo ''
\echo '>>> INDEX CRÉÉS:'
SELECT 
    schemaname,
    COUNT(*) as nb_index,
    pg_size_pretty(SUM(pg_relation_size(indexname::regclass))) as taille_totale
FROM pg_indexes 
WHERE schemaname LIKE 'accidents_%'
GROUP BY schemaname
ORDER BY schemaname;

\echo ''
\echo '>>> VUES MATÉRIALISÉES:'
SELECT 
    COUNT(*) as nb_vues_materialisees,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname||'.'||matviewname))) as taille_totale
FROM pg_matviews
WHERE schemaname = 'accidents_gold';

\echo ''
\echo '========================================='
\echo '✅ CRÉATION TERMINÉE AVEC SUCCÈS'
\echo '========================================='
\echo ''
\echo 'Prochaines étapes:'
\echo '  1. Charger les données CSV dans Bronze'
\echo '  2. Exécuter les scripts ETL Python'
\echo '  3. Rafraîchir les vues matérialisées'
\echo ''
\echo 'Commande pour refresh vues:'
\echo '  SELECT * FROM accidents_gold.refresh_all_materialized_views();'
\echo ''