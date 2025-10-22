-- ========================================
-- NETTOYAGE COMPLET : Suppression schémas
-- ========================================

DROP SCHEMA IF EXISTS accidents_bronze CASCADE;
DROP SCHEMA IF EXISTS accidents_silver CASCADE;
DROP SCHEMA IF EXISTS accidents_gold CASCADE;

-- Vérification
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name LIKE 'accidents_%';
-- Résultat attendu : 0 lignes