-- ========================================
-- Création des schémas Bronze/Silver/Gold
-- Auteur: Data Engineering Team
-- Description: Création des 3 couches de l'architecture medallion
-- ========================================

-- Schéma Bronze : Données brutes
CREATE SCHEMA IF NOT EXISTS accidents_bronze;
COMMENT ON SCHEMA accidents_bronze IS 'Couche Bronze : Données brutes issues des CSV (staging)';

-- Schéma Silver : Données nettoyées
CREATE SCHEMA IF NOT EXISTS accidents_silver;
COMMENT ON SCHEMA accidents_silver IS 'Couche Silver : Données nettoyées et normalisées (3NF)';

-- Schéma Gold : Modèle analytique
CREATE SCHEMA IF NOT EXISTS accidents_gold;
COMMENT ON SCHEMA accidents_gold IS 'Couche Gold : Modèle en constellation pour analytics';

-- Vérification
SELECT 
    schema_name,
    schema_owner,
    'Créé avec succès' as statut
FROM information_schema.schemata 
WHERE schema_name LIKE 'accidents_%'
ORDER BY schema_name;