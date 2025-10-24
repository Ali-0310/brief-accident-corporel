-- ========================================
-- CRÉATION SCHÉMAS SIMPLIFIÉS
-- ========================================

CREATE SCHEMA accidents_bronze;
CREATE SCHEMA accidents_silver;
CREATE SCHEMA accidents_gold;

COMMENT ON SCHEMA accidents_bronze IS 'Données brutes CSV';
COMMENT ON SCHEMA accidents_silver IS 'Données nettoyées (3NF)';
COMMENT ON SCHEMA accidents_gold IS 'Modèle analytique simplifié';