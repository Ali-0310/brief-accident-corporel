-- ========================================
-- COUCHE GOLD : Index sur tables de faits
-- Objectif: Optimiser les requêtes analytiques métier
-- ========================================

-- ================================
-- INDEX TABLE FAIT_ACCIDENTS
-- ================================

-- Index sur dimensions (jointures fréquentes)
CREATE INDEX idx_fait_acc_date 
ON accidents_gold.fait_accidents(date_id);

CREATE INDEX idx_fait_acc_geo 
ON accidents_gold.fait_accidents(geo_id) 
WHERE geo_id IS NOT NULL;

CREATE INDEX idx_fait_acc_conditions 
ON accidents_gold.fait_accidents(condition_id) 
WHERE condition_id IS NOT NULL;

CREATE INDEX idx_fait_acc_route 
ON accidents_gold.fait_accidents(route_id) 
WHERE route_id IS NOT NULL;

-- Index composites pour requêtes multi-critères
-- Question : "Conditions avec risque supérieur"
CREATE INDEX idx_fait_acc_conditions_risque 
ON accidents_gold.fait_accidents(condition_id, route_id, est_accident_grave)
INCLUDE (score_gravite_total, nb_victimes_total, nb_tues_total);

COMMENT ON INDEX accidents_gold.idx_fait_acc_conditions_risque IS 
'Optimise : SELECT AVG(score) FROM fait WHERE condition AND route AND grave GROUP BY...';

-- Question : "Analyses temporelles et zones à risque"
CREATE INDEX idx_fait_acc_analyse_temporelle
ON accidents_gold.fait_accidents(date_id, geo_id)
INCLUDE (nb_tues_total, nb_victimes_total, score_gravite_total);

COMMENT ON INDEX accidents_gold.idx_fait_acc_analyse_temporelle IS 
'Optimise : Requêtes filtrées par date ET géographie';

-- Question : "Zones à risque"
CREATE INDEX idx_fait_acc_zones_risque
ON accidents_gold.fait_accidents(geo_id, est_accident_grave, est_accident_mortel);

COMMENT ON INDEX accidents_gold.idx_fait_acc_zones_risque IS 
'Optimise : COUNT(*) GROUP BY geo WHERE est_accident_grave';

-- Index sur flags booléens (bitmap)
CREATE INDEX idx_fait_acc_flags
ON accidents_gold.fait_accidents(est_nuit, est_weekend, est_accident_mortel, en_agglomeration)
WHERE est_accident_mortel = TRUE OR est_accident_grave = TRUE;

COMMENT ON INDEX accidents_gold.idx_fait_acc_flags IS 
'Index bitmap compressé pour combinaisons booléennes';

-- Index sur heure (analyses par période de la journée)
CREATE INDEX idx_fait_acc_heure
ON accidents_gold.fait_accidents(heure, est_weekend)
INCLUDE (nb_victimes_total);

-- ================================
-- INDEX TABLE FAIT_VEHICULES
-- ================================

-- Jointure avec accidents
CREATE INDEX idx_fait_veh_accident
ON accidents_gold.fait_vehicules(num_acc);

COMMENT ON INDEX accidents_gold.idx_fait_veh_accident IS 
'Jointure clé métier avec fait_accidents';

-- Jointure avec dimension véhicule
CREATE INDEX idx_fait_veh_type
ON accidents_gold.fait_vehicules(vehicule_type_id)
WHERE vehicule_type_id IS NOT NULL;

COMMENT ON INDEX accidents_gold.idx_fait_veh_type IS 
'Jointure avec dim_vehicule pour agrégations par type';

-- Analyses par type de véhicule et gravité
CREATE INDEX idx_fait_veh_analyse
ON accidents_gold.fait_vehicules(vehicule_type_id, est_vehicule_implique_mortel)
INCLUDE (nb_tues_vehicule, nb_occupants);

COMMENT ON INDEX accidents_gold.idx_fait_veh_analyse IS 
'Optimise : Statistiques par type de véhicule (ex: taux mortalité motos)';

-- Index sur manœuvre (analyses comportementales)
CREATE INDEX idx_fait_veh_manoeuvre
ON accidents_gold.fait_vehicules(manoeuvre)
WHERE manoeuvre IS NOT NULL;

-- Index sur obstacles
CREATE INDEX idx_fait_veh_obstacles
ON accidents_gold.fait_vehicules(obstacle_fixe, obstacle_mobile)
WHERE obstacle_fixe > 0 OR obstacle_mobile > 0;

-- ================================
-- INDEX TABLE FAIT_USAGERS
-- ================================

-- Jointure avec véhicule (FK)
CREATE INDEX idx_fait_usager_vehicule
ON accidents_gold.fait_usagers(vehicule_fk);

-- Jointure avec accident (dénormalisé)
CREATE INDEX idx_fait_usager_accident
ON accidents_gold.fait_usagers(accident_fk);

CREATE INDEX idx_fait_usager_num_acc
ON accidents_gold.fait_usagers(num_acc);

-- Index sur gravité (métrique clé)
CREATE INDEX idx_fait_usager_gravite
ON accidents_gold.fait_usagers(gravite);

COMMENT ON INDEX accidents_gold.idx_fait_usager_gravite IS 
'Optimise : Comptage tués/blessés/indemnes';

-- Index composite pour analyses démographiques
-- Question : "Profil des victimes par âge/sexe/catégorie"
CREATE INDEX idx_fait_usager_profil
ON accidents_gold.fait_usagers(categorie_usager, sexe, tranche_age, gravite)
INCLUDE (score_gravite_usager);

COMMENT ON INDEX accidents_gold.idx_fait_usager_profil IS 
'Optimise : Analyses victimes par profil (conducteurs masculins 18-25 ans tués, etc.)';

-- Index sur âge
CREATE INDEX idx_fait_usager_age
ON accidents_gold.fait_usagers(age)
WHERE age IS NOT NULL;

-- Index sur flags générés
CREATE INDEX idx_fait_usager_flags
ON accidents_gold.fait_usagers(est_conducteur, est_pieton, est_victime, est_tue);

-- Index spécifique piétons
CREATE INDEX idx_fait_usager_pietons
ON accidents_gold.fait_usagers(localisation_pieton, action_pieton)
WHERE est_pieton = TRUE;

COMMENT ON INDEX accidents_gold.idx_fait_usager_pietons IS 
'Optimise : Analyses spécifiques piétons (ex: traversant hors passage)';

-- Index équipement de sécurité
CREATE INDEX idx_fait_usager_equipement
ON accidents_gold.fait_usagers(equipement_utilise, gravite)
WHERE equipement_utilise IS NOT NULL;

COMMENT ON INDEX accidents_gold.idx_fait_usager_equipement IS 
'Optimise : Efficacité équipements sécurité (ceinture, casque)';

-- ================================
-- STATISTIQUES
-- ================================

ANALYZE accidents_gold.fait_accidents;
ANALYZE accidents_gold.fait_vehicules;
ANALYZE accidents_gold.fait_usagers;

-- ================================
-- RAPPORT FINAL
-- ================================

SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as taille_index
FROM pg_indexes 
WHERE schemaname = 'accidents_gold'
  AND tablename LIKE 'fait_%'
ORDER BY tablename, indexname;

-- Résumé par table
SELECT 
    schemaname,
    tablename,
    COUNT(*) as nb_index,
    pg_size_pretty(SUM(pg_relation_size(indexname::regclass))) as taille_totale_index
FROM pg_indexes 
WHERE schemaname = 'accidents_gold'
  AND tablename LIKE 'fait_%'
GROUP BY schemaname, tablename
ORDER BY tablename;