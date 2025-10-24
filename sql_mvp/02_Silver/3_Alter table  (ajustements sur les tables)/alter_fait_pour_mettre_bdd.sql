-- ALTER TABLE accidents ALTER COLUMN latitude TYPE FLOAT, ALTER COLUMN longitude TYPE FLOAT;

-- ALTER TABLE vehicules RENAME COLUMN nb_occupants_tc TO nb_occupants;

-- ALTER TABLE usagers ALTER COLUMN equipement_securite TYPE INTEGER USING equipement_securite::integer;

-- ALTER TABLE lieux DROP CONSTRAINT lieux_nombre_voies_check;

-- ALTER TABLE lieux ALTER COLUMN numero_route TYPE VARCHAR(60);


-- Si vous voulez autoriser la valeur 9
ALTER TABLE lieux DROP CONSTRAINT lieux_infrastructure_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_infrastructure_check CHECK (infrastructure BETWEEN 0 AND 9);


-- si vous voulez garder une contrainte mais autoriser -1
ALTER TABLE lieux DROP CONSTRAINT lieux_regime_circulation_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_regime_circulation_check CHECK (regime_circulation BETWEEN -1 AND 5 OR regime_circulation IS NULL);


-- Ajouter une nouvelle contrainte qui autorise la valeur 8
ALTER TABLE lieux DROP CONSTRAINT IF EXISTS lieux_situation_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_situation_check CHECK (situation BETWEEN 0 AND 8 OR situation IS NULL);


-- Voie réservée (valeur -1)
ALTER TABLE lieux DROP CONSTRAINT IF EXISTS lieux_voie_reservee_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_voie_reservee_check CHECK (voie_reservee BETWEEN -1 AND 3 OR voie_reservee IS NULL);


-- gèrer les valeurs longues jusqu'à 845 caractères
-- 1. Ajouter une nouvelle colonne TEXT
-- ALTER TABLE vehicules ADD COLUMN sens_circulation_text TEXT;

-- 2. Copier les données en les convertissant en texte
-- UPDATE vehicules 
-- SET sens_circulation_text = sens_circulation::text;

-- 3. Supprimer l'ancienne colonne
-- ALTER TABLE vehicules DROP COLUMN sens_circulation;

-- 4. Renommer la nouvelle colonne
-- ALTER TABLE vehicules RENAME COLUMN sens_circulation_text TO sens_circulation;

-- gère les 26 possibles
ALTER TABLE vehicules DROP CONSTRAINT IF EXISTS vehicules_manoeuvre_check;

ALTER TABLE vehicules  ADD CONSTRAINT vehicules_manoeuvre_check CHECK (manoeuvre BETWEEN 1 AND 26 OR manoeuvre IS NULL);


-- même pcp
ALTER TABLE lieux DROP CONSTRAINT IF EXISTS lieux_voie_reservee_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_voie_reservee_check CHECK (voie_reservee BETWEEN -1 AND 3 OR voie_reservee IS NULL);


-- même pcp
-- 1. Supprimer l'ancienne contrainte trop restrictive
ALTER TABLE lieux DROP CONSTRAINT IF EXISTS lieux_profil_route_check;

-- 2. Ajouter une nouvelle contrainte plus permissive
ALTER TABLE lieux 
ADD CONSTRAINT lieux_profil_route_check CHECK (profil_route BETWEEN -1 AND 4 OR profil_route IS NULL);

-- 1. TRACE_PLAN (manquant dans votre liste)
ALTER TABLE lieux DROP CONSTRAINT IF EXISTS lieux_trace_plan_check;
ALTER TABLE lieux ADD CONSTRAINT lieux_trace_plan_check CHECK (trace_plan BETWEEN -1 AND 4 OR trace_plan IS NULL);


-- même pcp
ALTER TABLE vehicules DROP CONSTRAINT IF EXISTS vehicules_point_choc_check;
ALTER TABLE vehicules ADD CONSTRAINT vehicules_point_choc_check 
CHECK (point_choc BETWEEN -1 AND 9 OR point_choc IS NULL);

