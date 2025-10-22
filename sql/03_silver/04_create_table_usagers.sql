-- ========================================
-- COUCHE SILVER : Table usagers
-- Grain: 1 ligne = 1 usager (victime ou indemne)
-- Description: Personnes impliquées dans les accidents
-- ========================================

DROP TABLE IF EXISTS accidents_silver.usagers CASCADE;

CREATE TABLE accidents_silver.usagers (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    id_usager BIGSERIAL PRIMARY KEY,
    
    -- ================================
    -- CLES ETRANGERES
    -- ================================
    num_acc VARCHAR(20),
    num_veh VARCHAR(10),
    
    -- ================================
    -- POSITION USAGER
    -- ================================
    place_vehicule INTEGER,  -- Position dans le véhicule (conducteur, passager avant/arrière, etc.)
    categorie_usager INTEGER CHECK (categorie_usager BETWEEN 1 AND 4),  -- 1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton roller/trottinette
    
    -- ================================
    -- GRAVITE ET PROFIL
    -- ================================
    gravite INTEGER NOT NULL CHECK (gravite BETWEEN 1 AND 4),  -- 1=Indemne, 2=Tué, 3=Hospitalisé, 4=Blessé léger
    sexe INTEGER CHECK (sexe IN (1, 2)),  -- 1=Masculin, 2=Féminin
    annee_naissance INTEGER CHECK (annee_naissance BETWEEN 1900 AND 2025),
    age_au_moment_accident INTEGER CHECK (age_au_moment_accident BETWEEN 0 AND 120),
    
    -- ================================
    -- CONTEXTE DEPLACEMENT
    -- ================================
    motif_deplacement INTEGER,  -- 1-9 : Domicile-travail, école, courses, loisirs, etc.
    equipement_securite VARCHAR(2),  -- 2 caractères : 1er=type équipement, 2e=utilisation
    
    -- ================================
    -- SPECIFIQUE PIETONS
    -- ================================
    localisation_pieton INTEGER,  -- 1-8 : Sur chaussée, passage piéton, trottoir, etc.
    action_pieton INTEGER,  -- 0-9 : Traversant, masqué, jouant, etc.
    etat_pieton INTEGER,  -- 1-3 : Seul, accompagné, en groupe
    
    -- ================================
    -- CONTRAINTES
    -- ================================
    FOREIGN KEY (num_acc, num_veh) 
        REFERENCES accidents_silver.vehicules(num_acc, num_veh) 
        ON DELETE CASCADE
);

COMMENT ON TABLE accidents_silver.usagers IS 
'Usagers (victimes et indemnes) impliqués dans les accidents (grain: 1 usager)';

COMMENT ON COLUMN accidents_silver.usagers.gravite IS 
'1=Indemne, 2=Tué (sur le coup ou dans les 30 jours), 3=Hospitalisé >24h, 4=Blessé léger';

COMMENT ON COLUMN accidents_silver.usagers.categorie_usager IS 
'1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton en roller/trottinette';

COMMENT ON COLUMN accidents_silver.usagers.equipement_securite IS 
'Format: XY où X=type (1=Ceinture, 2=Casque, 3=Dispositif enfant, 4=Réfléchissant, 9=Autre) et Y=utilisation (1=Oui, 2=Non, 3=Indéterminé)';