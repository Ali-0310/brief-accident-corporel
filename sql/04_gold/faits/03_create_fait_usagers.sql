-- ========================================
-- TABLE DE FAITS : Usagers
-- Grain: 1 ligne = 1 usager (victime ou indemne)
-- Description: Personnes impliquées avec gravité des blessures
-- ========================================

DROP TABLE IF EXISTS accidents_gold.fait_usagers CASCADE;

CREATE TABLE accidents_gold.fait_usagers (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    usager_id BIGSERIAL PRIMARY KEY,
    
    -- ================================
    -- CLÉS ÉTRANGÈRES
    -- ================================
    vehicule_fk BIGINT NOT NULL REFERENCES accidents_gold.fait_vehicules(vehicule_id) ON DELETE CASCADE,
    accident_fk BIGINT NOT NULL,  -- Dénormalisé pour performance (évite double JOIN)
    num_acc VARCHAR(20) NOT NULL,  -- Clé métier
    
    -- ================================
    -- POSITION ET CATÉGORIE
    -- ================================
    place_vehicule INTEGER,
    -- Position dans le véhicule (variable selon type véhicule)
    
    categorie_usager INTEGER NOT NULL CHECK (categorie_usager BETWEEN 1 AND 4),
    -- 1=Conducteur, 2=Passager, 3=Piéton, 4=Piéton roller/trottinette
    
    -- ================================
    -- GRAVITÉ (Métrique clé)
    -- ================================
    gravite INTEGER NOT NULL CHECK (gravite BETWEEN 1 AND 4),
    -- 1=Indemne, 2=Tué, 3=Hospitalisé >24h, 4=Blessé léger
    
    -- ================================
    -- PROFIL DÉMOGRAPHIQUE
    -- ================================
    sexe INTEGER CHECK (sexe IN (1, 2, NULL)),
    -- 1=Masculin, 2=Féminin
    
    age INTEGER CHECK (age BETWEEN 0 AND 120),
    -- Âge au moment de l'accident
    
    tranche_age VARCHAR(10),
    -- '0-14', '15-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+'
    
    -- ================================
    -- CONTEXTE
    -- ================================
    motif_deplacement INTEGER CHECK (motif_deplacement BETWEEN 0 AND 9),
    -- 1=Domicile-travail, 2=Domicile-école, 3=Courses, 4=Pro, 5=Loisirs, 9=Autre
    
    equipement_securite VARCHAR(2),
    -- Format XY : X=type (1=Ceinture, 2=Casque, 3=Enfant, 4=Réfléchissant)
    --             Y=utilisation (1=Oui, 2=Non, 3=Indéterminé)
    
    equipement_present BOOLEAN,
    -- Décodage du 1er caractère
    
    equipement_utilise BOOLEAN,
    -- Décodage du 2e caractère
    
    -- ================================
    -- SPÉCIFIQUE PIÉTONS
    -- ================================
    localisation_pieton INTEGER CHECK (localisation_pieton BETWEEN 0 AND 8),
    -- 1-2=Sur chaussée, 3-4=Passage piéton, 5=Trottoir, 6=Accotement, etc.
    
    action_pieton INTEGER CHECK (action_pieton BETWEEN 0 AND 9),
    -- 0=Non renseigné, 1-2=Se déplaçant, 3=Traversant, 4=Masqué, 5=Jouant, 6=Avec animal, 9=Autre
    
    etat_pieton INTEGER CHECK (etat_pieton BETWEEN 0 AND 3),
    -- 1=Seul, 2=Accompagné, 3=En groupe
    
    -- ================================
    -- SCORE INDIVIDUEL
    -- ================================
    score_gravite_usager INTEGER DEFAULT 0 CHECK (score_gravite_usager >= 0),
    -- Pondération : tué=100, hospitalisé=10, blessé léger=1, indemne=0
    
    -- ================================
    -- FLAGS ANALYTIQUES
    -- ================================
    est_conducteur BOOLEAN GENERATED ALWAYS AS (categorie_usager = 1) STORED,
    est_pieton BOOLEAN GENERATED ALWAYS AS (categorie_usager IN (3, 4)) STORED,
    est_victime BOOLEAN GENERATED ALWAYS AS (gravite > 1) STORED,
    est_tue BOOLEAN GENERATED ALWAYS AS (gravite = 2) STORED,
    est_blesse_grave BOOLEAN GENERATED ALWAYS AS (gravite IN (2, 3)) STORED,
    
    -- ================================
    -- MÉTADONNÉES
    -- ================================
    date_chargement TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE accidents_gold.fait_usagers IS 
'Table de faits usagers : personnes impliquées dans les accidents. Grain: 1 usager (victime ou indemne)';

COMMENT ON COLUMN accidents_gold.fait_usagers.vehicule_fk IS 
'FK vers fait_vehicules : véhicule dans lequel se trouvait l''usager (ou véhicule heurtant si piéton)';

COMMENT ON COLUMN accidents_gold.fait_usagers.accident_fk IS 
'Dénormalisation pour éviter double JOIN (vehicule → accident)';

COMMENT ON COLUMN accidents_gold.fait_usagers.gravite IS 
'1=Indemne, 2=Tué (sur coup ou <30j), 3=Hospitalisé >24h, 4=Blessé léger';

COMMENT ON COLUMN accidents_gold.fait_usagers.tranche_age IS 
'Segmentation normalisée pour analyses démographiques';

COMMENT ON COLUMN accidents_gold.fait_usagers.equipement_securite IS 
'Code BAAC 2 caractères : ex "11"=Ceinture utilisée, "22"=Casque non utilisé';

COMMENT ON COLUMN accidents_gold.fait_usagers.score_gravite_usager IS 
'Métrique pondérée pour comparaisons : tué=100, hosp=10, blessé=1, indemne=0';