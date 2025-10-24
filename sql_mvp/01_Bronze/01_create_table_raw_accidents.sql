-- ========================================
-- COUCHE BRONZE : Table de staging
-- Grain: 1 ligne = 1 enregistrement CSV brut
-- Description: Stockage brut des données sans transformation
-- ========================================

DROP TABLE IF EXISTS accidents_bronze.raw_accidents CASCADE;

CREATE TABLE accidents_bronze.raw_accidents (
    -- ================================
    -- IDENTIFIANT TECHNIQUE
    -- ================================
    row_id BIGSERIAL PRIMARY KEY,
    
    -- ================================
    -- IDENTIFIANTS MÉTIER
    -- ================================
    num_acc VARCHAR(20) NOT NULL,
    num_veh VARCHAR(10),
    
    -- ================================
    -- CARACTERISTIQUES ACCIDENT
    -- ================================
    jour INTEGER,
    mois INTEGER,
    an INTEGER,
    hrmn VARCHAR(5),
    lum INTEGER,
    dep VARCHAR(3),
    com VARCHAR(5),
    agg INTEGER,
    intsect INTEGER,
    atm INTEGER,
    col INTEGER,
    adr TEXT,
    gps CHAR(1),
    lat DECIMAL(10, 8),
    long DECIMAL(11, 8),
    
    -- ================================
    -- LIEUX
    -- ================================
    catr INTEGER,
    voie VARCHAR(10),
    v1 VARCHAR(10),
    v2 VARCHAR(10),
    circ INTEGER,
    nbv INTEGER,
    vosp INTEGER,
    prof INTEGER,
    pr VARCHAR(10),
    pr1 INTEGER,
    trace_plan INTEGER,
    lartpc DECIMAL(5, 2),
    larrout DECIMAL(5, 2),
    surf INTEGER,
    infra INTEGER,
    situ INTEGER,
    env1 INTEGER,
    
    -- ================================
    -- VEHICULES
    -- ================================
    senc INTEGER,
    catv INTEGER,
    obs INTEGER,
    obsm INTEGER,
    choc INTEGER,
    manv INTEGER,
    occutc INTEGER,
    
    -- ================================
    -- USAGERS
    -- ================================
    place INTEGER,
    catu INTEGER,
    grav INTEGER,
    sexe INTEGER,
    an_nais INTEGER,
    trajet INTEGER,
    secu VARCHAR(2),
    locp INTEGER,
    actp INTEGER,
    etatp INTEGER,
    
    -- ================================
    -- COLONNES ENRICHIES (géographiques)
    -- ================================
    date DATE,
    year_georef INTEGER,
    com_name VARCHAR(100),
    dep_code VARCHAR(3),
    dep_name VARCHAR(100),
    epci_code VARCHAR(10),
    epci_name VARCHAR(100),
    reg_code VARCHAR(2),
    reg_name VARCHAR(100),
    com_arm_name VARCHAR(100),
    com_code VARCHAR(5),
    
    -- ================================
    -- MÉTADONNÉES D'IMPORT
    -- ================================
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

COMMENT ON TABLE accidents_bronze.raw_accidents IS 
'Table de staging : données brutes CSV sans transformation. Grain: 1 ligne CSV = 1 ligne table';

COMMENT ON COLUMN accidents_bronze.raw_accidents.row_id IS 
'Identifiant technique unique auto-incrémenté pour traçabilité';

COMMENT ON COLUMN accidents_bronze.raw_accidents.num_acc IS 
'Identifiant métier de l''accident (format BAAC)';

COMMENT ON COLUMN accidents_bronze.raw_accidents.load_timestamp IS 
'Horodatage de l''insertion en base pour audit';