-- ========================================
-- BRONZE : Table unique (staging)
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
    num_acc TEXT,
    num_veh TEXT,

    -- ================================
    -- CARACTERISTIQUES ACCIDENT
    -- ======================   ==========    
    jour TEXT,
    mois TEXT,
    an TEXT,
    hrmn TEXT,
    lum TEXT,
    dep TEXT,
    com TEXT,
    agg TEXT,
    intsect TEXT,
    atm TEXT,
    col TEXT,
    adr TEXT,
    gps TEXT,
    lat TEXT,
    long TEXT,

    -- ================================
    -- LIEUX
    -- ================================    
    catr TEXT,
    voie TEXT,
    v1 TEXT,
    v2 TEXT,
    circ TEXT,
    nbv TEXT,
    vosp TEXT,
    prof TEXT,
    pr TEXT,
    pr1 TEXT,
    trace_plan TEXT,
    lartpc TEXT,
    larrout TEXT,
    surf TEXT,
    infra TEXT,
    situ TEXT,
    env1 TEXT,

    -- ================================
    -- VEHICULES
    -- ================================
    senc TEXT,
    catv TEXT,
    obs TEXT,
    obsm TEXT,
    choc TEXT,
    manv TEXT,
    occutc TEXT,

    -- ================================
    -- USAGERS
    -- ================================
    place TEXT,
    catu TEXT,
    grav TEXT,
    sexe TEXT,
    an_nais TEXT,
    trajet TEXT,
    secu TEXT,
    locp TEXT,
    actp TEXT,
    etatp TEXT,
    
    -- ================================
    -- COLONNES ENRICHIES (géographiques)
    -- ================================
    date TEXT,
    year_georef TEXT,
    com_name TEXT,
    dep_code TEXT,
    dep_name TEXT,
    epci_code TEXT,
    epci_name TEXT,
    reg_code TEXT,
    reg_name TEXT,
    com_arm_name TEXT,
    com_code TEXT,
    code_postal TEXT,
    insee TEXT,
    num TEXT,
    coordonnees TEXT,
    plan TEXT,
    nom_com TEXT,
    secu_utl TEXT,

    -- ================================
    -- MÉTADONNÉES D'IMPORT
    -- ================================
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index minimal
CREATE INDEX idx_bronze_num_acc ON accidents_bronze.raw_accidents(num_acc);