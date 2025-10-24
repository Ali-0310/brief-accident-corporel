-- ========================================
-- DIMENSION : Route
-- Grain: 1 ligne = 1 combinaison caractéristiques route
-- Description: Typologie et état de la voie
-- ========================================

DROP TABLE IF EXISTS accidents_gold.dim_route CASCADE;

CREATE TABLE accidents_gold.dim_route (
    -- ================================
    -- IDENTIFIANT
    -- ================================
    route_id SERIAL PRIMARY KEY,
    
    -- ================================
    -- CATÉGORIE DE ROUTE
    -- ================================
    categorie_route_code INTEGER CHECK (categorie_route_code BETWEEN 1 AND 9),
    categorie_route_libelle VARCHAR(120),
    
    -- ================================
    -- PROFIL
    -- ================================
    profil_route_code INTEGER CHECK (profil_route_code BETWEEN 1 AND 4),
    profil_route_libelle VARCHAR(120),
    
    -- ================================
    -- TRACÉ
    -- ================================
    trace_plan_code INTEGER CHECK (trace_plan_code BETWEEN 1 AND 4),
    trace_plan_libelle VARCHAR(120),
    
    -- ================================
    -- ÉTAT SURFACE
    -- ================================
    etat_surface_code INTEGER CHECK (etat_surface_code BETWEEN 1 AND 9),
    etat_surface_libelle VARCHAR(120),
    
    -- ================================
    -- SCORE DE RISQUE COMPOSITE
    -- ================================
    niveau_risque_route INTEGER CHECK (niveau_risque_route BETWEEN 1 AND 5),
    
    -- ================================
    -- CONTRAINTE UNICITÉ
    -- ================================
    UNIQUE (categorie_route_code, profil_route_code, trace_plan_code, etat_surface_code)
);

COMMENT ON TABLE accidents_gold.dim_route IS 
'Dimension route : combinaison catégorie × profil × tracé × surface. Grain: 1 combinaison';

COMMENT ON COLUMN accidents_gold.dim_route.categorie_route_libelle IS 
'Autoroute, Route Nationale, Départementale, Communale, Hors réseau, Parking, Autre';

COMMENT ON COLUMN accidents_gold.dim_route.profil_route_libelle IS 
'Plat, Pente, Sommet de côte, Bas de côte';

COMMENT ON COLUMN accidents_gold.dim_route.trace_plan_libelle IS 
'Rectiligne, Courbe à gauche, Courbe à droite, En S';

COMMENT ON COLUMN accidents_gold.dim_route.etat_surface_libelle IS 
'Normal, Mouillé, Flaques, Inondé, Enneigé, Boue, Verglacé, Corps gras, Autre';

-- Index
CREATE INDEX idx_dim_route_categorie ON accidents_gold.dim_route(categorie_route_code);
CREATE INDEX idx_dim_route_surface ON accidents_gold.dim_route(etat_surface_code);
CREATE INDEX idx_dim_route_risque ON accidents_gold.dim_route(niveau_risque_route);
