import pandas as pd

def get_trace_plan_df():
    data = [
        {"code": 1, "libelle": "Partie rectiligne"},
        {"code": 2, "libelle": "En courbe à gauche"},
        {"code": 3, "libelle": "En courbe à droite"},
        {"code": 4, "libelle": "En « S »"},
    ]
    return pd.DataFrame(data)

def get_categorie_route_df():
    data = [
        {"code": 1, "libelle": "Autoroute"},
        {"code": 2, "libelle": "Route Nationale"},
        {"code": 3, "libelle": "Route Départementale"},
        {"code": 4, "libelle": "Voie Communale"},
        {"code": 5, "libelle": "Hors réseau public"},
        {"code": 6, "libelle": "Parc de stationnement ouvert à la circulation publique"},
        {"code": 9, "libelle": "Autre"},
    ]
    return pd.DataFrame(data)

def get_profil_route_df():
    data = [
        {"code": 1, "libelle": "Plat"},
        {"code": 2, "libelle": "Pente"},
        {"code": 3, "libelle": "Sommet de côte"},
        {"code": 4, "libelle": "Bas de côte"},
    ]
    return pd.DataFrame(data)

def get_etat_surface_df():
    data = [
        {"code": 1, "libelle": "Normale"},
        {"code": 2, "libelle": "Mouillée"},
        {"code": 3, "libelle": "Flaques"},
        {"code": 4, "libelle": "Inondée"},
        {"code": 5, "libelle": "Enneigée"},
        {"code": 6, "libelle": "Boue"},
        {"code": 7, "libelle": "Verglacée"},
        {"code": 8, "libelle": "Corps gras - huile"},
        {"code": 9, "libelle": "Autre"},
    ]
    return pd.DataFrame(data)

