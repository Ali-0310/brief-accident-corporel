"""
═══════════════════════════════════════════════════════════════════
ETL COUCHE SILVER 
═══════════════════════════════════════════════════════════════════
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import time
import sys
import logging
from dotenv import load_dotenv

load_dotenv()


# Désactiver logs
logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.pool').setLevel(logging.CRITICAL)


# ════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),                   
    'port': int(os.getenv('DB_PORT')),            
    'database': os.getenv('DB_NAME'),              
    'user': os.getenv('DB_USER'),                  
    'password': os.getenv('DB_PASSWORD')           
}


CONFIG = {
    'fichier_source': 'E:\\SIMPLON\\TD\\TD_4 Analyse sécu routière\\5_Gestion Parquet\\accidents-corporels-de-la-circulation-millesime.parquet',
    'chunk_size': 50000  # Traiter par batch de 50K
}

# ════════════════════════════════════════════════════════════════
# MAPPINGS 
# ════════════════════════════════════════════════════════════════

GRAVITE_MAPPING = {'Indemne': 1, 'Tué': 2, 'Blessé hospitalisé': 3, 'Blessé': 4}
LUMINOSITE_MAPPING = {'Plein jour': 1, 'Crépuscule ou aube': 2, 'Nuit sans éclairage public': 3, 
                      'Nuit avec éclairage public non allumé': 4, 'Nuit avec éclairage public allumé': 5}
CONDITIONS_ATMO_MAPPING = {'Normale': 1, 'Pluie légère': 2, 'Pluie forte': 3, 'Neige - grêle': 4, 
                           'Brouillard - fumée': 5, 'Vent fort - tempête': 6, 'Temps éblouissant': 7, 
                           'Temps couvert': 8, 'Autre': 9}
AGG_MAPPING = {'Hors agglomération': False, 'En agglomération': True}
INT_MAPPING = {'Hors intersection': 1, 'Intersection en X': 2, 'Intersection en T': 3, 
               'Intersection en Y': 4, 'Intersection à plus de 4 branches': 5, 'Giratoire': 6, 
               'Place': 7, 'Passage à niveau': 8, 'Autre intersection': 9}
COL_MAPPING = {'Deux véhicules - frontale': 1, 'Deux véhicules - par l\'arrière': 2, 
               'Deux véhicules - par le coté': 3, 'Trois véhicules et plus - en chaîne': 4, 
               'Trois véhicules et plus - collisions multiples': 5, 'Autre collision': 6, 'Sans collision': 7}
CATEGORIE_ROUTE_MAPPING = {'Autoroute': 1, 'Route Nationale': 2, 'Route Départementale': 3, 
                           'Voie Communale': 4, 'Hors réseau public': 5, 
                           'Parc de stationnement ouvert à la circulation publique': 6, 'autre': 9}
SURF_MAPPING = {'normale': 1, 'mouillée': 2, 'flaques': 3, 'inondée': 4, 'enneigée': 5, 
                'boue': 6, 'verglacée': 7, 'corps gras - huile': 8, 'autre': 9}
CATEGORIE_USAGER_MAPPING = {'Conducteur': 1, 'Passager': 2, 'Piéton': 3, 'Piéton en roller ou en trottinette': 4}
SEXE_MAPPING = {'Masculin': 1, 'Féminin': 2}
SITU_MAPPING = {'Sur chaussée': 1, 'Sur accotement': 2, 'Sur trottoir': 3, 'Sur bande d\'arrêt d\'urgence': 4, 
                'Sur piste cyclable': 5, '8': 8, '6': 6, '-1': 0, 'Autre': 9}
INFRA_MAPPING = {'Carrefour aménagé': 1, 'Pont - autopont': 2, 'Bretelle d\'échangeur ou de raccordement': 3, 
                 'Zone piétonne': 4, 'Souterrain - tunnel': 5, 'Voie ferrée': 6, 'Zone de péage': 7, 
                 '9': 9, '8': 8, '-1': 0, '0': 0, 'Aucune': 0, 'Autre': 9}
OBSTACLE_MOBILE_MAPPING = {'Véhicule': 1, 'Piéton': 2, 'Autre': 3, 'Animal sauvage': 4, 
                           'Animal domestique': 5, 'Véhicule sur rail': 6, '-1': 0, '0': 0, 'Aucun': 0}
CATEGORIE_VEHICULE_MAPPING = {
    'VL seul': 1,
    'VL + caravane': 2,
    'VL + remorque': 3,
    'VU seul': 4,
    'VU + caravane': 5,
    'VU + remorque': 6,
    'PL seul': 7,
    'PL + remorque': 8,
    'PL train': 9,
    'PL double': 10,
    'Cyclo 50cm3': 11,
    'Cyclo 125cm3': 12,
    'Moto > 125cm3': 13,
    'Scooter < 50cm3': 14,
    'Scooter > 50cm3': 15,
    'Quad': 16,
    'Autocar': 17,
    'Autobus': 18,
    'Train': 19,
    'Tramway': 20,
    'Engin agricole': 21,
    'Tracteur routier': 22,
    'Autre': 99
}
EQUIPEMENT_SECURITE_MAPPING = {
    'Ceinture': 1,
    'Casque': 2,
    'Gilet réfléchissant': 3,
    'Airbag': 4,
    'Gants': 5,
    'Gants + Casque': 6,
    'Ceinture + Airbag': 7,
    'Autre équipement': 99,
    'Aucun équipement': 0
}

VOSP_MAPPING = {
    'Piste cyclable': 1,
    'Banque cyclable': 2, 
    'Voie réservée': 3,
    '-1': -1,
    '': None,
    'nan': None
}

MANOEUVRE_MAPPING = {
    'manv d’évitement': 1, 'Sans changement de direction': 2, 'manv de stationnement': 3,
    'Tournant A gauche': 4, 'Déporté A gauche': 5, 'Même senc': 6, 'Arrêté (hors stationnement)': 7,
    'Dépassant A gauche': 8, 'En s\'insérant': 9, 'Traversant la chaussée': 10,
    'Tournant A droite': 11, 'Déporté A droite': 12, 'Dépassant A droite': 13,
    'Changeant de file A gauche': 14, 'Changeant de file A droite': 15,
    'En faisant demi-tour sur la chaussée': 16, 'En stationnement (avec occupants)': 17,
    'En marche arrière': 18, 'Ouverture de porte': 19, 'Entre 2 files': 20,
    'Dans le couloir bus': 21, 'En franchissant le terre-plein central': 22,
    'A contresenc': 23, '26': 26
}

OBSTACLE_FIXE_MAPPING = {
    'Véhicule en stationnement': 1, 'Arbre': 2, 'Glissière métallique': 3,
    'Glissière béton': 4, 'Autre glissière': 5, 'Mur': 6, 'Poteau': 7,
    'Mobilier urbain': 8, 'Parapet': 9, 'Support de signalisation': 10,
    'Gril': 11, 'Fossé': 12, 'Talus': 13, 'Autre obstacle fixe': 99, 'Aucun': 0
}
PROFIL_ROUTE_MAPPING = {
    'Plat': 1,
    'Pente': 2,
    'Sommet de côte': 3,
    'Bas de côte': 4,
    '-1': None,
    '': None,
    'nan': None
}

PROFIL_ROUTE_MAPPING = {
    'Plat': 1,
    'Pente': 2, 
    'Sommet de côte': 3,
    'Bas de côte': 4,
    '-1': None,
    '': None,
    'nan': None
}

TRACE_PLAN_MAPPING = {
    'Partie rectiligne': 1,
    'En courbe à gauche': 2,
    'En courbe à droite': 3, 
    'En « S »': 4,
    '-1': None,
    '': None,
    'nan': None
}

REGIME_CIRCULATION_MAPPING = {
    'Sens unique': 1,
    'Bidirectionnelle': 2,
    'À chaussées séparées': 3,
    'Avec voies d\'affectation variable': 4, 
    '-1': None,
    '': None,
    'nan': None
}

POINT_CHOC_MAPPING = {
    'Avant': 1,
    'Arrière': 2, 
    'Côté gauche': 3,
    'Côté droit': 4,
    '-1': None,
    '': None,
    'nan': None
}
COLS_USAGERS_MULTI = ['an_nais', 'sexe', 'actp', 'grav', 'secu', 'secu_utl', 'locp', 'place', 'catu', 'etatp', 'trajet']
COLS_VEHICULES_MULTI = ['num_veh', 'choc', 'manv', 'senc', 'obsm', 'obs', 'catv', 'occutc']

# ════════════════════════════════════════════════════════════════
# ⚡ FONCTION EXPLOSION VECTORISÉE 
# ════════════════════════════════════════════════════════════════

def explode_multivalue_vectorized(df, multi_cols, id_col='num_acc'):

    start = time.time()
    
    # Étape 1 : Compter le nombre de valeurs (VECTORISÉ)
    def count_vectorized(series):
        return series.fillna('').astype(str).str.count(',') + 1
    
    # Prendre la première colonne non vide pour compter
    nb_values = None
    for col in multi_cols:
        if col in df.columns:
            nb_values = count_vectorized(df[col])
            nb_values = nb_values.where(df[col].notna() & (df[col].astype(str) != ''), 1)
            break
    
    if nb_values is None:
        return df
    
    # Étape 2 : Répéter les lignes (VECTORISÉ)
    df_repeated = df.loc[df.index.repeat(nb_values)].copy()
    df_repeated['_position'] = df_repeated.groupby(id_col).cumcount()
    
    # Étape 3 : Extraire valeurs par position (VECTORISÉ)
    for col in multi_cols:
        if col not in df_repeated.columns:
            continue
        
        # Créer une série avec splits
        splits = df_repeated[col].fillna('').astype(str).str.split(',')
        
        # Extraire par position (VECTORISÉ avec list comprehension)
        def extract_at_pos(split_list, pos):
            try:
                val = split_list[pos].strip()
                return val if val != '' else None
            except (IndexError, AttributeError):
                return None
        
        # Application vectorisée 
        df_repeated[col] = [
            extract_at_pos(split_list, pos) 
            for split_list, pos in zip(splits, df_repeated['_position'])
        ]
    
    df_repeated = df_repeated.drop(columns=['_position'])
    
    print(f"    ✓ Explosion terminée en {time.time()-start:.1f}s ({len(df_repeated):,} lignes)")
    return df_repeated


# ════════════════════════════════════════════════════════════════
# ETL PRINCIPAL 
# ════════════════════════════════════════════════════════════════

def etl_silver():
    start_global = time.time()
    
    print("═"*70)
    print("ETL COUCHE SILVER ")
    print("═"*70)
    
    # ═══════════════════════════════════════════════════════════
    # ÉTAPE 1 : CHARGEMENT PARQUET 
    # ═══════════════════════════════════════════════════════════
    
    print("\n[1/5] CHARGEMENT PARQUET")
    print("-"*70)
    start = time.time()
    
    # ⚡ OPTIMISATION : Charger uniquement les colonnes nécessaires
    colonnes_necessaires = [
        'num_acc', 'an', 'mois', 'jour', 'hrmn',
        'lum', 'agg', 'int', 'atm', 'col',
        'com', 'dep', 'lat', 'long', 'adr',
        'catr', 'voie', 'circ', 'nbv', 'prof', 'plan',
        'lartpc', 'larrout', 'surf', 'infra', 'situ', 'env1','vosp'
    ] + COLS_USAGERS_MULTI + COLS_VEHICULES_MULTI
    
    df_bronze = pd.read_parquet(
        CONFIG['fichier_source'],
        engine='pyarrow',
        columns=colonnes_necessaires  # ⚡ Charger uniquement colonnes utiles
    )
    
    print(f"✓ {len(df_bronze):,} accidents chargés en {time.time()-start:.1f}s")
    print(f"  Mémoire : {df_bronze.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # ═══════════════════════════════════════════════════════════
    # ÉTAPE 2 : TABLE ACCIDENTS (VECTORISÉ)
    # ═══════════════════════════════════════════════════════════
    
    print("\n[2/5] TRANSFORMATION → TABLE ACCIDENTS")
    print("-"*70)
    start = time.time()
    
    df_accidents = df_bronze[[
        'num_acc', 'an', 'mois', 'jour', 'hrmn',
        'lum', 'agg', 'int', 'atm', 'col',
        'com', 'dep', 'lat', 'long', 'adr'
    ]].drop_duplicates(subset=['num_acc']).copy()
    
    print(f"  ✓ {len(df_accidents):,} accidents uniques")
    
    # Temporel (VECTORISÉ)
    df_accidents[['heure', 'minute']] = df_accidents['hrmn'].str.split(':', expand=True).astype('Int64')
    df_accidents['annee'] = pd.to_numeric(df_accidents['an'], errors='coerce').astype('Int64')
    df_accidents['mois'] = pd.to_numeric(df_accidents['mois'], errors='coerce').astype('Int64')
    df_accidents['jour'] = pd.to_numeric(df_accidents['jour'], errors='coerce').astype('Int64')
    
    # Jour semaine (VECTORISÉ)
    df_accidents['date_temp'] = pd.to_datetime(
        df_accidents[['an', 'mois', 'jour']].rename(columns={'an': 'year', 'mois': 'month', 'jour': 'day'}),
        errors='coerce'
    )
    df_accidents['jour_semaine'] = df_accidents['date_temp'].dt.dayofweek + 1
    
    # Géographie (VECTORISÉ)
    df_accidents['com_code'] = df_accidents['com'].astype(str).str.zfill(5)
    df_accidents['departement_code'] = df_accidents['dep'].astype(str).str.strip()
        
    # =============================================================================
    # CONVERSION GPS 
    # =============================================================================

    def convert_lat_long(coord):
        """
        Gère les deux formats : 7 chiffres sans séparateur ET format décimal normal
        """
        if pd.isna(coord):
            return None
        
        coord_str = str(coord).strip()
        
        # Gestion des valeurs vides
        if coord_str == '' or coord_str.lower() == 'nan':
            return None
        
        # Remplacement virgule par point
        coord_str = coord_str.replace(',', '.')
        
        # Cas format décimal standard
        if '.' in coord_str:
            try:
                return float(coord_str)
            except ValueError:
                return None
        
        # Format 7 chiffres sans séparateur
        elif len(coord_str) == 7 and coord_str.replace('-', '').isdigit():
            try:
                # Latitude positive : 4872760 → 48.72760
                if not coord_str.startswith('-'):
                    return float(coord_str[:2] + '.' + coord_str[2:])
                # Longitude négative : -2478760 → -2.478760
                else:
                    return float('-' + coord_str[1:2] + '.' + coord_str[2:])
            except ValueError:
                return None
        
        # Format 8 chiffres sans séparateur (cas longitude positive)
        elif len(coord_str) == 8 and coord_str.replace('-', '').isdigit():
            try:
                if not coord_str.startswith('-'):
                    return float(coord_str[:1] + '.' + coord_str[1:])
                else:
                    return float('-' + coord_str[1:2] + '.' + coord_str[2:])
            except ValueError:
                return None
        
        # Autres formats non gérés, on tente la conversion brute
        else:
            try:
                return float(coord_str)
            except ValueError:
                return None

    # Application sur le DataFrame
    print("  [GPS] Conversion des coordonnées...")
    df_accidents['latitude'] = df_accidents['lat'].apply(convert_lat_long)
    df_accidents['longitude'] = df_accidents['long'].apply(convert_lat_long)

    # Vérification qualité
    lat_valides = df_accidents['latitude'].notna().sum()
    long_valides = df_accidents['longitude'].notna().sum()
    total = len(df_accidents)
    print(f"    ✓ Latitude valide : {lat_valides}/{total} ({lat_valides/total*100:.1f}%)")
    print(f"    ✓ Longitude valide : {long_valides}/{total} ({long_valides/total*100:.1f}%)")
    # Adresses (VECTORISÉ)
    
    df_accidents['adresse'] = df_accidents['adr'].astype(str).replace('nan', None)
    
    # Conditions (VECTORISÉ avec map)
    df_accidents['en_agglomeration'] = df_accidents['agg'].map(AGG_MAPPING)
    df_accidents['luminosite'] = df_accidents['lum'].map(LUMINOSITE_MAPPING)
    df_accidents['conditions_atmospheriques'] = df_accidents['atm'].map(CONDITIONS_ATMO_MAPPING)
    df_accidents['type_intersection'] = df_accidents['int'].map(INT_MAPPING)
    df_accidents['type_collision'] = df_accidents['col'].map(COL_MAPPING)
    
    # Sélection finale
    df_accidents_silver = df_accidents[[
        'num_acc', 'heure', 'minute', 'annee', 'mois', 'jour', 'jour_semaine',
        'com_code', 'departement_code', 'en_agglomeration',
        'latitude', 'longitude', 'adresse',
        'luminosite', 'conditions_atmospheriques', 'type_intersection', 'type_collision'
    ]].copy()
    
    print(f"✓ Table accidents : {len(df_accidents_silver):,} lignes en {time.time()-start:.1f}s")
    
    # ═══════════════════════════════════════════════════════════
    # ÉTAPE 3 : TABLE LIEUX (VECTORISÉ)
    # ═══════════════════════════════════════════════════════════
    
    print("\n[3/5] TRANSFORMATION → TABLE LIEUX")
    print("-"*70)
    start = time.time()
    
    df_lieux = df_bronze[[
        'num_acc', 'catr', 'voie', 'circ', 'nbv', 'prof', 'plan',
        'lartpc', 'larrout', 'surf', 'infra', 'situ', 'env1', 'vosp'
    ]].drop_duplicates(subset=['num_acc']).copy()
    
    # Transformations (VECTORISÉ)
    df_lieux['categorie_route'] = df_lieux['catr'].map(CATEGORIE_ROUTE_MAPPING)
    df_lieux['numero_route'] = df_lieux['voie'].astype(str).replace('nan', None)
    df_lieux['regime_circulation'] = pd.to_numeric(df_lieux['circ'], errors='coerce').astype('Int64')
    df_lieux['nombre_voies'] = pd.to_numeric(df_lieux['nbv'], errors='coerce').astype('Int64')
    df_lieux['voie_reservee'] = df_lieux['vosp'].map(VOSP_MAPPING)
    df_lieux['profil_route'] = df_lieux['prof'].map(PROFIL_ROUTE_MAPPING)
    df_lieux['trace_plan'] = df_lieux['plan'].map(TRACE_PLAN_MAPPING)
    df_lieux['largeur_terre_plein'] = pd.to_numeric(df_lieux['lartpc'], errors='coerce') / 100
    df_lieux['largeur_chaussee'] = pd.to_numeric(df_lieux['larrout'], errors='coerce') / 100
    df_lieux['etat_surface'] = df_lieux['surf'].str.lower().map(SURF_MAPPING)
    df_lieux['infrastructure'] = df_lieux['infra'].map(INFRA_MAPPING).fillna(0).astype('Int64')
    df_lieux['situation'] = df_lieux['situ'].map(SITU_MAPPING).fillna(0).astype('Int64')
    df_lieux['proximite_ecole'] = pd.to_numeric(df_lieux['env1'], errors='coerce').notna()
    
    df_lieux_silver = df_lieux[[
        'num_acc', 'categorie_route', 'numero_route', 'regime_circulation',
        'nombre_voies', 'voie_reservee', 'profil_route', 'trace_plan',
        'largeur_terre_plein', 'largeur_chaussee', 'etat_surface',
        'infrastructure', 'situation', 'proximite_ecole'
    ]].copy()
    
    print(f"✓ Table lieux : {len(df_lieux_silver):,} lignes en {time.time()-start:.1f}s")
    
    # ═══════════════════════════════════════════════════════════
    # ÉTAPE 4 : TABLE VEHICULES (EXPLOSION VECTORISÉE)
    # ═══════════════════════════════════════════════════════════
    
    print("\n[4/5] TRANSFORMATION → TABLE VEHICULES")
    print("-"*70)
    start = time.time()
    
    df_for_vehicules = df_bronze[['num_acc'] + COLS_VEHICULES_MULTI].copy()
    
    # ⚡ EXPLOSION VECTORISÉE 
    df_vehicules = explode_multivalue_vectorized(df_for_vehicules, COLS_VEHICULES_MULTI)
    df_vehicules = df_vehicules.drop_duplicates(subset=['num_acc', 'num_veh']).copy()
    
    # Transformations (VECTORISÉ)
    df_vehicules['num_veh'] = df_vehicules['num_veh'].astype(str).str.strip()
    df_vehicules['point_choc'] = df_vehicules['choc'].map(POINT_CHOC_MAPPING)
    df_vehicules['manoeuvre'] = df_vehicules['manv'].map(MANOEUVRE_MAPPING)
    df_vehicules['sens_circulation'] = pd.to_numeric(df_vehicules['senc'], errors='coerce').astype('Int64')
    df_vehicules['obstacle_mobile'] = df_vehicules['obsm'].map(OBSTACLE_MOBILE_MAPPING).fillna(0).astype('Int64')
    df_vehicules['obstacle_fixe'] = df_vehicules['obs'].map(OBSTACLE_FIXE_MAPPING)
    df_vehicules['nb_occupants'] = pd.to_numeric(df_vehicules['occutc'], errors='coerce').astype('Int64')
    df_vehicules['categorie_vehicule'] = df_vehicules['catv'].map(CATEGORIE_VEHICULE_MAPPING)
    
    df_vehicules_silver = df_vehicules[[
        'num_acc', 'num_veh', 'sens_circulation', 'categorie_vehicule',
        'obstacle_fixe', 'obstacle_mobile', 'point_choc', 'manoeuvre', 'nb_occupants'
    ]].copy()
    
    print(f"✓ Table vehicules : {len(df_vehicules_silver):,} lignes en {time.time()-start:.1f}s")
    
    # ═══════════════════════════════════════════════════════════
    # ÉTAPE 5 : TABLE USAGERS 
    # ═══════════════════════════════════════════════════════════
    
    print("\n[5/5] TRANSFORMATION → TABLE USAGERS")
    print("-"*70)
    start = time.time()
    
    df_for_usagers = df_bronze[['num_acc', 'an'] + COLS_USAGERS_MULTI + ['num_veh']].copy()
    
    # ⚡ EXPLOSION VECTORISÉE
    df_usagers = explode_multivalue_vectorized(df_for_usagers, COLS_USAGERS_MULTI + ['num_veh'])
    
    # Transformations (VECTORISÉ)
    df_usagers['gravite'] = df_usagers['grav'].map(GRAVITE_MAPPING)
    df_usagers['sexe'] = df_usagers['sexe'].map(SEXE_MAPPING)
    df_usagers['annee_naissance'] = pd.to_numeric(df_usagers['an_nais'], errors='coerce').astype('Int64')
    df_usagers['annee_accident'] = pd.to_numeric(df_usagers['an'], errors='coerce').astype('Int64')
    df_usagers['age_au_moment_accident'] = df_usagers['annee_accident'] - df_usagers['annee_naissance']
    
    # Nettoyage âges (VECTORISÉ)
    df_usagers.loc[
        (df_usagers['age_au_moment_accident'] < 0) | (df_usagers['age_au_moment_accident'] > 120),
        'age_au_moment_accident'
    ] = None
    
    df_usagers['place_vehicule'] = pd.to_numeric(df_usagers['place'], errors='coerce').astype('Int64')
    df_usagers['categorie_usager'] = df_usagers['catu'].map(CATEGORIE_USAGER_MAPPING)
    df_usagers['motif_deplacement'] = pd.to_numeric(df_usagers['trajet'], errors='coerce').astype('Int64')
    df_usagers['equipement_securite'] = df_usagers['secu'].map(EQUIPEMENT_SECURITE_MAPPING)    
    df_usagers['localisation_pieton'] = pd.to_numeric(df_usagers['locp'], errors='coerce').astype('Int64')
    df_usagers['action_pieton'] = pd.to_numeric(df_usagers['actp'], errors='coerce').astype('Int64')
    df_usagers['etat_pieton'] = pd.to_numeric(df_usagers['etatp'], errors='coerce').astype('Int64')
    df_usagers['num_veh'] = df_usagers['num_veh'].astype(str).str.strip().replace('nan', None)
    
    df_usagers_silver = df_usagers[[
        'num_acc', 'num_veh', 'place_vehicule', 'categorie_usager', 'gravite',
        'sexe', 'annee_naissance', 'age_au_moment_accident', 'motif_deplacement',
        'equipement_securite', 'localisation_pieton', 'action_pieton', 'etat_pieton'
    ]].copy()
    
    print(f"✓ Table usagers : {len(df_usagers_silver):,} lignes en {time.time()-start:.1f}s")
    
    # ═══════════════════════════════════════════════════════════
    # STATISTIQUES
    # ═══════════════════════════════════════════════════════════
    
    elapsed_total = time.time() - start_global
    
    print("\n" + "═"*70)
    print("✅ TRANSFORMATION TERMINÉE")
    print("═"*70)
    print(f"\n⚡ Temps total : {elapsed_total:.1f}s ({elapsed_total/60:.1f} min)")
    print(f"\n📊 Tables générées :")
    print(f"  - accidents  : {len(df_accidents_silver):>10,} lignes")
    print(f"  - lieux      : {len(df_lieux_silver):>10,} lignes")
    print(f"  - vehicules  : {len(df_vehicules_silver):>10,} lignes")
    print(f"  - usagers    : {len(df_usagers_silver):>10,} lignes")
    print(f"\n💾 Gain mémoire : {(df_bronze.memory_usage(deep=True).sum() / 1024**2):.1f} MB → "
          f"{(df_accidents_silver.memory_usage(deep=True).sum() + df_lieux_silver.memory_usage(deep=True).sum() + df_vehicules_silver.memory_usage(deep=True).sum() + df_usagers_silver.memory_usage(deep=True).sum()) / 1024**2:.1f} MB")
    
    return {
        'accidents': df_accidents_silver,
        'lieux': df_lieux_silver,
        'vehicules': df_vehicules_silver,
        'usagers': df_usagers_silver
    }


# ════════════════════════════════════════════════════════════════
# CHARGEMENT SQL EN BASE
# ════════════════════════════════════════════════════════════════

def load_silver_tables(dataframes):
    print("\n" + "═"*70)
    print("CHARGEMENT TABLES SILVER")
    print("═"*70)
    
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        echo=False,
        pool_pre_ping=True  # ⚡ Vérifier connexion avant utilisation
    )
    
    tables = [
        ('accidents', dataframes['accidents']),
        ('lieux', dataframes['lieux']),
        ('vehicules', dataframes['vehicules']),
        ('usagers', dataframes['usagers'])
    ]
    

    for table_name, df in tables:
        print(f"\n[{table_name.upper()}]")
        print("-"*70)
        start = time.time()
        
    try:
        # ⚡ DÉSACTIVER TOUTES LES CONTRAINTES FK
        with engine.begin() as conn:
            conn.execute(text("SET session_replication_role = 'replica';"))
        
        for table_name, df in tables:
            print(f"\n[{table_name.upper()}]")
            print("-"*70)
            start = time.time()
            
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
            print(f"  ✓ Table vidée")
            
            df.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=5000
            )
            
            print(f"  ✓ {len(df):,} lignes insérées en {time.time()-start:.1f}s")
            
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                print(f"  ✓ Vérification : {count:,} lignes")
        
        # ⚡ RÉACTIVER LES CONTRAINTES FK
        with engine.begin() as conn:
            conn.execute(text("SET session_replication_role = 'origin';"))
            
    except Exception as e:
        print(f"  ❌ Erreur : {e}")
        # Réactiver les FK même en cas d'erreur
        with engine.begin() as conn:
            conn.execute(text("SET session_replication_role = 'origin';"))
        raise


# ════════════════════════════════════════════════════════════════
# EXÉCUTION
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    with open('Logs/etl_logs.txt', 'w', encoding='utf-8') as log_file:
        original_stdout = sys.stdout
        sys.stdout = log_file
        
        try:
            print("═"*70)
            print("ETL COUCHE SILVER - VERSION ULTRA-OPTIMISÉE")
            print("═"*70)
            
            dataframes_silver = etl_silver()
            load_silver_tables(dataframes_silver)
            
            print("\n✅ ETL SILVER TERMINÉ AVEC SUCCÈS!")
            
        except Exception as e:
            print(f"\n❌ ERREUR : {e}")
            import traceback
            traceback.print_exc()
        finally:
            sys.stdout = original_stdout
    
    print("✅ ETL terminé - voir etl_logs.txt")