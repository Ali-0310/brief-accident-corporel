"""
ANALYSES MÉTIER - CRITÈRES DE PERFORMANCE
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


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

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Configuration du logging
class Logger:
    def __init__(self, filename=None):
        self.console = sys.stdout
        self.log_file = None
        if filename:
            self.log_file = open(filename, 'w', encoding='utf-8')
    
    def write(self, message):
        self.console.write(message)
        if self.log_file:
            self.log_file.write(message)
            self.log_file.flush()
    
    def flush(self):
        self.console.flush()
        if self.log_file:
            self.log_file.flush()

# ════════════════════════════════════════════════════════════════
# DICTIONNAIRES POUR LA CORRESPONDANCE DES CODES
# ════════════════════════════════════════════════════════════════

DICT_CONDITIONS_ATMOS = {
    1: "Normale",
    2: "Pluie légère",
    3: "Pluie forte",
    4: "Neige - grêle",
    5: "Brouillard - fumée",
    6: "Vent fort - tempête",
    7: "Temps éblouissant",
    8: "Temps couvert",
    9: "Autre"
}

DICT_LUMINOSITE = {
    1: "Plein jour",
    2: "Crépuscule ou aube",
    3: "Nuit sans éclairage",
    4: "Nuit avec éclairage allumé",
    5: "Nuit avec éclairage non allumé"
}

DICT_CATEGORIE_ROUTE = {
    1: "Autoroute",
    2: "Route nationale",
    3: "Route Départementale",
    4: "Voie Communale",
    5: "Hors réseau public",
    6: "Parc de stationnement ouvert à la circulation publique",
    7: "Routes de métropole urbaine",
    9: "Autre"
}

DICT_ETAT_SURFACE = {
    1: "Normale",
    2: "Mouillée",
    3: "Flaques",
    4: "Inondée",
    5: "Enneigée",
    6: "Boue",
    7: "Verglacée",
    8: "Corps gras - huile",
    9: "Autre"
}

DICT_CATEGORIE_USAGER = {
    1: "Voiture",
    2: "Cyclomoteur <50cm3",
    3: "Moto >50cm3 et <=125cm3",
    4: "Moto >125cm3",
    5: "Utilitaire",
    6: "Poids lourd",
    7: "Autocar",
    8: "Matériel agricole",
    9: "Tramway",
    10: "Vélo",
    11: "Quad",
    12: "Autre",
    13: "Camionnette",
    14: "Moto >50cm3 et <=125cm3",
    15: "Moto >125cm3",
    16: "Quad lourd >50cm3",
    17: "Bus",
    18: "Train",
    19: "Tramway",
    20: "3 roues",
    21: "EDP à moteur",
    22: "EDP sans moteur",
    30: "Trottinette électrique",
    31: "Nouvelles mobilités"
}

DICT_GRAVITE = {
    1: "Indemne",
    2: "Tué",
    3: "Blessé hospitalisé",
    4: "Blessé léger"
}

# ════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES POUR LA TRANSFORMATION
# ════════════════════════════════════════════════════════════════

def transformer_dataframe_conditions(df):
    """
    Transforme les codes numériques en libellés pour les conditions
    """
    if 'conditions_atmospheriques' in df.columns:
        df['conditions_atmospheriques_lib'] = df['conditions_atmospheriques'].map(DICT_CONDITIONS_ATMOS).fillna('Non renseigné')
    
    if 'luminosite' in df.columns:
        df['luminosite_lib'] = df['luminosite'].map(DICT_LUMINOSITE).fillna('Non renseigné')
    
    if 'categorie_route' in df.columns:
        df['categorie_route_lib'] = df['categorie_route'].map(DICT_CATEGORIE_ROUTE).fillna('Non renseigné')
    
    if 'etat_surface' in df.columns:
        df['etat_surface_lib'] = df['etat_surface'].map(DICT_ETAT_SURFACE).fillna('Non renseigné')
    
    if 'categorie_usager' in df.columns:
        df['categorie_usager_lib'] = df['categorie_usager'].map(DICT_CATEGORIE_USAGER).fillna('Non renseigné')
    
    if 'gravite' in df.columns:
        df['gravite_lib'] = df['gravite'].map(DICT_GRAVITE).fillna('Non renseigné')
    
    return df

def afficher_top_conditions(df, top_n=5):
    """
    Affiche le top des conditions dangereuses de manière lisible
    """
    print(f"\nTOP {top_n} CONDITIONS LES PLUS DANGEREUSES :")
    print("-" * 80)
    
    for i, row in df.head(top_n).iterrows():
        print(f"{i+1}. {row.get('conditions_atmospheriques_lib', 'N/A')} | "
              f"{row.get('luminosite_lib', 'N/A')} | "
              f"{row.get('categorie_route_lib', 'N/A')} | "
              f"{row.get('etat_surface_lib', 'N/A')}")
        print(f"   Accidents: {row['nb_accidents']} | "
              f"Taux mortalité: {row['taux_mortalite']}% | "
              f"Ratio vs national: {row['ratio_vs_national']}x | "
              f"Niveau: {row['niveau_risque']}")
        print()

# ════════════════════════════════════════════════════════════════
# CRITÈRE 1 : CONDITIONS À RISQUE ÉLEVÉ
# ════════════════════════════════════════════════════════════════

def critere_1_conditions_risque():
    """
    Y a-t-il des conditions (météo + luminosité + type de route) qui 
    présentent un risque significativement supérieur à la moyenne nationale ?
    """
    
    print("\n" + "="*60)
    print("CRITÈRE 1 : CONDITIONS À RISQUE ÉLEVÉ")
    print("="*60)
    
    query_sql = """
    WITH stats_nationales AS (
        -- Calcul moyenne nationale
        SELECT 
            COUNT(DISTINCT a.num_acc) as total_accidents,
            COUNT(u.id_usager) as total_usagers,
            SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) as total_tues,
            ROUND(100.0 * SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_mortalite_national
        FROM accidents a
        JOIN usagers u ON a.num_acc = u.num_acc
    ),
    conditions_combinees AS (
        -- Agrégation par combinaison de conditions
        SELECT 
            a.conditions_atmospheriques,
            a.luminosite,
            l.categorie_route,
            l.etat_surface,
            COUNT(DISTINCT a.num_acc) as nb_accidents,
            COUNT(u.id_usager) as nb_usagers,
            SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) as nb_tues,
            SUM(CASE WHEN u.gravite = 3 THEN 1 ELSE 0 END) as nb_blesses_graves,
            ROUND(100.0 * SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_mortalite
        FROM accidents a
        JOIN lieux l ON a.num_acc = l.num_acc
        JOIN usagers u ON a.num_acc = u.num_acc
        WHERE a.conditions_atmospheriques IS NOT NULL 
          AND a.luminosite IS NOT NULL 
          AND l.categorie_route IS NOT NULL
          AND l.etat_surface IS NOT NULL
        GROUP BY a.conditions_atmospheriques, a.luminosite, l.categorie_route, l.etat_surface
        HAVING COUNT(DISTINCT a.num_acc) >= 50  -- Seuil significativité
    )
    SELECT 
        c.conditions_atmospheriques,
        c.luminosite,
        c.categorie_route,
        c.etat_surface,
        c.nb_accidents,
        c.nb_usagers,
        c.nb_tues,
        c.nb_blesses_graves,
        c.taux_mortalite,
        n.taux_mortalite_national,
        ROUND(c.taux_mortalite / NULLIF(n.taux_mortalite_national, 0), 2) as ratio_vs_national,
        CASE 
            WHEN c.taux_mortalite > n.taux_mortalite_national * 3 THEN 'TRÈS DANGEREUX (>3x)'
            WHEN c.taux_mortalite > n.taux_mortalite_national * 2 THEN 'DANGEREUX (>2x)'
            WHEN c.taux_mortalite > n.taux_mortalite_national * 1.5 THEN 'RISQUE ÉLEVÉ (>1.5x)'
            ELSE 'RISQUE NORMAL'
        END as niveau_risque
    FROM conditions_combinees c, stats_nationales n
    WHERE c.taux_mortalite > n.taux_mortalite_national  -- Uniquement au-dessus moyenne
    ORDER BY c.taux_mortalite DESC
    LIMIT 20;
    """
    
    print("Exécution requête...")
    df_sql = pd.read_sql(query_sql, engine)
    
    # Transformation des codes en libellés
    df_sql = transformer_dataframe_conditions(df_sql)
    
    print(f"Résultats : {len(df_sql)} combinaisons dangereuses")
    
    # Affichage formaté avec libellés
    afficher_top_conditions(df_sql, top_n=5)
    
    # Affichage du taux national
    if len(df_sql) > 0:
        taux_national = df_sql.iloc[0]['taux_mortalite_national']
        print(f"Taux de mortalité national de référence : {taux_national}%")
    
    return df_sql

# ════════════════════════════════════════════════════════════════
# CRITÈRE 2 : ZONES FRÉQUENTÉES VS ZONES DANGEREUSES
# ════════════════════════════════════════════════════════════════

def critere_2_zones_frequentees_vs_dangereuses():
    """
    Les zones les plus fréquentées ont-elles plus d'accidents graves 
    ou simplement plus d'accidents tout court ?
    """
    
    print("\n" + "="*60)
    print("CRITÈRE 2 : ZONES FRÉQUENTÉES VS DANGEREUSES")
    print("="*60)
    
    query_sql = """
    WITH stats_departements AS (
        -- Statistiques par département
        SELECT 
            a.departement_code,
            COUNT(DISTINCT a.num_acc) as nb_accidents,
            COUNT(u.id_usager) as nb_usagers,
            SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) as nb_tues,
            SUM(CASE WHEN u.gravite = 3 THEN 1 ELSE 0 END) as nb_blesses_graves,
            ROUND(100.0 * SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_mortalite,
            ROUND(100.0 * SUM(CASE WHEN u.gravite IN (2,3) THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_gravite
        FROM accidents a
        JOIN usagers u ON a.num_acc = u.num_acc
        GROUP BY a.departement_code
    ),
    quartiles AS (
        -- Calcul quartiles pour segmentation
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY nb_accidents) as q2_volume,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY nb_accidents) as q3_volume,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY taux_gravite) as q2_gravite,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY taux_gravite) as q3_gravite
        FROM stats_departements
    )
    SELECT 
        s.departement_code,
        s.nb_accidents,
        s.nb_tues,
        s.taux_mortalite,
        s.taux_gravite,
        -- Segmentation 4 quadrants
        CASE 
            WHEN s.nb_accidents >= q.q3_volume AND s.taux_gravite >= q.q3_gravite 
                THEN 'CRITIQUE (Volume + Gravité)'
            WHEN s.nb_accidents >= q.q3_volume AND s.taux_gravite < q.q2_gravite 
                THEN 'FRÉQUENTÉE (Volume élevé, gravité normale)'
            WHEN s.nb_accidents < q.q2_volume AND s.taux_gravite >= q.q3_gravite 
                THEN 'DANGEREUSE (Volume faible, gravité élevée)'
            ELSE 'STANDARD'
        END as typologie_zone,
        -- Classements
        RANK() OVER (ORDER BY s.nb_accidents DESC) as rang_volume,
        RANK() OVER (ORDER BY s.taux_gravite DESC) as rang_gravite
    FROM stats_departements s, quartiles q
    ORDER BY s.nb_accidents DESC;
    """
    
    print("Exécution requête...")
    df_sql = pd.read_sql(query_sql, engine)
    
    print(f"{len(df_sql)} départements analysés")
    
    # Affichage formaté de la répartition
    print("\nRÉPARTITION PAR TYPOLOGIE :")
    repartition = df_sql['typologie_zone'].value_counts()
    for typologie, count in repartition.items():
        print(f"  {typologie} : {count} départements")
    
    # Affichage des départements critiques
    df_critiques = df_sql[df_sql['typologie_zone'] == 'CRITIQUE (Volume + Gravité)']
    if len(df_critiques) > 0:
        print(f"\nDÉPARTEMENTS CRITIQUES (Volume + Gravité élevés) :")
        for _, dept in df_critiques.head(10).iterrows():
            print(f"  {dept['departement_code']} - {dept['nb_accidents']} accidents - "
                  f"Taux gravité: {dept['taux_gravite']}%")
    
    # Affichage des top départements par volume
    print("\nTOP 10 DÉPARTEMENTS PAR VOLUME D'ACCIDENTS :")
    for i, row in df_sql.head(10).iterrows():
        print(f"  {row['rang_volume']:2d}. Dépt {row['departement_code']} - "
              f"{row['nb_accidents']:4d} accidents - "
              f"Gravité: {row['taux_gravite']:5.1f}% - "
              f"{row['typologie_zone']}")
    
    return df_sql

# ════════════════════════════════════════════════════════════════
# CRITÈRE 3 : ANALYSE PAR CATÉGORIE D'USAGERS
# ════════════════════════════════════════════════════════════════

def critere_3_analyse_usagers():
    """
    Analyse spécifique par catégorie d'usagers (piétons, cyclistes, etc.)
    pour identifier les populations les plus vulnérables
    """
    
    print("\n" + "="*60)
    print("CRITÈRE 3 : ANALYSE PAR CATÉGORIE D'USAGERS")
    print("="*60)
    
    query_sql = """
    WITH stats_usagers AS (
        -- Statistiques par catégorie d'usager
        SELECT 
            u.categorie_usager,
            COUNT(u.id_usager) as nb_usagers,
            SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) as nb_tues,
            SUM(CASE WHEN u.gravite = 3 THEN 1 ELSE 0 END) as nb_blesses_graves,
            SUM(CASE WHEN u.gravite = 4 THEN 1 ELSE 0 END) as nb_blesses_legers,
            SUM(CASE WHEN u.gravite = 1 THEN 1 ELSE 0 END) as nb_indemnes,
            ROUND(100.0 * SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_mortalite,
            ROUND(100.0 * SUM(CASE WHEN u.gravite IN (2,3) THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_gravite
        FROM usagers u
        WHERE u.categorie_usager IS NOT NULL
        GROUP BY u.categorie_usager
        HAVING COUNT(u.id_usager) >= 100  -- Seuil de significativité
    ),
    stats_globales AS (
        -- Statistiques globales pour référence
        SELECT 
            COUNT(u.id_usager) as total_usagers,
            ROUND(100.0 * SUM(CASE WHEN u.gravite = 2 THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(u.id_usager), 0), 3) as taux_mortalite_global
        FROM usagers u
    )
    SELECT 
        s.categorie_usager,
        s.nb_usagers,
        s.nb_tues,
        s.nb_blesses_graves,
        s.nb_blesses_legers,
        s.nb_indemnes,
        s.taux_mortalite,
        s.taux_gravite,
        g.taux_mortalite_global,
        ROUND(s.taux_mortalite / NULLIF(g.taux_mortalite_global, 0), 2) as ratio_vs_global,
        CASE 
            WHEN s.taux_mortalite > g.taux_mortalite_global * 3 THEN 'TRÈS VULNÉRABLE (>3x)'
            WHEN s.taux_mortalite > g.taux_mortalite_global * 2 THEN 'VULNÉRABLE (>2x)'
            WHEN s.taux_mortalite > g.taux_mortalite_global * 1.5 THEN 'À RISQUE (>1.5x)'
            ELSE 'RISQUE NORMAL'
        END as niveau_vulnerabilite
    FROM stats_usagers s, stats_globales g
    ORDER BY s.taux_mortalite DESC;
    """
    
    print("Exécution requête analyse usagers...")
    df_sql = pd.read_sql(query_sql, engine)
    
    # Transformation des codes en libellés
    df_sql = transformer_dataframe_conditions(df_sql)
    
    print(f"{len(df_sql)} catégories d'usagers analysées")
    
    # Affichage des usagers les plus vulnérables
    print("\nTOP 10 CATÉGORIES D'USAGERS LES PLUS VULNÉRABLES :")
    print("-" * 70)
    for i, row in df_sql.head(10).iterrows():
        print(f"{i+1:2d}. {row.get('categorie_usager_lib', 'N/A'):40}")
        print(f"     Usagers: {row['nb_usagers']:5d} | "
              f"Tués: {row['nb_tues']:3d} | "
              f"Taux mortalité: {row['taux_mortalite']:5.1f}% | "
              f"Ratio: {row['ratio_vs_global']:4.1f}x | "
              f"{row['niveau_vulnerabilite']}")
    
    # Analyse spécifique des usagers vulnérables
    usagers_vulnerables = ['Vélo', 'Piéton', 'Cyclomoteur', 'Trottinette', 'EDP']
    df_vulnerables = df_sql[df_sql['categorie_usager_lib'].str.contains('|'.join(usagers_vulnerables), na=False)]
    
    if len(df_vulnerables) > 0:
        print(f"\nANALYSE SPÉCIFIQUE DES USAGERS VULNÉRABLES :")
        print("-" * 50)
        for _, row in df_vulnerables.iterrows():
            print(f"  {row.get('categorie_usager_lib', 'N/A')}: "
                  f"{row['taux_mortalite']:5.1f}% de mortalité "
                  f"({row['ratio_vs_global']:4.1f}x moyenne)")
    
    return df_sql

# ════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════

def main():
    """
    Fonction principale exécutant les analyses métier
    """
    # Configuration du logging
    log_filename = f"analyse_accidents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logger = Logger('Logs/' + log_filename)
    sys.stdout = logger
    
    print("ANALYSE COMPLÈTE DES PERFORMANCES - BASE DE DONNÉES ACCIDENTS")
    print(f"Log enregistré dans : {log_filename}")
    print("=" * 60)
    
    try:
        # Test connexion base de données
        print("Test de connexion à la base de données...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM accidents"))
            nb_accidents = result.scalar()
        print(f"Connexion réussie - {nb_accidents} accidents dans la base")
        
        # ═══════════════════════════════════════════════════════════
        # EXÉCUTION DES 3 CRITÈRES
        # ═══════════════════════════════════════════════════════════
        
        # Critère 1 : Conditions à risque élevé
        print("\n" + "-"*40)
        print("CRITÈRE 1 : CONDITIONS À RISQUE ÉLEVÉ")
        print("-"*40)
        df_critere1_sql = critere_1_conditions_risque()
        
        # Critère 2 : Zones fréquentées vs dangereuses
        print("\n" + "-"*40)
        print("CRITÈRE 2 : ZONES FRÉQUENTÉES VS DANGEREUSES")
        print("-"*40)
        df_critere2_sql = critere_2_zones_frequentees_vs_dangereuses()
        
        # Critère 3 : Analyse par catégorie d'usagers
        print("\n" + "-"*40)
        print("CRITÈRE 3 : ANALYSE PAR CATÉGORIE D'USAGERS")
        print("-"*40)
        df_critere3_sql = critere_3_analyse_usagers()
        
        # ═══════════════════════════════════════════════════════════
        # SYNTHÈSE DES RÉSULTATS
        # ═══════════════════════════════════════════════════════════
        
        print("\n" + "="*60)
        print("SYNTHÈSE DES ANALYSES - RÉSULTATS CLÉS")
        print("="*60)
        
        # Synthèse Critère 1
        conditions_dangereuses = len(df_critere1_sql)
        if conditions_dangereuses > 0:
            top_condition = df_critere1_sql.iloc[0]
            print(f"\nCRITÈRE 1 - CONDITIONS DANGEREUSES :")
            print(f"  {conditions_dangereuses} combinaisons identifiées comme plus dangereuses que la moyenne")
            print(f"  Condition la plus risquée :")
            print(f"    Météo: {top_condition.get('conditions_atmospheriques_lib', 'N/A')}")
            print(f"    Luminosité: {top_condition.get('luminosite_lib', 'N/A')}")
            print(f"    Route: {top_condition.get('categorie_route_lib', 'N/A')}")
            print(f"    Surface: {top_condition.get('etat_surface_lib', 'N/A')}")
            print(f"    Taux mortalité: {top_condition['taux_mortalite']}% (x{top_condition['ratio_vs_national']} vs national)")
        
        # Synthèse Critère 2
        repartition_zones = df_critere2_sql['typologie_zone'].value_counts()
        
        print(f"\nCRITÈRE 2 - TYPOLOGIE DES ZONES :")
        for zone_type, count in repartition_zones.items():
            print(f"  {zone_type} : {count} départements")
        
        # Synthèse Critère 3
        if len(df_critere3_sql) > 0:
            top_usager = df_critere3_sql.iloc[0]
            print(f"\nCRITÈRE 3 - USAGERS VULNÉRABLES :")
            print(f"  Catégorie la plus vulnérable : {top_usager.get('categorie_usager_lib', 'N/A')}")
            print(f"  Taux mortalité: {top_usager['taux_mortalite']}% (x{top_usager['ratio_vs_global']} vs moyenne globale)")
            

    except Exception as e:
        print(f"\nERREUR : {e}")
        print("Vérifiez la connexion à la base de données et la structure des tables")
        return None
    finally:
        # Restaurer la sortie standard
        sys.stdout = logger.console
        if logger.log_file:
            logger.log_file.close()

# ════════════════════════════════════════════════════════════════
# EXÉCUTION
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import time
    
    debut = time.time()
    main()
    duree = time.time() - debut
    
    print(f"\nTemps d'exécution total : {duree:.2f} secondes")