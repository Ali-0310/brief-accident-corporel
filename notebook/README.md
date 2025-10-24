# brief-accident-corporel

### 1. Configuration PostgreSQL

Modifiez le fichier .env avec vos paramètres PostgreSQL existants :
```
POSTGRES_HOST=localhost         # Ou IP de votre serveur
POSTGRES_PORT=5432              # Port PostgreSQL
POSTGRES_DB=nom_votre_db        # Nom de votre base
POSTGRES_USER=votre_user        # Votre utilisateur
POSTGRES_PASSWORD=votre_mdp     # Votre mot de passe
```

### 2. Installation des dépendances Python

```
pip install -r requirements.txt
```
### 3. Création des tables dans la base
Pour créer les tables, utiliser les scripts SQL du dossier "sql".

### 4. Utilisation du Notebook
Le notebook permet:
- De télécharger le dataset par appel API
- De remplir la base avec ces données brutes
- De créer les tables de la couche Gold.
