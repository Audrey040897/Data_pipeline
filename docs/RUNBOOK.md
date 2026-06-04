# RUNBOOK SPOTIFY — Procédures incidents

># RUNBOOK — Spotify Data Pipeline Phase 1  

---
**Statut Phase 1:** ✅ RESOLVED (0 incidents actifs)  

---

## 📑 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Incident #1 - TaskFlow API](#incident-1--taskflow-api-non-supportée)
3. [Incident #2 - Tests pytest](#incident-2--tests-pytest-échouent)
4. [Incident #3 - Dépendances](#incident-3--dépendances-python-incompatibles)
5. [Procédures courantes](#procédures-courantes)
6. [Checklist santé](#checklist-santé)
7. [Escalade](#escalade-et-support)

---

## 📊 Vue d'ensemble

### Incidents Phase 1

```
┌──────────────────────────────────────────────────────────────┐
│             INCIDENTS PHASE 1 (RÉSOLUS)                      │
├────────────────────┬───────────┬────────────┬───────────────┤
│ ID                 │ Sévérité  │ Impact     │ Status        │
├────────────────────┼───────────┼────────────┼───────────────┤
│ INC-01: TaskFlow   │ CRITICAL  │ 2h         │ ✅ RESOLVED   │
│ INC-02: Tests DB   │ MEDIUM    │ 1h         │ ✅ RESOLVED   │
│ INC-03: Dépendances│ MEDIUM    │ 30min      │ ✅ RESOLVED   │
└────────────────────┴───────────┴────────────┴───────────────┘

Temps total résolution: ~3.5 heures
Leçons apprises: 3 (documentées ci-dessous)
Prévention: 100% des cas couverts
```

---

## 🔴 Incident #1 — TaskFlow API non supportée

### 📋 Métadonnées

| Champ | Valeur |
|-------|--------|
| **Ticket** | #4 (catalog_ingestion_pipeline) |
| **Date** | 1er juin 2026, 09:15 UTC |
| **Sévérité** | 🔴 CRITICAL |
| **Temps résolution** | ~2 heures |
| **Cause** | TaskFlow API incompatible Airflow 2.9 |
| **Statut** | ✅ RESOLVED |

### 🔍 Symptômes

Plusieurs indicateurs d'erreur simultanés:

```
❌ DAG disparaît complètement de l'UI Airflow
❌ Logs: "No module named 'airflow.sdk'"
❌ DagBag parse error lors du démarrage
❌ Aucune tâche n'est exécutable
❌ Erreur: "NameError: name 'DAG_DOC' is not defined"
```

**Impact observable:**
- L'UI Airflow affiche "No DAGs" pour catalog_ingestion_pipeline
- Les logs Airflow scheduler montrent des erreurs d'import
- Le DAG ne peut pas être déclenché manuellement

### 🔬 Cause racine

**Problème:** TaskFlow API (décorateurs `@task`) n'est **PAS supportée** dans Airflow 2.9.

```python
# ❌ CODE CASSÉ (TaskFlow API - Airflow 3.0+)
from airflow.decorators import task, dag

@dag
def catalog_ingestion_pipeline():
    @task(task_id="extract")
    def extract_from_minio(**context):
        return data
    
    @task
    def validate_schema(raw):
        return validated

    extract_from_minio() >> validate_schema()

# ❌ Erreur à l'import:
# ImportError: cannot import name 'airflow.sdk'
```

**Root cause analysis:**
1. Le code utilise la **TaskFlow API** (Python 3.9+ feature)
2. Airflow 2.9 ne supporte pas TaskFlow
3. Résultat: ImportError lors du chargement du DAG
4. DagBag refuse d'enregistrer le DAG
5. L'UI Airflow ne le montre pas

### 💥 Impact

```
┌─────────────────────────────────────┐
│  CRITICAL IMPACT                    │
├─────────────────────────────────────┤
│ Scope:        DAG complet cassé     │
│ Users:        Toutes les équipes    │
│ Downtime:     ~2 heures             │
│ Data loss:    ❌ Non                │
│ Cascading:    ❌ Non (isolé)        │
│ Cost:         ~€50 (2h * inaction)  │
└─────────────────────────────────────┘
```

### ✅ Résolution détaillée

#### Étape 1: Identifier le problème

```bash
# Vérifier les erreurs d'import
python -m py_compile dags/catalog_ingestion_pipeline.py

# ❌ Sortie si erreur:
# SyntaxError: invalid syntax
# ou
# ImportError: cannot import name ...

# Voir les logs Airflow
docker compose logs airflow-scheduler | grep -i "error\|import"

# Chercher les décorateurs TaskFlow
grep -n "@task\|@dag" dags/*.py

# ✅ Si des @task trouvés → problème identifié!
```

#### Étape 2: Refactorer vers PythonOperator

**Stratégie:** Remplacer la TaskFlow API par des PythonOperator classiques.

```python
# ❌ AVANT (TaskFlow - Cassé)
from airflow.decorators import task

@task(task_id="extract_from_minio")
def extract_from_minio(**context):
    s3 = boto3.client('s3', ...)
    catalog = s3.get_object(...)
    context['ti'].xcom_push(key='data', value=catalog)
    return catalog

@task(task_id="validate_schema")
def validate_schema(raw_catalogs, **context):
    validated = [...]
    context['ti'].xcom_push(key='valid', value=validated)
    return validated

# ✅ APRÈS (PythonOperator - Correct)
from airflow.operators.python import PythonOperator

def extract_from_minio(**context):
    """Fonction Python standard."""
    s3 = boto3.client('s3', ...)
    catalog = s3.get_object(...)
    context['ti'].xcom_push(key='data', value=catalog)
    return catalog

def validate_schema(**context):
    """Fonction Python standard."""
    raw = context['ti'].xcom_pull(task_ids='extract_from_minio', key='data')
    validated = [...]
    context['ti'].xcom_push(key='valid', value=validated)
    return validated

# Créer les opérateurs
dag = DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    doc_md=DAG_DOC,  # ← Doit être défini AVANT
)

t1 = PythonOperator(
    task_id='extract_from_minio',
    python_callable=extract_from_minio,
    dag=dag
)

t2 = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag
)

# Dépendances
t1 >> t2 >> t3 >> t4 >> t5
```

#### Étape 3: Définir DAG_DOC au bon endroit

**Point critique:** `DAG_DOC` doit être défini AVANT d'être utilisé!

```python
# ✅ STRUCTURE CORRECTE

import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1️⃣ CONFIGURATION
DEFAULT_ARGS = {...}

# 2️⃣ DOCUMENTATION (AVANT le DAG!)
DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- s3://labels-raw/sunset_records.json
- s3://labels-raw/nightwave_music.json
- s3://labels-raw/urban_pulse.json

### Destinations
- Table artists (upsert)
- Table albums (upsert)
- Table tracks (upsert)
- Table dead_letter_events (erreurs)

### Idempotence
✅ Tous les upserts utilisent ON CONFLICT DO UPDATE
→ Relancer le DAG plusieurs fois = même résultat
"""

# 3️⃣ FONCTIONS
def extract_from_minio(**context):
    # ...
    pass

def validate_schema(**context):
    # ...
    pass

# 4️⃣ CRÉER LE DAG (utilise DAG_DOC)
dag = DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion du catalogue musical",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion"],
    doc_md=DAG_DOC,  # ← Maintenant c'est défini!
)

# 5️⃣ CRÉER LES OPÉRATEURS
t1 = PythonOperator(...)
t2 = PythonOperator(...)
# ...

# 6️⃣ DÉPENDANCES
t1 >> t2 >> t3 >> t4 >> t5
```

#### Étape 4: Redémarrer Airflow

```bash
# Arrêter le scheduler
docker compose restart airflow-scheduler

# Attendre le démarrage complet (~1 minute)
sleep 60

# Vérifier les logs
docker compose logs airflow-scheduler | tail -50

# Chercher: "Scheduler started" ou "Loaded X DAGs"
```

#### Étape 5: Valider la résolution

```bash
# ✅ Test 1: Compilation
python -m py_compile dags/catalog_ingestion_pipeline.py
# Résultat: OK (pas d'erreur)

# ✅ Test 2: Airflow UI
# http://localhost:8080
# Chercher: catalog_ingestion_pipeline doit être VISIBLE et VERT

# ✅ Test 3: Tests pytest
pytest tests/structure/test_dag_structure.py::TestCatalogIngestionDAG -v
# Résultat: 7 PASSED

# ✅ Test 4: Trigger manuel
docker compose exec airflow-scheduler airflow dags trigger \
    catalog_ingestion_pipeline \
    --exec-date 2025-06-01

# Résultat: DAGRun créé avec succès
```

### 🛡️ Prévention

**Checklist pour éviter ce problème à l'avenir:**

```markdown
### Avant de créer un DAG:
- [ ] Vérifier la version Airflow: `airflow version`
  - Si 2.9.x → Utiliser PythonOperator classique
  - Si 3.0+ → TaskFlow API OK

- [ ] Utiliser PythonOperator classique:
  ```python
  from airflow.operators.python import PythonOperator
  # ✅ BON
  
  from airflow.decorators import task
  # ❌ MAUVAIS (si Airflow 2.9)
  ```

- [ ] Tester la compilation:
  ```bash
  python -m py_compile dags/your_dag.py
  ```

- [ ] Définir DAG_DOC AVANT le DAG
  ```python
  DAG_DOC = """..."""  # ← AVANT
  dag = DAG(..., doc_md=DAG_DOC)  # ← APRÈS
  ```

- [ ] Vérifier les imports disponibles:
  ```bash
  python -c "from airflow.sdk import ..." 2>&1 | grep Error
  # Si erreur → feature non disponible
  ```

- [ ] Tester dans l'UI après déploiement
```

### 📚 Ressources

- [Airflow 2.9 Operators](https://airflow.apache.org/docs/apache-airflow/2.9.0/howto/operator/python.html)
- [TaskFlow API (Airflow 3.0+)](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
- [XCom Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)

---

## 🟡 Incident #2 — Tests pytest échouent (DB)

### 📋 Métadonnées

| Champ | Valeur |
|-------|--------|
| **Ticket** | #2 (schema creation) |
| **Date** | 1er juin 2026, 14:30 UTC |
| **Sévérité** | 🟡 MEDIUM |
| **Temps résolution** | ~1 heure |
| **Cause** | Mauvaise commande pytest + DB migration |
| **Statut** | ✅ RESOLVED |

### 🔍 Symptômes

```
❌ sqlite3.OperationalError: no such table: dag
❌ ImportError: cannot import name 'ignore_sqlite_value_error'
❌ pytest: command not found
❌ Tests ne s'exécutent pas
❌ DagBag essaie de créer une BD vide
```

### 🔬 Cause racine

**Problème:** Pytest utilise le **mauvais Python/pytest** qui essaie de créer une DB Airflow.

```bash
# ❌ MAUVAISE COMMANDE
pytest tests/

# Python cherche le pytest système (pas du venv)
# → Essaie de créer une DB Airflow
# → Crée une SQLite invalid
# → Erreurs de schéma

# ✅ BONNE COMMANDE
python -m pytest tests/

# Python du venv exécute pytest
# → Pas de DB créée
# → Tests unitaires isolés
# → ✅ PASS
```

### 💥 Impact

```
Scope:        Tests non exécutables
Users:        Équipe QA
Downtime:     ~1 heure
Risk:         Code non validé → bugs en production
```

### ✅ Résolution détaillée

#### Étape 1: Identifier le problème

```bash
# Vérifier qu'on utilise le bon Python
which python
# Résultat: /Users/.../env/bin/python

# Vérifier le pytest
which pytest
# Résultat: /Users/.../env/bin/pytest (BON) ou /usr/bin/pytest (MAUVAIS)

# Chercher l'erreur exacte
pytest tests/ 2>&1 | head -50

# Si erreur = "no such table: dag" → problème identifié!
```

#### Étape 2: Utiliser la bonne commande pytest

```bash
# Setup
source env/bin/activate
export PYTHONPATH=$(pwd)

# ✅ BONNE MÉTHODE
python -m pytest tests/ -v --tb=short

# Résultat:
# tests/unit/test_transformations.py::... PASSED
# tests/structure/test_dag_structure.py::... PASSED
# ======================== 34 passed ========================
```

#### Étape 3: Créer des tests unitaires simples (sans DB)

```python
# ✅ TESTS UNITAIRES (Sans DB, Sans Airflow)

import pytest
from src.transformations.catalog import normalize_artist_name

def test_normalize_artist_name():
    """Test simple sans dépendance externe."""
    # Arrange
    input_name = "  the beatles  "
    
    # Act
    result = normalize_artist_name(input_name)
    
    # Assert
    assert result == "The Beatles"

# ❌ TESTS CASSÉS (Avec DB)
def test_with_airflow_db():
    """Ce test va créer une DB et échouer."""
    from airflow.models.dagbag import DagBag
    dagbag = DagBag()  # ← Essaie de créer une DB!
    # ❌ Error: no such table
```

#### Étape 4: Configurer pytest.ini

```ini
# pytest.ini
[pytest]
pythonpath = .
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Ignorer les warnings Airflow
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning
```

#### Étape 5: Valider

```bash
# Lancer TOUS les tests
python -m pytest tests/ -v --tb=short

# Résultat attendu:
# ======================== 34 passed in 1.51s ========================
# 0 errors, 0 skipped
```

### 🛡️ Prévention

```markdown
### Checklist tests:
- [ ] Toujours lancer: `python -m pytest` (pas `pytest`)
- [ ] Exporter PYTHONPATH: `export PYTHONPATH=$(pwd)`
- [ ] Tests unitaires simples (sans DB)
- [ ] Ne PAS importer DagBag dans tests unitaires
- [ ] Vérifier imports: `python -c "from src.transformations import *"`
- [ ] CI/CD: Utiliser `python -m pytest` dans les scripts
```

---

## 🟡 Incident #3 — Dépendances Python incompatibles

### 📋 Métadonnées

| Champ | Valeur |
|-------|--------|
| **Ticket** | #1 (setup initial) |
| **Date** | 1-2 juin 2026 |
| **Sévérité** | 🟡 MEDIUM |
| **Temps résolution** | ~30 min/incident |
| **Cause** | versions trop strictes (==) dans requirements |
| **Statut** | ✅ RESOLVED |

### 🔍 Symptômes

```
❌ ImportError: cannot import name 'ClientError'
❌ ModuleNotFoundError: No module named 'faker'
❌ boto3 1.20.0 requires botocore>=1.23.0 (have 1.19.0)
❌ Environnement complètement cassé
❌ Impossible de démarrer les services
```

### 🔬 Cause racine

**Problème:** requirements.txt avec versions trop strictes crée des **conflits de dépendances**.

```bash
# ❌ MAUVAIS (trop strict)
boto3==1.26.0
botocore==1.29.0
faker==15.0.0
redis==4.5.0

# Problèmes:
# 1. Si un package dépend de faker>=16.0 → conflit!
# 2. Mise à jour d'une dépendance → tout casse
# 3. Incompatibilité transitive cachée

# ✅ BON (avec ranges)
boto3>=1.26.0,<2.0.0
botocore>=1.29.0,<2.0.0
faker>=15.0.0,<20.0.0
redis>=4.5.0,<5.0.0
```

### 💥 Impact

```
Scope:        Environnement complet
Users:        Toute l'équipe
Downtime:     ~30 minutes par occurrence
Risk:         Impossible de développer/tester
```

### ✅ Résolution détaillée

#### Étape 1: Identifier la dépendance manquante

```bash
# Voir quel package manque
pip list | grep -E "boto3|faker|redis"

# Chercher le module dans le code
grep -r "from faker import\|import boto3\|import redis" src/ dags/

# Chercher les dépendances transitivesconfits
pip check

# Résultat si conflit:
# boto3 1.20.0 requires botocore>=1.23.0
#   but you have botocore 1.19.0
```

#### Étape 2: Assouplir les versions

```txt
# ❌ ANCIEN (requirements.txt - Strict)
boto3==1.26.0
botocore==1.29.0
faker==15.0.0
airflow==2.9.0
pandas==1.5.0
redis==4.5.0
psycopg2-binary==2.9.0

# ✅ NOUVEAU (requirements.txt - Flexible)
# === CORE FRAMEWORKS ===
apache-airflow[postgres,redis]==2.9.0
psycopg2-binary>=2.9.0,<3.0.0

# === DATA PROCESSING ===
pandas>=1.5.0,<2.0.0
faker>=15.0.0,<20.0.0

# === CLOUD ===
boto3>=1.26.0,<2.0.0
botocore>=1.29.0,<2.0.0

# === CACHE ===
redis>=4.5.0,<5.0.0

# === TESTING ===
pytest>=8.0.0,<9.0.0
pytest-cov>=4.0.0,<5.0.0
```

#### Étape 3: Réinstaller l'environnement

```bash
# Option A: Upgrade les packages existants
pip install -r requirements.txt --upgrade

# Option B: Reset complet (plus sûr)
# 1. Supprimer le venv
rm -rf env/

# 2. Créer un nouveau venv
python3.13 -m venv env
source env/bin/activate

# 3. Installer fresh
pip install -r requirements.txt

# 4. Vérifier
pip check
# Résultat: No broken requirements found!
```

#### Étape 4: Tester les imports

```bash
# Tester chaque dépendance clé
python -c "import boto3; print('✅ boto3')"
python -c "import faker; print('✅ faker')"
python -c "import airflow; print('✅ airflow')"
python -c "import pandas; print('✅ pandas')"
python -c "import redis; print('✅ redis')"
python -c "from src.transformations import *; print('✅ src')"

# Si tous OK → environnement sain!
```

#### Étape 5: Valider complètement

```bash
# Tests unitaires
python -m pytest tests/ -v

# Résultat attendu:
# ======================== 34 passed ========================
```

### 🛡️ Prévention

```markdown
### Checklist dépendances:
- [ ] Utiliser ranges: `>=X.Y.Z,<X+1.0.0`
- [ ] Tester: `pip check` (pas de conflits)
- [ ] Documenter: pourquoi cette version?
- [ ] Vérifier conflits: `pip install -r requirements.txt --dry-run`
- [ ] Tests après chaque update
- [ ] Documenter les dépendances critiques
```

**Exemple structure requirements.txt:**

```txt
# ========== CORE ==========
apache-airflow[postgres,redis]==2.9.0
psycopg2-binary>=2.9.0,<3.0.0

# ========== DATA ==========
pandas>=1.5.0,<2.0.0
faker>=15.0.0,<20.0.0

# ========== CLOUD/STORAGE ==========
boto3>=1.26.0,<2.0.0
botocore>=1.29.0,<2.0.0
minio>=7.0.0,<8.0.0

# ========== CACHE/QUEUE ==========
redis>=4.5.0,<5.0.0

# ========== TESTING ==========
pytest>=8.0.0,<9.0.0
pytest-cov>=4.0.0,<5.0.0
pytest-mock>=3.0.0,<4.0.0
```

---

## 🔧 Procédures courantes

### Redémarrer Airflow complètement

```bash
# 1. Arrêter tous les conteneurs
docker compose down

# 2. Nettoyer les données temporaires
docker volume rm $(docker volume ls -q | grep airflow)

# 3. Redémarrer
docker compose up -d

# 4. Vérifier
docker compose logs airflow-scheduler | grep "Scheduler started"
```

### Vérifier la santé du pipeline

```bash
# Airflow
docker compose logs airflow-scheduler | tail -50

# PostgreSQL
docker compose exec postgres psql -U spotify spotify -c "SELECT COUNT(*) FROM artists;"

# MinIO
docker compose logs minio | tail -20

# Redis
docker compose exec redis redis-cli PING
# Résultat: PONG ✅
```

### Nettoyer et réinitialiser (⚠️  Destructif)

```bash
# Cela supprime TOUTES les données!
docker compose down
docker volume rm $(docker volume ls -q | grep spotify)
docker compose up -d
```

---

## ✅ Checklist santé

### Avant chaque déploiement

```markdown
- [ ] Tous les tests passent: `pytest tests/ -v`
- [ ] DAGs compilent: `python -m py_compile dags/*.py`
- [ ] Pas de dépendances cassées: `pip check`
- [ ] Logs Airflow OK: `docker compose logs airflow-scheduler`
- [ ] PostgreSQL accessible: `docker compose exec postgres psql -U spotify spotify`
- [ ] MinIO accessible: UI à http://localhost:9001
- [ ] Redis accessible: `docker compose exec redis redis-cli PING`
```

### Avant un changement majeur

```markdown
- [ ] Backup des données: `docker compose exec postgres pg_dump -U spotify spotify > backup.sql`
- [ ] Documenter le changement
- [ ] Tester en dev d'abord
- [ ] Avoir un plan de rollback
- [ ] Notifier l'équipe
```

---

## 📞 Escalade et support

### Informations à collecter

```bash
# Environnement
python --version
docker --version
docker compose --version
airflow version

# Logs pertinents
docker compose logs airflow-scheduler > logs-scheduler.txt
docker compose logs postgres > logs-postgres.txt
docker compose ps > docker-status.txt

# Tests
pytest tests/ -v > test-output.txt 2>&1

# Configuration
docker compose config > config-dump.txt

# Dépendances
pip freeze > pip-freeze.txt
pip check > pip-check.txt
```

### Escalade au formateur

Fournir:
1. Description du symptôme
2. Logs complets (voir ci-dessus)
3. Commandes essayées
4. Environnement (OS, versions)
5. RUNBOOK consulté: quelle section?

---

## 📚 Glossaire

| Terme | Définition |
|-------|-----------|
| **DAG** | Directed Acyclic Graph — pipeline Airflow |
| **TaskFlow** | API moderne pour DAGs (Airflow 3.0+) |
| **PythonOperator** | Opérateur pour exécuter des fonctions Python |
| **XCom** | Mécanisme de communication entre tâches |
| **DLQ** | Dead Letter Queue — événements invalides |
| **UPSERT** | INSERT OR UPDATE en SQL |
| **Idempotence** | Même résultat peu importe le nombre d'exécutions |

---

## ✨ Résumé Phase 1

```
INCIDENTS RENCONTRÉS: 3
INCIDENTS RÉSOLUS:    3 ✅
INCIDENTS ACTIFS:     0
TEMPS TOTAL:          ~3.5 heures

LEÇONS APPRISES:
1. Toujours vérifier la version Airflow avant TaskFlow
2. Utiliser `python -m pytest`, pas juste `pytest`
3. Assouplir les versions Python (>=X.Y.Z,<X+1)

PRÉVENTION:
✅ Checklist pré-déploiement
✅ Tests automatisés
✅ Gestion des dépendances rigoureuse
```

---

**Last updated:** 3 juin 2026  
**Version:** 2.0.0

**Fin du RUNBOOK Phase 1**  
---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec minio mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spark-master | grep "checkpoint"
```

**Résolution :**
→ À compléter par votre groupe

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...


## Issue #16 — Vérification exactly-once

### Objectif

Vérifier que la chaîne Producteur Kafka → Spark Structured Streaming → PostgreSQL ne génère pas de doublons après un arrêt et redémarrage du job Spark.

### Configuration attendue

Producteur Kafka :

- `enable_idempotence=True`
- `acks='all'`
- `transactional_id='p2p-simulator-1'`

Consommateur Spark :

- `kafka.isolation.level=read_committed`
- checkpoint Spark conservé sur MinIO

### Procédure de test

1. Lancer le simulateur Kafka :

```bash
python -m src.p2p_simulator.simulator --kafka --peers 10 --rate 3