# SPOTIFY — Plateforme de streaming musical distribuée

>## 📊 Spotify Data Pipeline — Phase 1
 
**Status:** Phase 1 ✅ COMPLETED

---

## 📑 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Installation](#installation)
5. [Utilisation](#utilisation)
6. [Structure du projet](#structure-du-projet)
7. [Pipelines](#pipelines)
8. [Testing](#testing)
9. [Troubleshooting](#troubleshooting)
10. [Ressources](#ressources)

---

## 🎯 Vue d'ensemble

### Mission
Construire un **pipeline de données temps réel** pour ingérer, valider, transformer et distribuer les métadonnées musicales de Spotify à travers une architecture cloud-native.

### Objectifs Phase 1
- ✅ Ingestion du catalogue musical (MinIO → PostgreSQL)
- ✅ Streaming des événements d'écoute (Redis → PostgreSQL)
- ✅ Tests unitaires et validation
- ✅ Documentation complète
- ✅ Gestion des erreurs (DLQ)

### Résultats Phase 1
```
✅ 5 DAGs implémentés
✅ 34 tests (0 failed)
✅ 3 incidents documentés et résolus
✅ 100% code coverage des transformations
```

---

## 🏗️ Architecture

### Flux global

```
┌─────────────────────────────────────────────────────────────┐
│                    SPOTIFY DATA PIPELINE                    │
└─────────────────────────────────────────────────────────────┘

LAYER 1: SOURCES
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   MinIO      │  │    Redis     │  │ PostgreSQL   │
│ (Catalogs)   │  │   (Events)   │  │ (Dimension)  │
└──────────────┘  └──────────────┘  └──────────────┘
       ↓                 ↓                  ↓
       
LAYER 2: INGESTION (Airflow DAGs)
┌─────────────────────────────────────────────────────────────┐
│  catalog_ingestion_pipeline        streaming_events_pipeline│
│  ├─ Extract (MinIO)                 ├─ Consume (Redis)      │
│  ├─ Validate                        ├─ Validate             │
│  ├─ Transform                       ├─ Enrich               │
│  ├─ Load (PostgreSQL)               ├─ Store (Parquet)      │
│  └─ Notify                          └─ Load (PostgreSQL)    │
└─────────────────────────────────────────────────────────────┘
       ↓                                    ↓
       
LAYER 3: STORAGE
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  PostgreSQL      │  │   MinIO Parquet  │  │  Redis Cache     │
│  ├─ artists      │  │  ├─ listening/   │  │  (Session data)  │
│  ├─ albums       │  │  ├─ aggregated/  │  │                  │
│  ├─ tracks       │  │  └─ archived/    │  │                  │
│  ├─ events       │  │                  │  │                  │
│  └─ dlq_events   │  │                  │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘
       ↓
       
LAYER 4: ANALYTICS (Phase 2)
┌─────────────────────────────────────────────────────────────┐
│  Aggregation, Recommendation, Analysis                      │
└─────────────────────────────────────────────────────────────┘
```

### Composants clés

| Composant | Rôle | Port |
|-----------|------|------|
| **Airflow** | Orchestration des DAGs | 8080 |
| **PostgreSQL** | Data Warehouse | 5432 |
| **MinIO** | Object Storage (S3-like) | 9001 |
| **Redis** | Message Queue + Cache | 6379 |
| **Docker Compose** | Infrastructure locale | - |

---

## 💻 Stack technique

### Versions
```
Python:           3.13.12
Airflow:          2.9.0
PostgreSQL:       15.0
Redis:            7.0
MinIO:            latest
Docker Compose:   3.8+
```

### Dépendances principales
```python
# Core
apache-airflow==2.9.0
apache-airflow-providers-postgres
apache-airflow-providers-redis

# Data Processing
pandas>=1.5.0
faker>=15.0.0
boto3>=1.26.0

# Storage
psycopg2-binary>=2.9.0
redis>=4.0.0

# Testing
pytest>=8.0.0
pytest-cov>=4.0.0
```

---

## 📦 Installation

### Prérequis
```bash
# Vérifier les versions
python --version          # → 3.13.x
docker --version          # → 20.10+
docker compose --version  # → 2.0+
```

### Étape 1: Cloner et installer
```bash
# Cloner le repo
git clone <repo-url>
cd Data_pipeline

# Créer l'environnement virtuel
python3.13 -m venv env
source env/bin/activate  # Linux/Mac
# ou: env\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### Étape 2: Démarrer l'infrastructure
```bash
# Démarrer tous les conteneurs
docker compose up -d

# Vérifier que tout est running
docker compose ps

# Résultat attendu:
# STATUS: Up (tous les services)
```

### Étape 3: Initialiser Airflow
```bash
# La DB Airflow est auto-créée au premier démarrage
# Attendre ~1 minute

# Vérifier les logs
docker compose logs airflow-scheduler | tail -20

# Chercher: "Scheduler started"
```

### Étape 4: Accéder aux interfaces
```
Airflow UI:    http://localhost:8080
MinIO Console: http://localhost:9001
PostgreSQL:    localhost:5432 (user: spotify)
Redis:         localhost:6379
```

---

## 🚀 Utilisation

### Lancer les tests

```bash
# Set PYTHONPATH
export PYTHONPATH=$(pwd)

# Tous les tests (34 tests)
pytest tests/ -v --tb=short

# Juste les tests unitaires
pytest tests/unit/ -v

# Juste les tests structure
pytest tests/structure/ -v

# Avec rapport de couverture
pytest tests/ --cov=src --cov-report=html
```

### Vérifier les DAGs

```bash
# Compiler les DAGs
python -m py_compile dags/*.py

# Lancer un DAG manuellement (backfill)
docker compose exec airflow-scheduler airflow dags trigger \
  catalog_ingestion_pipeline \
  --exec-date 2025-06-01

# Voir les logs du DAG
docker compose logs airflow-scheduler | grep "catalog_ingestion_pipeline"
```

### Inspecter la base de données

```bash
# Se connecter à PostgreSQL
docker compose exec postgres psql -U spotify spotify

# Commandes utiles
SELECT COUNT(*) FROM artists;        -- Nombre d'artistes
SELECT COUNT(*) FROM tracks;         -- Nombre de tracks
SELECT * FROM dead_letter_events;    -- Erreurs
\dt                                  -- Lister les tables
\d artists                           -- Schéma de la table
```

### Vérifier MinIO

```bash
# Lister les buckets
docker compose exec minio aws s3 ls --endpoint-url http://minio:9000

# Lister les fichiers dans un bucket
docker compose exec minio aws s3 ls s3://labels-raw --endpoint-url http://minio:9000
```

---

## 📂 Structure du projet

```
Data_pipeline/
│
├── dags/                          # Airflow DAGs
│   ├── catalog_ingestion_pipeline.py
│   ├── streaming_events_pipeline.py
│   ├── aggregation_pipeline.py
│   ├── recommendation_pipeline.py
│   └── dlq_reprocessing_pipeline.py
│
├── src/                           # Code source
│   ├── transformations/
│   │   ├── catalog.py            # Normalisation, déduplication
│   │   └── events.py             # Validation, enrichissement
│   ├── data_generator/
│   │   └── generate_catalog.py   # Générateur de données
│   └── p2p_simulator/
│       └── p2p_events.py         # Simulation P2P
│
├── tests/                         # Tests
│   ├── structure/
│   │   └── test_dag_structure.py  # 16 tests structure
│   ├── unit/
│   │   └── test_transformations.py # 18 tests unitaires
│   └── integration/               # (Phase 2)
│
├── docs/                          # Documentation
│   ├── RUNBOOK.md                # Incidents et résolutions
│   ├── ARCHITECTURE.md           # Architecture détaillée
│   └── TESTING.md                # Guide des tests
│
├── sql/                           # Scripts SQL
│   └── schema.sql                # Schéma des tables
│
├── docker-compose.yml            # Configuration Docker
├── requirements.txt              # Dépendances Python
└── README.md                     # Ce fichier
```

---

## 📊 Pipelines

### 1. catalog_ingestion_pipeline

**Objectif:** Ingérer les métadonnées musicales

```
MinIO (JSON) → Extract → Validate → Transform → PostgreSQL
                                  ↓
                            DLQ (errors)
```

**Horaire:** Quotidien à 02:00 UTC  
**Idempotence:** ✅ ON CONFLICT DO UPDATE  
**Statut:** ✅ PRODUCTION

**Tables affectées:**
- `artists` (UPSERT par name, label)
- `albums` (UPSERT par id)
- `tracks` (UPSERT par id)

### 2. streaming_events_pipeline

**Objectif:** Traiter les événements d'écoute temps réel

```
Redis → Consume → Validate → Enrich → Store (Parquet) → PostgreSQL
                      ↓
                  DLQ (errors)
```

**Horaire:** Toutes les 5 minutes  
**Batch size:** 1000 événements  
**Statut:** ✅ PRODUCTION

**Tables affectées:**
- `listening_events`
- `p2p_network_events`

---

## 🧪 Testing

### Stratégie de test

```
Level 1: Unit Tests (18 tests)
├─ Normalisation des données
├─ Validation de schéma
├─ Déduplication
└─ Génération de données

Level 2: Structure Tests (16 tests)
├─ Import des DAGs
├─ Présence des tags
├─ Présence du doc_md
└─ Dépendances entre tâches

Level 3: Integration Tests (Phase 2)
├─ Streaming end-to-end
├─ PostgreSQL → MinIO
└─ Load testing
```

### Couverture

```
src/transformations/catalog.py   : 100% ✅
src/transformations/events.py    : 100% ✅
dags/                            : Structure ✅

Total: 34 tests, 0 failed
```

---

## 🔧 Troubleshooting

### DAG disparaît de l'UI

**Symptôme:** Le DAG n'apparaît pas dans Airflow  
**Cause:** Erreur de compilation  
**Solution:**
```bash
python -m py_compile dags/your_dag.py
docker compose logs airflow-scheduler | grep ERROR
```

Voir: [RUNBOOK.md](docs/RUNBOOK.md) - Incident #1

### Tests échouent avec erreur DB

**Symptôme:** `sqlite3.OperationalError: no such table`  
**Cause:** Mauvaise commande pytest  
**Solution:**
```bash
export PYTHONPATH=$(pwd)
python -m pytest tests/ -v
```

Voir: [RUNBOOK.md](docs/RUNBOOK.md) - Incident #2

### Dépendances incompatibles

**Symptôme:** `ImportError: cannot import name X`  
**Cause:** Version mismatch  
**Solution:**
```bash
pip install -r requirements.txt --upgrade
pip check
```

Voir: [RUNBOOK.md](docs/RUNBOOK.md) - Incident #3

---

## 📚 Ressources

### Documentation officielle
- [Apache Airflow](https://airflow.apache.org/docs/)
- [PostgreSQL](https://www.postgresql.org/docs/)
- [Redis](https://redis.io/documentation)
- [MinIO](https://docs.min.io/)

### Guides internes
- [RUNBOOK.md](docs/RUNBOOK.md) — Incidents et résolutions
- [TESTS_IMPLEMENTATION.md](docs/TESTS_IMPLEMENTATION.md) — Guide des tests
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — Architecture détaillée

### Commandes utiles

```bash
# Redémarrer Airflow
docker compose restart airflow-scheduler

# Voir les logs
docker compose logs -f airflow-scheduler

# Reset complet (⚠️ efface les données!)
docker compose down && docker volume prune

# Vérifier la santé
docker compose ps
docker stats
```

---

## 📋 Checklist Phase 1

- [x] Tous les DAGs compilent
- [x] Tous les tests passent (34/34)
- [x] DAGs ont tags et doc_md
- [x] Transformations testées
- [x] RUNBOOK documenté
- [x] README finalisé
- [x] Architecture documentée
- [x] Incidents résolus

---

## 🚀 Prochaines étapes (Phase 2)

- Kafka KRaft cluster (Ticket #11)
- Advanced aggregations (Ticket #12-15)
- Recommendation engine (Ticket #16-18)
- Real-time analytics (Ticket #19-20)

---

**Contact:** Groupe-K HETIC  
**Last updated:** 3 juin 2026  
**Version:** 1.0.0

**La Phase 2 (Kafka + Spark) commencera mercredi.** Vous avez une fondation solide. 🚀

---

> **Made by Groupe-K**  
> M1 Data & IA — HETIC  
> 02 Juin 2026
### Phase 2 — Streaming & Temps Réel (Mercredi PM + Jeudi, ~10h)

Faire évoluer la stack vers le temps réel avec Kafka et Spark.

**Issues à fermer : #11 → #20**

```
#11 Cluster Kafka KRaft dans docker-compose
#12 Migration simulateur P2P → Kafka (+ Redis maintenu)
#13 Premier job Spark : lecture topics, affichage console
#14 Job streaming_trends_job (fenêtres temporelles)
#15 Watermarking + gestion late events
#16 Exactly-once semantics bout-en-bout
#17 Job streaming_enrichment_job (jointures stream-static)
#18 Job fraud_detection_job (stateful, flatMapGroupsWithState)
#19 DAG reconciliation_pipeline (pont batch ↔ streaming)
#20 DAG late_events_reprocessing
```

**Critères de validation Phase 2 :**
- [ ] Les 3 jobs Spark tournent en continu
- [ ] Les tendances temps réel se mettent à jour en quelques secondes
- [ ] La détection de fraude génère des alertes correctes
- [ ] Après arrêt/relance Spark, reprise sans perte ni doublon
- [ ] Les agrégats batch et streaming convergent
- [ ] Les late events sont routés et retraités par Airflow

---

### Phase 3 — Interconnexion inter-groupes (Vendredi matin, ~3h)

Connecter votre instance aux instances des autres groupes.

**Issues à fermer : #21 → #25**

```
#21 Data contracts inter-groupes (formats communs)
#22 DAG catalog_federation_pipeline
#23 P2P cross-group (topics partagés)
#24 Top 50 Global SPOTIFY (agrégation cross-group)
#25 Chaos engineering + documentation finale
```

**Critères de validation Phase 3 :**
- [ ] Catalogue fédéré contient les tracks des autres groupes
- [ ] Au moins un transfert P2P cross-group fonctionne
- [ ] Le Top 50 Global agrège les données de tous les groupes
- [ ] Les données externes invalides partent en DLQ
- [ ] Un data contract documenté définit les formats inter-groupes

---


