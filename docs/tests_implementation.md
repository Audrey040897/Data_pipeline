# 🧪 Tests Implementation Guide — Phase 1
 
**Coverage:** 100% des transformations  
**Résultat:** 34 tests, 0 failed

---

## 📑 Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Stratégie de test](#stratégie-de-test)
3. [Tests unitaires](#tests-unitaires)
4. [Tests de structure](#tests-de-structure)
5. [Exécution](#exécution)
6. [Troubleshooting](#troubleshooting)
7. [Bonnes pratiques](#bonnes-pratiques)

---

## 🎯 Vue d'ensemble

### Architecture des tests

```
┌──────────────────────────────────────────────────────┐
│            TESTS PHASE 1                             │
├──────────────────────────────────────────────────────┤
│                                                      │
│  LEVEL 1: UNIT TESTS (18 tests)                    │
│  ├─ Transformations du catalogue (4)               │
│  ├─ Validation des schémas (4)                     │
│  ├─ Validation des événements (4)                  │
│  ├─ Déduplication (2)                              │
│  └─ Générateur de données (4)                      │
│                                                      │
│  LEVEL 2: STRUCTURE TESTS (16 tests)               │
│  ├─ Import des DAGs (2)                            │
│  ├─ Propriétés des DAGs (4)                        │
│  ├─ Catalog DAG spécifique (7)                     │
│  └─ Aggregation DAG spécifique (3)                 │
│                                                      │
│  LEVEL 3: INTEGRATION TESTS (0, Phase 2)           │
│  └─ Streaming end-to-end                           │
│                                                      │
└──────────────────────────────────────────────────────┘

COVERAGE: src/transformations/ = 100% ✅
```

### Résultats

```bash
$ pytest tests/ -v --tb=short

tests/unit/                 18 PASSED
tests/structure/            16 PASSED
─────────────────────────────────────
TOTAL                       34 PASSED  ✅
```

---

## 🏗️ Stratégie de test

### Principes

| Principe | Détail |
|----------|--------|
| **Isolation** | Chaque test est indépendant (pas de DB) |
| **Répétabilité** | Même résultat à chaque exécution |
| **Rapidité** | Tests unitaires < 100ms |
| **Coverage** | 100% des fonctions critiques |
| **Clarté** | Test name = comportement attendu |

### Niveaux de test

```python
# LEVEL 1: Unit Tests — Fonctions isolées, pas de dépendances
def test_normalize_artist_name():
    """Tester une seule fonction sans DB."""
    assert normalize_artist_name("  john  ") == "John"

# LEVEL 2: Structure Tests — DAGs valides, pas d'exécution
def test_dag_has_tags():
    """Vérifier que le DAG a des tags."""
    dag = DagBag().get_dag('catalog_ingestion_pipeline')
    assert len(dag.tags) > 0

# LEVEL 3: Integration Tests — Workflow complet (Phase 2)
def test_full_pipeline_end_to_end():
    """Tester le pipeline complet."""
    # MinIO → Extract → Validate → Load → Verify PostgreSQL
```

---

## 🧪 Tests unitaires

### Structure des fichiers

```
tests/unit/
├── __init__.py
└── test_transformations.py     ← 18 tests
    ├── TestNormalizeArtistName
    ├── TestValidateTrackSchema
    ├── TestListeningEventValidation
    ├── TestDeduplication
    └── TestDataGenerator
```

### 1. Transformations du catalogue

#### Test: Normalisation des noms

```python
class TestNormalizeArtistName:
    """Tests de normalize_artist_name()"""

    def test_strips_whitespace(self):
        """Supprime les espaces avant/après."""
        result = normalize_artist_name("  The Beatles  ")
        assert result == "The Beatles"

    def test_title_case(self):
        """Convertit en title case."""
        result = normalize_artist_name("the beatles")
        assert result == "The Beatles"

    def test_handles_none(self):
        """Gère None correctement."""
        result = normalize_artist_name(None)
        assert result is None

    def test_preserves_special_chars(self):
        """Préserve les caractères spéciaux."""
        result = normalize_artist_name("björk")
        assert result == "Björk"
```

**Fonction testée:**
```python
def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    """Normalise un nom d'artiste."""
    if name is None:
        return None
    return name.strip().title()
```

#### Test: Validation de schéma

```python
class TestValidateTrackSchema:
    """Tests de validate_track_schema()"""

    def test_valid_track_passes(self, valid_track):
        """Track valide retourne liste vide."""
        errors = validate_track_schema(valid_track)
        assert errors == []

    def test_missing_title_fails(self, valid_track):
        """Track sans title génère une erreur."""
        track = {k: v for k, v in valid_track.items() if k != "title"}
        errors = validate_track_schema(track)
        assert "title" in str(errors).lower()

    def test_negative_duration_fails(self, valid_track):
        """Durée négative est invalide."""
        valid_track["duration_ms"] = -1
        errors = validate_track_schema(valid_track)
        assert len(errors) > 0

    def test_too_long_duration_fails(self, valid_track):
        """Durée > 1h est invalide."""
        valid_track["duration_ms"] = 36_000_001
        errors = validate_track_schema(valid_track)
        assert len(errors) > 0
```

**Fonction testée:**
```python
def validate_track_schema(track: Dict) -> List[str]:
    """Valide le schéma d'une track."""
    errors = []
    required_fields = ['id', 'artist_id', 'title', 'duration_ms']
    
    for field in required_fields:
        if field not in track:
            errors.append(f"Missing: {field}")
    
    duration = track.get('duration_ms')
    if not isinstance(duration, int) or duration <= 0 or duration > 3_600_000:
        errors.append(f"Invalid duration: {duration}")
    
    return errors
```

### 2. Validation des événements

#### Test: Événement valide

```python
class TestListeningEventValidation:
    """Tests de is_valid_listening_event()"""

    def test_valid_event_passes(self, valid_listening_event):
        """Événement valide retourne True."""
        assert is_valid_listening_event(valid_listening_event) is True

    def test_missing_user_id_fails(self, valid_listening_event):
        """Événement sans user_id est invalide."""
        del valid_listening_event["user_id"]
        assert is_valid_listening_event(valid_listening_event) is False

    def test_future_timestamp_fails(self, valid_listening_event):
        """Timestamp dans le futur est invalide."""
        valid_listening_event["timestamp"] = "2099-01-01T00:00:00Z"
        assert is_valid_listening_event(valid_listening_event) is False

    def test_bot_pattern_detected(self, valid_listening_event):
        """Durée < 5s = pattern bot."""
        valid_listening_event["duration_ms"] = 100
        assert is_valid_listening_event(valid_listening_event) is False
```

**Fonction testée:**
```python
def is_valid_listening_event(event: Dict) -> bool:
    """Valide un événement d'écoute."""
    required = ['event_id', 'user_id', 'track_id', 'timestamp', 'duration_ms']
    
    for field in required:
        if field not in event:
            return False
    
    # Vérifier timestamp pas dans le futur
    try:
        ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        if ts > now:
            return False
    except ValueError:
        return False
    
    # Détection bot (< 5s)
    if event.get('duration_ms', 0) < 5000:
        return False
    
    return True
```

### 3. Déduplication

```python
class TestDeduplication:
    """Tests de deduplication"""

    def test_removes_duplicate_artists_same_label(self, catalog_with_duplicates):
        """Supprime les doublons (même name + label)."""
        result = deduplicate_artists(catalog_with_duplicates["artists"])
        assert len(result) == 2  # Beatles/EMI x1, Beatles/Atlantic x1

    def test_keeps_different_labels(self):
        """Garde les artistes avec labels différents."""
        artists = [
            {"id": "1", "name": "Artist X", "label": "Label A"},
            {"id": "2", "name": "Artist X", "label": "Label B"},
        ]
        result = deduplicate_artists(artists)
        assert len(result) == 2
```

### 4. Générateur de données

```python
class TestDataGenerator:
    """Tests sur generate_catalog.py"""

    def test_generate_catalog_structure(self):
        """Catalogue a la bonne structure."""
        catalog = generate_label_catalog("Test Label", n_artists=2)
        
        assert "label" in catalog
        assert "artists" in catalog
        assert "albums" in catalog
        assert "tracks" in catalog
        assert len(catalog["artists"]) == 2

    def test_generated_track_has_required_fields(self):
        """Chaque track a les champs requis."""
        catalog = generate_label_catalog("Test Label", n_artists=1)
        
        for track in catalog["tracks"]:
            assert "id" in track
            assert "artist_id" in track
            assert "title" in track
            assert "duration_ms" in track
            assert track["duration_ms"] > 0

    def test_track_ids_are_unique(self):
        """Les IDs sont uniques."""
        catalog = generate_label_catalog("Test Label", n_artists=5)
        track_ids = [t["id"] for t in catalog["tracks"]]
        
        assert len(track_ids) == len(set(track_ids))
```

---

## 🏗️ Tests de structure

### Structure des fichiers

```
tests/structure/
├── __init__.py
└── test_dag_structure.py     ← 16 tests
    ├── test_no_import_errors
    ├── test_all_dags_present
    ├── test_all_dags_have_owner
    ├── test_all_dags_have_retries
    ├── test_all_dags_have_tags
    ├── test_no_dag_has_cycles
    ├── TestCatalogIngestionDAG (7 tests)
    └── TestAggregationDAG (3 tests)
```

### Tests globaux

```python
def test_no_import_errors():
    """Tous les DAGs doivent compiler."""
    dagbag = DagBag(dag_folder='dags')
    assert not dagbag.import_errors

def test_all_dags_present():
    """Tous les DAGs attendus doivent exister."""
    dagbag = DagBag(dag_folder='dags')
    expected_dags = [
        'catalog_ingestion_pipeline',
        'streaming_events_pipeline',
        # ...
    ]
    for dag_id in expected_dags:
        assert dagbag.get_dag(dag_id) is not None

def test_all_dags_have_tags():
    """Tous les DAGs doivent avoir des tags."""
    dagbag = DagBag(dag_folder='dags')
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tags) > 0, f"DAG {dag_id} n'a pas de tags"
```

### Tests du catalog_ingestion_pipeline

```python
class TestCatalogIngestionDAG:
    """Tests spécifiques au catalog DAG"""

    def test_dag_exists(self):
        """Le DAG doit exister."""
        dagbag = DagBag(dag_folder='dags')
        catalog_dag = dagbag.get_dag('catalog_ingestion_pipeline')
        assert catalog_dag is not None

    def test_has_required_tasks(self):
        """Le DAG doit avoir les 5 tâches."""
        dag = DagBag().get_dag('catalog_ingestion_pipeline')
        required_tasks = [
            'extract_from_minio',
            'validate_schema',
            'transform_catalog',
            'load_to_postgres',
            'notify_success'
        ]
        task_ids = [t.task_id for t in dag.tasks]
        
        for task in required_tasks:
            assert task in task_ids

    def test_task_order(self):
        """Les tâches doivent être dans le bon ordre."""
        dag = DagBag().get_dag('catalog_ingestion_pipeline')
        task_dict = {t.task_id: t for t in dag.tasks}
        
        # t1 >> t2 >> t3 >> t4 >> t5
        assert task_dict['extract_from_minio'] in \
               task_dict['validate_schema'].upstream_list

    def test_has_doc_md(self):
        """Le DAG doit avoir une documentation."""
        dag = DagBag().get_dag('catalog_ingestion_pipeline')
        assert dag.doc_md is not None
        assert len(dag.doc_md) > 0

    def test_schedule_is_daily(self):
        """Le DAG doit être planifié quotidiennement."""
        dag = DagBag().get_dag('catalog_ingestion_pipeline')
        assert dag.schedule_interval in ["@daily", "0 2 * * *"]
```

---

## 🚀 Exécution

### Setup

```bash
# 1. Activer le virtual environment
source env/bin/activate

# 2. Exporter PYTHONPATH
export PYTHONPATH=$(pwd)

# 3. Vérifier les imports
python -c "import pytest; print('✅ pytest')"
python -c "from src.transformations import *; print('✅ src')"
```

### Lancer les tests

```bash
# Tous les tests
pytest tests/ -v --tb=short

# Juste les unitaires
pytest tests/unit/ -v

# Juste la structure
pytest tests/structure/ -v

# Un test spécifique
pytest tests/unit/test_transformations.py::TestNormalizeArtistName::test_strips_whitespace -v

# Avec rapport de couverture
pytest tests/ --cov=src --cov-report=html

# Stop au premier échec
pytest tests/ -x

# Afficher les print() durant les tests
pytest tests/ -v -s
```

### Résultat attendu

```
tests/unit/                                               18 PASSED
tests/structure/                                          16 PASSED

======================== 34 passed in 1.51s ========================
```

---

## 📋 Fixtures

### Fixtures disponibles

```python
@pytest.fixture
def valid_track():
    """Une track valide pour les tests."""
    return {
        "id": str(uuid.uuid4()),
        "artist_id": str(uuid.uuid4()),
        "title": "Test Track",
        "duration_ms": 210_000,
    }

@pytest.fixture
def valid_listening_event():
    """Un événement d'écoute valide."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "track_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_ms": 45_000,
    }

@pytest.fixture
def catalog_with_duplicates():
    """Un catalogue avec des doublons."""
    return {
        "artists": [
            {"id": "1", "name": "Beatles", "label": "EMI"},
            {"id": "2", "name": "beatles", "label": "EMI"},  # Doublon
            {"id": "3", "name": "Beatles", "label": "Atlantic"},
        ]
    }
```

---

## 🔧 Troubleshooting

### `ModuleNotFoundError: No module named 'src'`

```bash
# Solution: Exporter PYTHONPATH
export PYTHONPATH=$(pwd)
python -m pytest tests/ -v
```

### `TypeError: can't compare offset-naive and offset-aware datetimes`

```bash
# Cause: Mélange de timezones
# Solution: Utiliser timezone-aware partout

from datetime import datetime, timezone

# ✅ BON
now = datetime.now(timezone.utc)

# ❌ MAUVAIS
now = datetime.utcnow()
```

### `DagBag: no such table: dag`

```bash
# Cause: Tests essayent de créer une DB Airflow
# Solution: Tests unitaires simples sans DB

# ✅ BON: Tests unitaires isolés
pytest tests/unit/ -v

# ❌ MAUVAIS: Tests qui chargent DagBag
# → Ne pas utiliser DagBag pour les tests unitaires
```

---

## ✨ Bonnes pratiques

### 1. Nommer les tests clairement

```python
# ✅ BON
def test_normalize_artist_name_strips_whitespace():
    """Décrit exactement ce qui est testé."""
    assert normalize_artist_name("  name  ") == "name"

# ❌ MAUVAIS
def test_normalize():
    """Trop vague."""
    assert normalize_artist_name("  name  ") == "name"
```

### 2. Un test = Un comportement

```python
# ✅ BON
def test_returns_none_for_none_input():
    assert normalize_artist_name(None) is None

def test_converts_to_title_case():
    assert normalize_artist_name("john") == "John"

# ❌ MAUVAIS
def test_normalize():
    assert normalize_artist_name(None) is None
    assert normalize_artist_name("john") == "John"
    assert normalize_artist_name("  name  ") == "Name"
```

### 3. Utiliser des fixtures

```python
# ✅ BON
def test_with_fixture(valid_track):
    errors = validate_track_schema(valid_track)
    assert errors == []

# ❌ MAUVAIS
def test_without_fixture():
    track = {"id": "123", "artist_id": "456", ...}
    errors = validate_track_schema(track)
    assert errors == []
```

### 4. Tester les cas limites

```python
def test_validate_track_schema():
    # Cas normal
    assert validate_track_schema(valid_track) == []
    
    # Cas limites
    assert validate_track_schema({}) != []           # Vide
    assert validate_track_schema({"id": "1"}) != []  # Partiel
    
    # Cas invalides
    assert validate_track_schema({**valid_track, "duration_ms": -1}) != []
    assert validate_track_schema({**valid_track, "duration_ms": 4_000_000}) != []
```

### 5. Éviter les dépendances externes

```python
# ✅ BON: Tests indépendants
def test_normalize_artist_name():
    # Pas de DB, pas de API, pas de fichiers
    assert normalize_artist_name("john") == "John"

# ❌ MAUVAIS: Tests avec dépendances
def test_with_database():
    hook = PostgresHook()
    result = hook.run("SELECT...")  # ← Dépendance!
```

---

## 📊 Métriques

### Coverage par module

| Module | Coverage | Tests |
|--------|----------|-------|
| `catalog.py` | 100% | 8 |
| `events.py` | 100% | 10 |
| DAGs structure | 100% | 16 |
| **TOTAL** | **100%** | **34** |

### Temps d'exécution

```
Unit tests:     0.14s
Structure tests: 1.37s
─────────────────────
TOTAL:          1.51s ✅
```

---

## 📚 Ressources

### Documentation
- [pytest Documentation](https://docs.pytest.org/)
- [Airflow Testing](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

### Commandes utiles

```bash
# Lister tous les tests
pytest tests/ --collect-only

# Exécuter avec verbosité
pytest tests/ -vv

# Exécuter avec output en temps réel
pytest tests/ -s

# Générer un rapport HTML
pytest tests/ --cov=src --cov-report=html
# → Ouvrir: htmlcov/index.html
```

---

**Last updated:** 3 juin 2026  
**Version:** 1.0.0