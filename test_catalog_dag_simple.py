import sys
sys.path.insert(0, 'dags')

from catalog_ingestion_pipeline import dag

def test_dag_loads():
    """Vérifier que la DAG se charge sans erreur"""
    assert dag is not None
    assert dag.dag_id == 'catalog_ingestion_pipeline'

def test_dag_has_5_tasks():
    """Vérifier que la DAG a 5 tâches"""
    assert len(dag.tasks) == 5
    task_ids = [t.task_id for t in dag.tasks]
    assert 'extract_from_minio' in task_ids
    assert 'validate_schema' in task_ids
    assert 'transform_catalog' in task_ids
    assert 'load_to_postgres' in task_ids
    assert 'notify_success' in task_ids

if __name__ == '__main__':
    test_dag_loads()
    test_dag_has_5_tasks()
    print("✅ All tests passed!")
