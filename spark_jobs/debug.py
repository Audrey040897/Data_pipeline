import psycopg2
import time

# On utilise les identifiants définis dans ton docker-compose
try:
    conn = psycopg2.connect(
        host="127.0.0.1", 
        port=5432, 
        database="airflow", 
        user="airflow", 
        password="airflow"
    )
    print("✅ CONNEXION RÉUSSIE avec airflow/airflow !")
    conn.close()
except Exception as e:
    print(f"❌ ÉCHEC : {e}")