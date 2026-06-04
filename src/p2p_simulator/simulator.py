"""
P2P Simulator — Configuration Exactly-Once
===========================================
Simulateur qui publie les événements d'écoute sur Kafka avec idempotence.

Configuration exactly-once :
    - enable.idempotence=True : déduplique les messages en cas de retry
    - acks=all : attend confirmation de tous les replicas
    - transactional.id : garantit l'unicité du producteur
    - compression.type=snappy : compresse les messages

Lancement :
    python -m src.p2p_simulator.simulator --kafka --peers 10 --rate 3
"""

import json
import uuid
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/1"
KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

from kafka import KafkaProducer
from kafka.errors import KafkaError


class P2PSimulator:
    """
    Simulateur de réseau P2P Spotify avec publication Kafka idempotente.
    """

    def __init__(self, peers: int = 10, rate: int = 3, use_kafka: bool = True):
    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Phase 2 — Kafka producer
        self.kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, 'acks': 'all', 'enable.idempotence': True})

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # Alterner listening et réseau P2P (80% / 20%)
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Args:
            peers: nombre de pairs P2P à simuler
            rate: nombre d'événements par seconde
            use_kafka: activer la publication Kafka (vs Redis pour Phase 1)
        """
        self.peers = peers
        self.rate = rate
        self.use_kafka = use_kafka
        
        # Initialiser le producteur Kafka avec configuration exactly-once
        if self.use_kafka:
            self.producer = self._create_kafka_producer()
        else:
            self.producer = None

    def _create_kafka_producer(self) -> KafkaProducer:
        """
        Crée un producteur Kafka avec configuration exactly-once.
        
        Configuration clés pour exactly-once :
            - enable.idempotence=True : les retries ne créent pas de doublons
            - acks=all : attend confirmation de tous les replicas (plus lent mais sûr)
            - retries=3 : nombre de retries en cas d'erreur
            - max.in.flight.requests.per.connection=5 : limiter les requests en vol
        """
        return KafkaProducer(
            bootstrap_servers=['kafka-1:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Configuration exactly-once
            acks='all',                           # Attend tous les replicas
            retries=3,                            # Retry 3 fois en cas d'erreur
            max_in_flight_requests_per_connection=5,
            # Idempotence et transactions
            enable_idempotence=True,              # Déduplique les retries
            transactional_id="p2p-simulator-1",  # ID unique du producteur
            # Compression
            compression_type='snappy',            # Compresse les messages
            # Timeouts
            request_timeout_ms=30000,
            # Batching
            batch_size=16384,
            linger_ms=10,
        )

    def _generate_listening_event(self) -> Dict:
        """
        Génère un événement d'écoute avec event_id unique (crucial pour exactly-once).
        """
        now = datetime.utcnow()
        
        return {
            "event_id": str(uuid.uuid4()),  # ID unique — clé pour la dédoublonnage
            "user_id": f"user-{random.randint(1, 1000)}",
            "track_id": f"track-{random.randint(1, 10000)}",
            "source_peer": f"peer-{random.randint(0, self.peers-1)}",
            "timestamp": now.isoformat() + "Z",
            "duration_ms": random.randint(30000, 300000),  # 30s à 5min
            "device_type": random.choice(["mobile", "desktop", "smart_speaker"]),
            "geo_country": random.choice(["FR", "DE", "US", "GB", "ES"]),
            "completed": random.random() > 0.1,  # 90% complètes
            "event_source": "p2p",
        }

        if event_type == "chunk_transfer":
            event["target_peer"]    = random.choice(self.active_peers)
            event["chunk_size_kb"]  = random.randint(64, 512)
            event["track_id"]       = random.choice(SAMPLE_TRACKS)["id"]
        elif event_type in ("cache_hit", "cache_miss"):
            event["track_id"]       = random.choice(SAMPLE_TRACKS)["id"]
        elif event_type == "peer_connect":
            event["geo_country"]    = random.choice(GEO_COUNTRIES)

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        # Phase 2 — décommenter
        
        self._publish_to_kafka(channel, event.get("user_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """Publie dans les listes Redis consommées par Airflow."""
        try:
            # On utilise le préfixe 'queue:' pour correspondre à ton DAG
            queue_name = f"queue:{channel}"
            self.redis.lpush(queue_name, payload)
            # Optionnel : limiter la taille de la file pour éviter de saturer Redis
            self.redis.ltrim(queue_name, 0, 9999)
        except Exception as e:
            logger.error(f"Erreur Redis sur {channel}: {e}")

    def _publish_to_kafka(self, topic: str, key: str, payload: str):
            def delivery_report(err, msg):
                if err is not None:
                    logger.error(f"Erreur Kafka: {err}")
            
            self.kafka_producer.produce(topic, key=key, value=payload, callback=delivery_report)
            self.kafka_producer.poll(0)

        start_time = time.time()
        event_count = 0

        try:
            while time.time() - start_time < duration_seconds:
                # Générer et publier un événement
                event = self._generate_listening_event()
                
                if self.use_kafka:
                    self._publish_to_kafka(event)
                
                event_count += 1
                
                # Respecter le débit (rate événements/sec)
                time.sleep(1.0 / self.rate)

        except KeyboardInterrupt:
            print("\n⏹️  Simulateur arrêté")
        
        finally:
            # Flush les messages en attente
            if self.use_kafka and self.producer:
                self.producer.flush()
                self.producer.close()
            
            elapsed = time.time() - start_time
            print(f"\n📊 Statistiques :")
            print(f"   - Événements publiés : {event_count}")
            print(f"   - Durée : {elapsed:.1f}s")
            print(f"   - Débit réel : {event_count/elapsed:.2f} événements/sec")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="P2P Simulator for SPOTIFY")
    parser.add_argument("--kafka", action="store_true", help="Publier sur Kafka (vs Redis)")
    parser.add_argument("--peers", type=int, default=10, help="Nombre de peers")
    parser.add_argument("--rate", type=int, default=3, help="Événements par seconde")
    parser.add_argument("--duration", type=int, default=600, help="Durée en secondes")
    
    args = parser.parse_args()
    
    simulator = P2PSimulator(
        peers=args.peers,
        rate=args.rate,
        use_kafka=args.kafka
    )
    
    simulator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()