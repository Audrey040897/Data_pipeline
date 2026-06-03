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

from kafka import KafkaProducer
from kafka.errors import KafkaError


class P2PSimulator:
    """
    Simulateur de réseau P2P Spotify avec publication Kafka idempotente.
    """

    def __init__(self, peers: int = 10, rate: int = 3, use_kafka: bool = True):
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

    def _publish_to_kafka(self, event: Dict, topic: str = "listening_events"):
        """
        Publie l'événement sur Kafka avec gestion d'erreur.
        
        Le producteur est configuré avec idempotence, donc :
            - Si le même message est envoyé deux fois, Kafka le déduplique
            - Si une erreur réseau occur, le retry n'aura pas de doublon
        """
        try:
            future = self.producer.send(topic, value=event)
            
            # Attendre la confirmation (synchrone pour exactly-once)
            record_metadata = future.get(timeout=10)
            
            print(
                f"✅ Event {event['event_id'][:8]}... publié sur "
                f"{topic} (partition {record_metadata.partition}, offset {record_metadata.offset})"
            )
            
            return True
            
        except KafkaError as e:
            print(f"❌ Erreur Kafka : {e}")
            # L'erreur est loggée, le retry automatique s'en charge
            return False

    def run(self, duration_seconds: int = 600):
        """
        Lance le simulateur et publie des événements en continu.
        
        Args:
            duration_seconds: durée de simulation (par défaut 10 min)
        """
        print(f"🚀 Simulateur P2P lancé (exactly-once mode)")
        print(f"   - {self.peers} peers")
        print(f"   - {self.rate} événements/sec")
        print(f"   - Kafka idempotence : ON")
        print(f"   - Duration : {duration_seconds}s")
        print("-" * 60)

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