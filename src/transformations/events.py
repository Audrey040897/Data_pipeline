# src/transformations/events.py

"""
Transformations des événements d'écoute
======================================
Validation et enrichissement des événements utilisateurs.
"""

import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def is_valid_listening_event(event: Dict) -> bool:
    """
    Valide un événement d'écoute.
    
    Champs obligatoires:
        - event_id: str (UUID)
        - user_id: str (UUID)
        - track_id: str (UUID)
        - timestamp: str (ISO format, pas dans le futur)
        - duration_ms: int (> 5000 pour pas un bot)
        - completed: bool
    
    Détection de fraude:
        - duration_ms < 5000 → pattern bot (skipping rapidement)
        - timestamp dans le futur → impossible
        - Pas de user_id → invalide
    
    Args:
        event: Dictionnaire d'événement
    
    Returns:
        True si l'événement est valide, False sinon
    """
    # Champs obligatoires
    required = ['event_id', 'user_id', 'track_id', 'timestamp', 'duration_ms']
    for field in required:
        if field not in event:
            logger.warning(f"Missing field: {field}")
            return False
    
    # Vérifier que timestamp est pas dans le futur
    try:
        # Parser le timestamp (avec timezone)
        ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        
        # Utiliser timezone-aware pour la comparaison
        from datetime import timezone
        now = datetime.now(timezone.utc)
        
        if ts > now:
            logger.warning(f"Timestamp in future: {ts}")
            return False
    except ValueError as e:
        logger.warning(f"Invalid timestamp format: {event['timestamp']}")
        return False
    
    # Détection de pattern bot (listening < 5 secondes)
    duration_ms = event.get('duration_ms', 0)
    if duration_ms < 5000:
        logger.warning(f"Bot pattern: duration < 5s ({duration_ms}ms)")
        return False
    
    return True


def enrich_listening_event(event: Dict, artist_name: Optional[str] = None) -> Dict:
    """
    Enrichit un événement d'écoute avec des données additionnelles.
    
    Ajoute:
        - artist_name (si fourni)
        - is_bot: détection de pattern bot
        - event_date: date extraite du timestamp
    
    Args:
        event: Événement brut
        artist_name: Nom de l'artiste (optionnel)
    
    Returns:
        Événement enrichi
    """
    enriched = event.copy()
    
    if artist_name:
        enriched['artist_name'] = artist_name
    
    # Ajouter le flag bot
    duration_ms = event.get('duration_ms', 0)
    enriched['is_bot'] = duration_ms < 5000
    
    # Extraire la date
    try:
        ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        enriched['event_date'] = ts.strftime('%Y-%m-%d')
    except ValueError:
        enriched['event_date'] = None
    
    return enriched