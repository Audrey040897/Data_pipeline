# src/transformations/catalog.py

"""
Transformations du catalogue musical
===================================
Normalisation des noms d'artistes, validation de schéma, déduplication.
"""

import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    """
    Normalise un nom d'artiste.
    
    Règles :
        - Strip les espaces avant/après
        - Title case (première lettre majuscule)
        - Préserve les caractères spéciaux (accents, apostrophes)
    
    Args:
        name: Nom de l'artiste (peut être None)
    
    Returns:
        Nom normalisé ou None si input est None
    
    Exemples:
        >>> normalize_artist_name("  the beatles  ")
        'The Beatles'
        >>> normalize_artist_name("björk")
        'Björk'
        >>> normalize_artist_name(None)
        None
    """
    if name is None:
        return None
    
    # Strip et title case
    return name.strip().title()


def validate_track_schema(track: Dict) -> List[str]:
    """
    Valide le schéma d'une track.
    
    Champs obligatoires:
        - id: str (UUID)
        - artist_id: str (UUID)
        - title: str (non-vide)
        - duration_ms: int (0 < duration < 3_600_000)
    
    Args:
        track: Dictionnaire représentant une track
    
    Returns:
        Liste des erreurs (vide = valide)
    
    Exemples:
        >>> validate_track_schema({"id": "123", "artist_id": "456", 
        ...                        "title": "Song", "duration_ms": 210000})
        []  # ✅ Valide
        
        >>> validate_track_schema({"id": "123"})
        ['Missing: artist_id', 'Missing: title', 'Missing: duration_ms']  # ❌ Invalide
    """
    errors = []
    
    # Vérifier les champs obligatoires
    required_fields = ['id', 'artist_id', 'title', 'duration_ms']
    for field in required_fields:
        if field not in track:
            errors.append(f"Missing: {field}")
    
    # Si des champs manquent, arrêter là
    if errors:
        return errors
    
    # Vérifier la duration
    duration = track.get('duration_ms')
    if not isinstance(duration, int):
        errors.append(f"duration_ms must be int, got {type(duration)}")
    elif duration <= 0 or duration > 3_600_000:  # Max 1 heure
        errors.append(f"duration_ms must be 0 < d <= 3600000, got {duration}")
    
    # Vérifier que title n'est pas vide
    title = track.get('title', '')
    if not title or not str(title).strip():
        errors.append("title must be non-empty")
    
    return errors


def deduplicate_artists(artists: List[Dict]) -> List[Dict]:
    """
    Déduplique les artistes par (name, label).
    
    Deux artistes avec le même nom ET label = doublon → garder le premier.
    Deux artistes avec le même nom mais labels différents = OK, garder les deux.
    
    Args:
        artists: Liste d'artistes
    
    Returns:
        Liste dédupliquée (ordre préservé)
    
    Exemples:
        >>> artists = [
        ...     {"id": "1", "name": "Beatles", "label": "EMI"},
        ...     {"id": "2", "name": "beatles", "label": "EMI"},  # doublon (après normalize)
        ...     {"id": "3", "name": "Beatles", "label": "Atlantic"},  # OK, label différent
        ... ]
        >>> len(deduplicate_artists(artists))
        2  # Beatles/EMI (1 copy) + Beatles/Atlantic
    """
    seen = set()
    result = []
    
    for artist in artists:
        # Clé de déduplication: (name normalisé, label)
        key = (
            normalize_artist_name(artist.get('name', '')),
            artist.get('label', '')
        )
        
        if key not in seen:
            seen.add(key)
            result.append(artist)
        else:
            logger.debug(f"Skipping duplicate artist: {key}")
    
    return result


def deduplicate_tracks(tracks: List[Dict]) -> List[Dict]:
    """
    Déduplique les tracks par (id, title).
    
    Deux tracks avec le même ID = doublon → garder le premier.
    
    Args:
        tracks: Liste de tracks
    
    Returns:
        Liste dédupliquée
    """
    seen = set()
    result = []
    
    for track in tracks:
        key = track.get('id')
        
        if key not in seen:
            seen.add(key)
            result.append(track)
    
    return result