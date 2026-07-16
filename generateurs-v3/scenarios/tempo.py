"""Scenario TEMPO - streaming musical (inspire Spotify).

"""
import random
from . import common

NAME = "tempo"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "tempo.listening.events"
TOPIC_TRACKS = "tempo.tracks"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_TRACKS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
