"""Scenario BINGE - plateforme de video a la demande (inspire Netflix).
"""
import random
from . import common

NAME = "binge"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "binge.playback.events"
TOPIC_CATALOG = "binge.catalog"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_CATALOG: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
