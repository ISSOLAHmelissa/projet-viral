"""Scenario FORGE - supervision d'usine / maintenance predictive
(inspire des plateformes type Siemens MindSphere).

"""
import random

from . import common

NAME = "forge"
DEFAULT_RATE = 8.0            # pieces produites par seconde
CATALOG_REFRESH = 600

TOPIC_READINGS = "forge.readings"
TOPIC_PRODUCTION = "forge.production"
TOPIC_MACHINES = "forge.machines"

TOPICS = {
    TOPIC_READINGS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_PRODUCTION: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_MACHINES: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
