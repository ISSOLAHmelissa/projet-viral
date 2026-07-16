"""Scenario RELAY - regie publicitaire programmatique (inspire Criteo).
"""
import random

from . import common

NAME = "relay"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 600

TOPIC_IMPRESSIONS = "relay.impressions"
TOPIC_CLICKS = "relay.clicks"
TOPIC_CAMPAIGNS = "relay.campaigns"

TOPICS = {
    TOPIC_IMPRESSIONS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_CLICKS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_CAMPAIGNS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
