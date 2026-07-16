"""Scenario ARENA - jeu de tir competitif en ligne (inspire Riot Games).
"""
import random

from . import common

NAME = "arena"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 600

TOPIC_EVENTS = "arena.match.events"
TOPIC_PURCHASES = "arena.purchases"
TOPIC_PLAYERS = "arena.players"

TOPICS = {
    TOPIC_EVENTS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_PURCHASES: {"partitions": 3, "config": {"retention.ms": "86400000"}},
    TOPIC_PLAYERS: {"partitions": 3, "config": {"cleanup.policy": "compact"}},
}
