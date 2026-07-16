"""Scenario VIRAL - reseau social (inspire Instagram / Meta).

"""
from . import common

NAME = "viral"
DEFAULT_RATE = 15.0
CATALOG_REFRESH = 10 ** 9

TOPIC_POSTS = "viral.posts"
TOPIC_INTERACTIONS = "viral.interactions"

TOPICS = {
    TOPIC_POSTS: {"partitions": 3, "config": {"retention.ms": "86400000"}},
    TOPIC_INTERACTIONS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
}
