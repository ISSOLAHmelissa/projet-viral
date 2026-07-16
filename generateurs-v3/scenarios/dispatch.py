"""Scenario DISPATCH - VTC / mobilite urbaine (inspire Uber).

"""
from . import common

NAME = "dispatch"
DEFAULT_RATE = 12.0
CATALOG_REFRESH = 10 ** 9

TOPIC_REQUESTS = "dispatch.ride.requests"
TOPIC_LOCATIONS = "dispatch.driver.locations"
TOPIC_TRIPS = "dispatch.trip.events"

TOPICS = {
    TOPIC_REQUESTS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_LOCATIONS: {"partitions": 6, "config": {"retention.ms": "43200000"}},
    TOPIC_TRIPS: {"partitions": 3, "config": {"retention.ms": "86400000"}},
}
