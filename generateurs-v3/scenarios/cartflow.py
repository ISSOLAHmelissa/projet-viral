"""Scenario CARTFLOW - e-commerce (inspire Amazon).
"""
import random
from . import common

NAME = "cartflow"
DEFAULT_RATE = 5.0
CATALOG_REFRESH = 10 ** 9  # pas de topic compacte pour ce scenario

TOPIC_ORDERS = "cartflow.orders"
TOPIC_PAYMENTS = "cartflow.payments"
TOPIC_STOCK = "cartflow.stock.movements"

TOPICS = {
    TOPIC_ORDERS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_PAYMENTS: {"partitions": 6, "config": {"retention.ms": "86400000"}},
    TOPIC_STOCK: {"partitions": 3, "config": {"retention.ms": "86400000"}},
}
