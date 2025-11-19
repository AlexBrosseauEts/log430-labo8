"""
Order event producer
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import config
from logger import Logger
from singleton import Singleton


class OrderEventProducer(metaclass=Singleton):
    """Kafka producer pour les événements de la saga de commandes."""

    def __init__(self):
        self.logger = Logger.get_instance("OrderEventProducer")
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_HOST,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            self.logger.debug(f"KafkaProducer initialisé sur {config.KAFKA_HOST}")
        except NoBrokersAvailable as e:
            self.logger.error(f"Kafka indisponible à l'initialisation : {e}")
            self.producer = None
        except Exception as e:
            self.logger.error(f"Erreur à l'initialisation de KafkaProducer : {e}")
            self.producer = None

    def get_instance(self):
        """Conserve la compatibilité avec le pattern Singleton utilisé ailleurs."""
        return self

    def send(self, topic: str, value: dict):
        """Envoie un événement sur Kafka, ou loggue simplement si Kafka est indisponible."""
        if self.producer is None:
            self.logger.warning(
                f"KafkaProducer non initialisé, événement ignoré. topic={topic}, value={value}"
            )
            return

        try:
            self.producer.send(topic, value=value)
            self.producer.flush()
            self.logger.debug(f"Événement envoyé sur Kafka topic={topic} : {value}")
        except Exception as e:
            self.logger.error(f"Erreur lors de l'envoi vers Kafka : {e}")
