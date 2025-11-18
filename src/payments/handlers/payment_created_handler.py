"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.models.order import Order


class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""

    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()

    def get_event_type(self) -> str:
        return "PaymentCreated"

    def handle(self, event_data: Dict[str, Any]) -> None:
        """
        Le paiement a été créé : on met à jour la commande
        avec le payment_link et on termine la saga (SagaCompleted).
        """
        session = get_sqlalchemy_session()

        try:
            order_id = event_data["order_id"]

            # On reconstruit/garantit un payment_link
            payment_id = event_data.get("payment_id", 1)
            payment_link = event_data.get(
                "payment_link",
                f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            )
            event_data["payment_link"] = payment_link

            # Mise à jour de la commande en BD
            order = session.query(Order).filter(Order.id == order_id).first()
            if order:
                order.is_paid = True
                order.payment_link = payment_link
                session.commit()
            else:
                raise Exception(f"Commande {order_id} introuvable pour mise à jour.")

            # Saga terminée avec succès
            event_data["event"] = "SagaCompleted"
            self.logger.debug(f"payment_link={event_data['payment_link']}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)

        except Exception as e:
            session.rollback()
            # Si ça casse, on termine quand même la saga mais avec erreur
            event_data["event"] = "SagaCompleted"
            event_data["error"] = str(e)
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        finally:
            session.close()
