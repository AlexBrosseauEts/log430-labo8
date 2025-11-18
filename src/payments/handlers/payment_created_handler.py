"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from db import get_sqlalchemy_session
from orders.models.order import Order


class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        return "PaymentCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        session = get_sqlalchemy_session()
        try:
            order = session.query(Order).filter(Order.id == event_data["order_id"]).first()
            if order is not None:
                order.payment_link = event_data.get("payment_link", "")
                session.commit()

            event_data['event'] = "SagaCompleted"
            self.logger.debug(f"payment_link={event_data.get('payment_link', '')}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        except Exception as e:
            session.rollback()
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        finally:
            session.close()
