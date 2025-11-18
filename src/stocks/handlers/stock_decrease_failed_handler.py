"""
Handler: Stock Decrease Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from db import get_sqlalchemy_session
from payments.models.outbox import Outbox
from payments.outbox_processor import OutboxProcessor

class StockDecreaseFailedHandler(EventHandler):
    """Handles StockDecreaseFailed events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreaseFailed"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        # Ici, on compense en annulant la commande.
        try:
            # Si l'operation a réussi, déclenchez OrderCancelled.
            event_data['event'] = "OrderCancelled"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        except Exception as e:
            # Si l'operation a échoué, continuez la compensation des étapes précedentes.
            event_data['event'] = "OrderCreationFailed"
            event_data['error'] = str(e)
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
