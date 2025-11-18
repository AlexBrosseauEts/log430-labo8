"""
Handler: Stock Decreased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer


class StockDecreasedHandler(EventHandler):
    """Handles StockDecreased events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        """
        Ici, on suppose que la transaction de paiement a été créée
        (ou sera créée par un autre service) et on poursuit la saga.
        """
        try:
            # Si la transaction de paiement a été créée, déclenchez PaymentCreated.
            event_data['event'] = "PaymentCreated"
            self.logger.debug(f"payment_link={event_data.get('payment_link', 'no-link')}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        except Exception as e:
            # Si la transaction de paiement n'était pas créée, déclenchez l'événement adéquat.
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
