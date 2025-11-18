"""
Outbox processor
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from datetime import datetime
import requests
import config
from db import get_sqlalchemy_session
from logger import Logger
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import modify_order
from payments.models.outbox import Outbox

class OutboxProcessor():
    """Process items in the outbox"""
    
    def __init__(self):
        """Constructor method"""
        self.logger = Logger.get_instance("OutboxProcessor")

    def run(self, outbox_item=None):
        """ 
        Run the processor. If you pass an item to it, the processor will process it right away.
        Otherwise, it will try to fetch all pending items from the database and process them.
        If processing is successful, the processor will update the payment_id in every item, effectively marking it as processed.
        """
        self.logger.debug("Start run")
        if outbox_item:
            self.logger.debug("item informed")
            event_data = self._get_event_data(outbox_item)
            self._process_outbox_item(event_data, outbox_item)
        else:
            self.logger.debug("no item informed")
            session = get_sqlalchemy_session()
            outbox_items = session.query(Outbox).filter(Outbox.payment_id.is_(None)).all()
            for outbox_item in outbox_items:
                event_data = self._get_event_data(outbox_item)
                self._process_outbox_item(event_data, outbox_item)
            if not outbox_items:
                self.logger.info("No outbox items to process.")
            else:
                self.logger.info(f"{len(outbox_items)} outbox items processed.")
            session.close()

        def _process_outbox_item(self, event_data, outbox_item):
        """Processes a single outbox item and changes the saga state based on result"""
        session = get_sqlalchemy_session()
        event_data['event'] = "PaymentCreated"
        try:
            payment_response = self._request_payment_transaction(outbox_item)

            if payment_response.ok:
                # On essaie de lire le JSON
                try:
                    data = payment_response.json()
                    payment_id = data.get('payment_id', 1)
                except Exception:
                    # Si le JSON est vide ou invalide, on met un ID par défaut
                    payment_id = 1
            else:
                # Statut HTTP pas OK -> échec
                raise Exception(f"Error {payment_response.status_code} : {payment_response.text}")

            # Met à jour la table Outbox
            order = session.query(Outbox).filter(Outbox.order_id == outbox_item.order_id).first()
            order.payment_id = payment_id
            session.commit()

            # Met à jour la commande (is_paid, payment_id, payment_link)
            update_succeeded = modify_order(event_data["order_id"], True, payment_id)
            event_data["payment_link"] = f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            if not update_succeeded:
                raise Exception("Erreur : la mise à jour de la commande après la génération du paiement a échoué.")
        except Exception as e:
            session.rollback()
            self.logger.debug("La création d'une transaction de paiement a échoué (2) : " + str(e))
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
        finally:
            session.close()
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)


    def _request_payment_transaction(self, outbox_item):
        """Request payment transaction to Payments API"""
        order_data = {
            "user_id": outbox_item.user_id,
            "order_id": outbox_item.order_id,
            "total_amount": outbox_item.total_amount
        }
        payment_response = requests.post('http://api-gateway:8080/payments-api/payments',
            json=order_data,
            headers={'Content-Type': 'application/json'}
        )
        return payment_response
      
    def _get_event_data(self, outbox_item):
        return {
            "user_id": outbox_item.user_id,
            "order_id": outbox_item.order_id,
            "total_amount": outbox_item.total_amount,
            "order_items": outbox_item.order_items,
            'is_paid': False,
            'payment_link': 'no-link',
            'datetime': str(datetime.now())
        }
