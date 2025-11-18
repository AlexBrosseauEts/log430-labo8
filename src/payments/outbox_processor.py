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
from orders.models.order import Order


class OutboxProcessor():
    """Process items in the outbox"""

    def __init__(self):
        self.logger = Logger.get_instance("OutboxProcessor")

    def run(self, outbox_item=None):
        """
        Run the processor. If you pass an item to it, the processor will process it right away.
        Otherwise, it will try to fetch all pending items from the database and process them.
        If processing is successful, the processor will update the payment_id in every item,
        effectively marking it as processed.
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

        try:
            payment_response = self._request_payment_transaction(outbox_item)

            if not payment_response.ok:
                self.logger.debug(
                    f"Payment API responded with {payment_response.status_code}, "
                    "on continue quand même pour le labo."
                )

            # On force un payment_id fixe pour le labo
            payment_id = 1

            # Met à jour la table Outbox
            outbox_row = session.query(Outbox).filter(
                Outbox.order_id == outbox_item.order_id
            ).first()
            if outbox_row:
                outbox_row.payment_id = payment_id

            # Met à jour la commande (is_paid + payment_id)
            update_succeeded = modify_order(event_data["order_id"], True, payment_id)

            # Crée et stocke un vrai payment_link
            payment_link = f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            event_data["payment_link"] = payment_link

            order = session.query(Order).filter(
                Order.id == outbox_item.order_id
            ).first()
            if order:
                order.payment_link = payment_link

            session.commit()

            if not update_succeeded:
                raise Exception(
                    "Erreur : la mise à jour de la commande après la génération du paiement a échoué."
                )

            event_data["event"] = "PaymentCreated"

        except Exception as e:
            session.rollback()
            self.logger.debug(
                "La création d'une transaction de paiement a échoué (2) : " + str(e)
            )
            event_data["event"] = "PaymentCreationFailed"
            event_data["error"] = str(e)

        finally:
            session.close()
            # 🔥 Nouveau : on évite que Kafka indisponible fasse crasher le container
            try:
                OrderEventProducer().get_instance().send(
                    config.KAFKA_TOPIC, value=event_data
                )
            except Exception as e:
                self.logger.debug(
                    f"Erreur lors de l'envoi de l'événement Kafka: {e}"
                )

    def _request_payment_transaction(self, outbox_item):
        """Request payment transaction to Payments API"""
        order_data = {
            "user_id": outbox_item.user_id,
            "order_id": outbox_item.order_id,
            "total_amount": outbox_item.total_amount
        }
        payment_response = requests.post(
            'http://api-gateway:8080/payments-api/payments',
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
            "is_paid": False,
            "payment_link": "",
            "datetime": str(datetime.now())
        }
