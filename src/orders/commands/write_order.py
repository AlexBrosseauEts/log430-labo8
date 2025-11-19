"""
Orders (write-only model)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from datetime import datetime
import json
import config
from logger import Logger
from orders.commands.order_event_producer import OrderEventProducer
from orders.models.order import Order
from stocks.models.product import Product
from orders.models.order_item import OrderItem
from db import get_sqlalchemy_session, get_redis_conn

logger = Logger.get_instance("add_order")


def add_order(user_id: int, items: list):
    """Insert order with items in MySQL, keep Redis in sync"""
    event_data = {'event': 'OrderCreationFailed'}
    session = get_sqlalchemy_session()

    try:
        if not items:
            raise ValueError("Cannot create order. An order must have 1 or more items.")

        logger.debug("Commencer : ajout de commande")

        # Charger les produits
        product_ids = [item['product_id'] for item in items]
        products_query = session.query(Product).filter(Product.id.in_(product_ids)).all()
        price_map = {product.id: product.price for product in products_query}

        # Calcul total + préparer OrderItems
        total_amount = 0
        order_items = []

        for item in items:
            pid = item["product_id"]
            qty = item["quantity"]

            if pid not in price_map:
                raise ValueError(f"Product ID {pid} not found in database.")

            unit_price = price_map[pid]
            total_amount += unit_price * qty

            order_items.append({
                'product_id': pid,
                'quantity': qty,
                'unit_price': unit_price
            })

        # Créer la commande (payment_link = "no-link")
        new_order = Order(user_id=user_id, total_amount=total_amount, payment_link="no-link")
        session.add(new_order)
        session.flush()

        order_id = new_order.id

        # Créer les items
        for item in order_items:
            order_item = OrderItem(
                order_id=order_id,
                product_id=item['product_id'],
                quantity=item['quantity'],
                unit_price=item['unit_price']
            )
            session.add(order_item)

        session.commit()
        logger.debug("Une commande a été ajouté")

        # Redis
        add_order_to_redis(order_id, user_id, total_amount, items, payment_link="no-link")

        # Événement OrderCreated
        event_data = {
            'event': 'OrderCreated',
            'order_id': order_id,
            'user_id': user_id,
            'total_amount': total_amount,
            'is_paid': False,
            'payment_link': "no-link",
            'order_items': items,
            'datetime': str(datetime.now())
        }

        return order_id

    except Exception as e:
        session.rollback()
        event_data['error'] = str(e)
        raise e

    finally:
        # Toujours envoyer l'événement
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        session.close()


def modify_order(order_id: int, is_paid: bool, payment_link: str) -> bool:
    """Update order in MySQL and Redis"""
    session = get_sqlalchemy_session()
    try:
        order = session.query(Order).filter(Order.id == order_id).first()

        if not order:
            logger.error(f"Order {order_id} not found")
            return False

        order.is_paid = is_paid
        order.payment_link = payment_link
        session.commit()

        # Mise à jour Redis
        r = get_redis_conn()
        r.hset(
            f"order:{order_id}",
            mapping={
                "payment_link": payment_link,
                "is_paid": int(is_paid)
            }
        )

        logger.debug(
            f"Order {order_id} updated: is_paid={is_paid}, payment_link={payment_link}"
        )
        return True

    except Exception as e:
        session.rollback()
        logger.error(f"Error updating order {order_id}: {e}")
        return False

    finally:
        session.close()


def delete_order(order_id: int):
    """Delete order in MySQL, keep Redis in sync"""
    session = get_sqlalchemy_session()
    try:
        order = session.query(Order).filter(Order.id == order_id).first()

        if not order:
            return 0

        # Supprimer les items
        items = session.query(OrderItem).filter(OrderItem.order_id == order_id).all()
        for item in items:
            session.delete(item)

        session.delete(order)
        session.commit()

        delete_order_from_redis(order_id)
        return 1

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def add_order_to_redis(order_id, user_id, total_amount, items, payment_link="no-link"):
    """Insert order to Redis"""
    r = get_redis_conn()
    r.hset(
        f"order:{order_id}",
        mapping={
            "user_id": user_id,
            "total_amount": float(total_amount),
            "items": json.dumps(items),
            "payment_link": payment_link
        }
    )


def delete_order_from_redis(order_id):
    """Delete order from Redis"""
    r = get_redis_conn()
    r.delete(f"order:{order_id}")
