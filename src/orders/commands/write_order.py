"""
Orders (write-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from datetime import datetime
import json
import requests
import config
from logger import Logger
from orders.commands.order_event_producer import OrderEventProducer
from orders.models.order import Order
from stocks.models.product import Product
from sqlalchemy.exc import SQLAlchemyError
from orders.models.order_item import OrderItem
from db import get_sqlalchemy_session, get_redis_conn

logger = Logger.get_instance("add_order")

def add_order(user_id: int, items: list):
    """Insert order with items in MySQL, keep Redis in sync"""
    event_data = {'event': 'OrderCreationFailed'} 
    try:
        if not items:
            raise ValueError("Cannot create order. An order must have 1 or more items.")

        product_ids = [item['product_id'] for item in items]
        session = get_sqlalchemy_session()

        logger.debug("Commencer : ajout de commande")
        products_query = session.query(Product).filter(Product.id.in_(product_ids)).all()
        price_map = {product.id: product.price for product in products_query}
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

        # NOTE: Au départ, il n'y a pas de lien de paiement. 
        # Il sera généré par StockDecreasedHandler et ensuite il sera intégré à la commande par PaymentCreatedHandler.
        new_order = Order(user_id=user_id, total_amount=total_amount, payment_link="no-link")
        session.add(new_order)
        session.flush()   

        order_id = new_order.id
        session.flush()  
        
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

        add_order_to_redis(order_id, user_id, total_amount, items)

        # Déclencher l'événement OrderCreated
        event_data = {'event': 'OrderCreated', 
                        'order_id': new_order.id, 
                        'user_id': new_order.user_id,
                        'total_amount': new_order.total_amount,
                        'is_paid': new_order.is_paid,
                        'payment_link': new_order.payment_link,
                        'order_items': items,
                        'datetime': str(datetime.now())}
        return order_id

    except Exception as e:
        # Déclencher l'événement OrderCreationFailed
        event_data['error'] = str(e)
        session.rollback()
        raise e
    finally:
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        session.close()

def modify_order(order_id: int, is_paid: bool, payment_link: str) -> bool:
    session = get_sqlalchemy_session()
    try:
        order = session.query(Order).filter(Order.id == order_id).first()
        if not order:
            logger.error(f"Order {order_id} not found")
            return False

        order.is_paid = is_paid
        order.payment_link = payment_link

        session.commit()
        logger.debug(
            f"Order {order_id} updated: is_paid={order.is_paid}, payment_link={order.payment_link}"
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
        if order:
            # MySQL
            session.query(OrderItem).filter(OrderItem.order_id == order_id).all()
            session.delete(order)
            session.commit()

            # Redis
            delete_order_from_redis(order_id)
            return 1  
        else:
            return 0  
            
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def add_order_to_redis(order_id, user_id, total_amount, items, payment_link=""):
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