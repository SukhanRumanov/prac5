import asyncio
import aio_pika
import json
from sqlalchemy import select
from datetime import datetime
import os

from database import AsyncSessionLocal, Product, PriceHistory, create_tables
from rabbitmq import RabbitMQ
from mvideo_parser import parse_mvideo_product
from logger import setup_logger

logger = setup_logger(__name__)

rabbitmq = RabbitMQ()


async def process_new_product(message: aio_pika.IncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        logger.info(f"Обработка нового товара: {data}")

        async with AsyncSessionLocal() as db:
            try:
                result = await db.execute(select(Product).where(Product.id == data["product_id"]))
                product = result.scalar_one_or_none()

                if product:
                    product_info = parse_mvideo_product(product.url)

                    if product_info and product_info.get("price"):
                        product.name = product_info["name"]
                        product.description = product_info.get("description", "")
                        product.rating = product_info.get("rating")

                        price_text = product_info["price"]
                        price_value = float(price_text.replace(' ', '').replace('₽', ''))

                        price_history = PriceHistory(
                            product_id=product.id,
                            price=price_value
                        )
                        db.add(price_history)

                        await db.commit()
                        logger.info(f"Товар {product.id} обработан: {product.name} - {price_value} руб")
                    else:
                        logger.error(f"Не удалось спарсить товар {product.id}")
                else:
                    logger.error(f"Товар {data['product_id']} не найден в базе")

            except Exception as e:
                logger.error(f"Ошибка обработки товара {data['product_id']}: {e}")
                await db.rollback()


async def monitor_prices():
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(select(Product))
            products = result.scalars().all()

            logger.info(f"Мониторинг цен для {len(products)} товаров")

            for product in products:
                try:
                    product_info = parse_mvideo_product(product.url)

                    if product_info and product_info.get("price"):
                        price_text = product_info["price"]
                        current_price = float(price_text.replace(' ', '').replace('₽', ''))

                        result = await db.execute(
                            select(PriceHistory)
                            .where(PriceHistory.product_id == product.id)
                            .order_by(PriceHistory.created_at.desc())
                        )
                        latest_price = result.scalar_one_or_none()

                        if not latest_price or latest_price.price != current_price:
                            new_price = PriceHistory(
                                product_id=product.id,
                                price=current_price
                            )
                            db.add(new_price)
                            await db.commit()

                            logger.info(f"Цена обновлена для '{product.name}': {current_price} руб")

                            await rabbitmq.publish("price_updates", {
                                "product_id": product.id,
                                "product_name": product.name,
                                "old_price": latest_price.price if latest_price else None,
                                "new_price": current_price,
                                "timestamp": datetime.utcnow().isoformat()
                            })
                        else:
                            logger.debug(f"Цена без изменений для '{product.name}': {current_price} руб")
                    else:
                        logger.error(f"Не удалось получить цену для товара {product.id}")

                except Exception as e:
                    logger.error(f"Ошибка мониторинга товара {product.id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка в мониторинге цен: {e}")


async def price_monitor_loop():
    while True:
        await monitor_prices()
        logger.info("Ожидание 1 час до следующей проверки цен")
        await asyncio.sleep(3600)


async def main():
    await create_tables()
    await rabbitmq.connect(
        os.getenv("RABBITMQ_URL", "amqp://rabbitmq:5672/"),
        max_retries=10,
        retry_delay=5
    )

    logger.info("Сервис мониторинга цен запущен")

    asyncio.create_task(
        rabbitmq.consume("new_products", process_new_product)
    )

    await price_monitor_loop()


if __name__ == "__main__":
    asyncio.run(main())