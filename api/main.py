from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from typing import List
import os
import uvicorn

from database import get_async_session, Product, PriceHistory, create_tables
from rabbitmq import RabbitMQ
import models

from logger import setup_logger

logger = setup_logger(__name__)
app = FastAPI(
    title="Price Monitor API",
    description="API для мониторинга цен товаров MVideo",
    version="1.0.0"
)

rabbitmq = RabbitMQ()


@app.on_event("startup")
async def startup_event():
    logger.info("Запуск сервиса мониторинга цен")
    await create_tables()
    logger.info("База данных инициализирована")

    await rabbitmq.connect(
        os.getenv("RABBITMQ_URL", "amqp://rabbitmq:5672/"),
        max_retries=10,
        retry_delay=5
    )
    logger.info("Подключение к RabbitMQ установлено")
    logger.info("API сервис запущен и готов к работе")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Завершение работы сервиса мониторинга цен")
    await rabbitmq.close()
    logger.info("Соединение с RabbitMQ закрыто")


@app.post("/products/", response_model=models.ProductResponse, summary="Добавить товар")
async def add_product(
        product: models.ProductCreate,
        db: AsyncSession = Depends(get_async_session)
):
    logger.info(f"Попытка добавления нового товара с URL: {product.url}")

    result = await db.execute(select(Product).where(Product.url == product.url))
    existing_product = result.scalar_one_or_none()

    if existing_product:
        logger.warning(f"Попытка добавить уже существующий товар: {product.url}")
        raise HTTPException(status_code=400, detail="Товар уже отслеживается")

    db_product = Product(url=product.url)
    db.add(db_product)
    await db.commit()
    await db.refresh(db_product)

    logger.info(f"Товар успешно добавлен в базу данных с ID: {db_product.id}")

    await rabbitmq.publish("new_products", {
        "product_id": db_product.id,
        "url": db_product.url
    })

    logger.info(f"Сообщение о новом товаре отправлено в RabbitMQ для ID: {db_product.id}")

    return db_product


@app.delete("/products/{product_id}", summary="Удалить товар")
async def delete_product(
        product_id: int,
        db: AsyncSession = Depends(get_async_session)
):
    logger.info(f"Запрос на удаление товара с ID: {product_id}")

    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        logger.warning(f"Попытка удалить несуществующий товар с ID: {product_id}")
        raise HTTPException(status_code=404, detail="Товар не найден")

    await db.delete(product)
    await db.commit()

    logger.info(f"Товар с ID: {product_id} успешно удален из базы данных")

    return {"message": f"Товар {product_id} удален"}


@app.get("/products/", response_model=List[models.ProductResponse], summary="Список товаров")
async def get_products(db: AsyncSession = Depends(get_async_session)):
    logger.info("Запрос на получение списка всех товаров")

    result = await db.execute(select(Product))
    products = result.scalars().all()

    logger.info(f"Возвращено {len(products)} товаров")

    return products


@app.get("/products/{product_id}/prices", response_model=List[models.PriceHistoryResponse], summary="История цен")
async def get_price_history(
        product_id: int,
        db: AsyncSession = Depends(get_async_session)
):
    logger.info(f"Запрос истории цен для товара с ID: {product_id}")

    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        logger.warning(f"Запрос истории цен для несуществующего товара с ID: {product_id}")
        raise HTTPException(status_code=404, detail="Товар не найден")

    result = await db.execute(
        select(PriceHistory)
        .where(PriceHistory.product_id == product_id)
        .order_by(PriceHistory.created_at.desc())
    )
    prices = result.scalars().all()

    logger.info(f"Возвращено {len(prices)} записей истории цен для товара с ID: {product_id}")

    return prices


@app.get("/products/{product_id}", response_model=models.ProductResponse, summary="Информация о товаре")
async def get_product(
        product_id: int,
        db: AsyncSession = Depends(get_async_session)
):
    logger.info(f"Запрос информации о товаре с ID: {product_id}")

    result = await db.execute(select(Product).where(Product.id == product_id))
    product = result.scalar_one_or_none()

    if not product:
        logger.warning(f"Запрос информации о несуществующем товаре с ID: {product_id}")
        raise HTTPException(status_code=404, detail="Товар не найден")

    logger.info(f"Информация о товаре с ID: {product_id} успешно возвращена")

    return product


@app.get("/health", summary="Проверка здоровья сервиса")
async def health_check():
    logger.info("Проверка здоровья сервиса")
    return {"status": "healthy", "service": "price-monitor-api"}


if __name__ == "__main__":
    logger.info("Запуск сервера Uvicorn")
    uvicorn.run(app, host="0.0.0.0", port=8000)