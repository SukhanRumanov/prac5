import os
import asyncio
from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import aio_pika

from database import get_async_session, Product, PriceHistory, create_tables
from rabbitmq import RabbitMQ
from logger import setup_logger

logger = setup_logger(__name__)

load_dotenv()

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

rabbitmq = RabbitMQ()


class ProductCreate:
    def __init__(self, url: str):
        self.url = url


class ProductResponse:
    def __init__(self, id: int, url: str, name: Optional[str] = None,
                 description: Optional[str] = None, rating: Optional[float] = None,
                 created_at: datetime = None):
        self.id = id
        self.url = url
        self.name = name
        self.description = description
        self.rating = rating
        self.created_at = created_at


class PriceHistoryResponse:
    def __init__(self, id: int, product_id: int, price: float, created_at: datetime):
        self.id = id
        self.product_id = product_id
        self.price = price
        self.created_at = created_at


class PriceUpdateResponse:
    def __init__(self, product_id: int, product_name: str, old_price: Optional[float] = None,
                 new_price: float = None, timestamp: str = None):
        self.product_id = product_id
        self.product_name = product_name
        self.old_price = old_price
        self.new_price = new_price
        self.timestamp = timestamp


class ProductStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_product_id_delete = State()
    waiting_for_product_id_info = State()
    waiting_for_product_id_history = State()


def main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/add_product"), KeyboardButton(text="/products")],
            [KeyboardButton(text="/delete_product"), KeyboardButton(text="/product_info")],
            [KeyboardButton(text="/price_history"), KeyboardButton(text="/last_updates")]
        ],
        resize_keyboard=True
    )
    return keyboard


async def get_db():
    async for session in get_async_session():
        yield session


@router.message(Command("start"))
async def cmd_start(message: Message):
    logger.info(f"Пользователь {message.from_user.id} запустил бота")
    await message.answer(
        "Бот для мониторинга цен MVideo\n\n"
        "Доступные команды:\n"
        "/add_product - добавить товар для отслеживания\n"
        "/delete_product - удалить товар\n"
        "/products - список всех товаров\n"
        "/product_info - информация о товаре\n"
        "/price_history - история цен товара\n"
        "/last_updates - последние изменения цен",
        reply_markup=main_keyboard()
    )


@router.message(Command("add_product"))
async def add_product_start(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} начал добавление товара")
    await message.answer("Введите URL товара MVideo:")
    await state.set_state(ProductStates.waiting_for_url)


@router.message(ProductStates.waiting_for_url)
async def add_product_finish(message: Message, state: FSMContext):
    url = message.text.strip()
    logger.info(f"Пользователь {message.from_user.id} отправил URL для добавления: {url}")

    if not url.startswith(('https://www.mvideo.ru/', 'http://www.mvideo.ru/')):
        logger.warning(f"Пользователь {message.from_user.id} отправил неверный URL: {url}")
        await message.answer("Неверный URL")
        return

    product_create = ProductCreate(url=url)

    async for db in get_db():
        try:
            result = await db.execute(select(Product).where(Product.url == product_create.url))
            existing_product = result.scalar_one_or_none()

            if existing_product:
                logger.warning(f"Попытка добавить уже существующий товар: {url}")
                await message.answer("Этот товар уже отслеживается")
                await state.clear()
                return

            db_product = Product(url=product_create.url)
            db.add(db_product)
            await db.commit()
            await db.refresh(db_product)

            product_response = ProductResponse(
                id=db_product.id,
                url=db_product.url,
                created_at=db_product.created_at
            )

            await rabbitmq.publish("new_products", {
                "product_id": db_product.id,
                "url": db_product.url
            })

            logger.info(f"Товар успешно добавлен с ID: {db_product.id}, URL: {db_product.url}")

            await message.answer(
                f"Товар успешно добавлен!\n"
                f"ID: {product_response.id}\n"
                f"URL: {product_response.url}\n"
                f"Добавлен: {product_response.created_at}"
            )

        except Exception as e:
            logger.error(f"Ошибка при добавлении товара: {str(e)}")
            await message.answer(f"Ошибка при добавлении товара: {str(e)}")
        finally:
            await state.clear()


@router.message(Command("delete_product"))
async def delete_product_start(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} начал удаление товара")
    await message.answer("Введите ID товара для удаления:")
    await state.set_state(ProductStates.waiting_for_product_id_delete)


@router.message(ProductStates.waiting_for_product_id_delete)
async def delete_product_finish(message: Message, state: FSMContext):
    try:
        product_id = int(message.text.strip())
        logger.info(f"Пользователь {message.from_user.id} запросил удаление товара с ID: {product_id}")
    except ValueError:
        logger.warning(f"Пользователь {message.from_user.id} ввел неверный формат ID: {message.text}")
        await message.answer("Неверный формат ID. Введите число.")
        return

    async for db in get_db():
        try:
            result = await db.execute(select(Product).where(Product.id == product_id))
            product = result.scalar_one_or_none()

            if not product:
                logger.warning(f"Попытка удалить несуществующий товар с ID: {product_id}")
                await message.answer("Товар не найден")
                await state.clear()
                return

            await db.delete(product)
            await db.commit()

            logger.info(f"Товар с ID: {product_id} успешно удален пользователем {message.from_user.id}")

            await message.answer(f"Товар {product_id} успешно удален")

        except Exception as e:
            logger.error(f"Ошибка при удалении товара: {str(e)}")
            await message.answer(f"Ошибка при удалении товара: {str(e)}")
        finally:
            await state.clear()


@router.message(Command("products"))
async def get_products_list(message: Message):
    logger.info(f"Пользователь {message.from_user.id} запросил список товаров")

    async for db in get_db():
        try:
            result = await db.execute(select(Product))
            products = result.scalars().all()

            if not products:
                logger.info("Запрос списка товаров - товаров нет")
                await message.answer("Нет отслеживаемых товаров")
                return

            response = "Список отслеживаемых товаров:\n\n"
            for product in products:
                product_response = ProductResponse(
                    id=product.id,
                    url=product.url,
                    name=product.name,
                    description=product.description,
                    rating=product.rating,
                    created_at=product.created_at
                )

                response += (
                    f"ID: {product_response.id}\n"
                    f"URL: {product_response.url}\n"
                )

                if product_response.name:
                    response += f"Название: {product_response.name}\n"

                if product_response.rating:
                    response += f"Рейтинг: {product_response.rating}\n"

                response += f"Добавлен: {product_response.created_at}\n\n"

            logger.info(f"Возвращено {len(products)} товаров пользователю {message.from_user.id}")

            if len(response) > 4096:
                for i in range(0, len(response), 4096):
                    await message.answer(response[i:i + 4096])
            else:
                await message.answer(response)

        except Exception as e:
            logger.error(f"Ошибка при получении списка товаров: {str(e)}")
            await message.answer(f"Ошибка при получении списка товаров: {str(e)}")


@router.message(Command("product_info"))
async def get_product_info_start(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} запросил информацию о товаре")
    await message.answer("Введите ID товара для получения информации:")
    await state.set_state(ProductStates.waiting_for_product_id_info)


@router.message(ProductStates.waiting_for_product_id_info)
async def get_product_info_finish(message: Message, state: FSMContext):
    try:
        product_id = int(message.text.strip())
        logger.info(f"Пользователь {message.from_user.id} запросил информацию о товаре с ID: {product_id}")
    except ValueError:
        logger.warning(f"Пользователь {message.from_user.id} ввел неверный формат ID: {message.text}")
        await message.answer("Неверный формат ID. Введите число.")
        return

    async for db in get_db():
        try:
            result = await db.execute(select(Product).where(Product.id == product_id))
            product = result.scalar_one_or_none()

            if not product:
                logger.warning(f"Запрос информации о несуществующем товаре с ID: {product_id}")
                await message.answer("Товар не найден")
                await state.clear()
                return

            product_response = ProductResponse(
                id=product.id,
                url=product.url,
                name=product.name,
                description=product.description,
                rating=product.rating,
                created_at=product.created_at
            )

            response = f"Информация о товаре:\n\n"
            response += f"ID: {product_response.id}\n"
            response += f"URL: {product_response.url}\n"

            if product_response.name:
                response += f"Название: {product_response.name}\n"

            if product_response.description:
                desc = product_response.description
                if len(desc) > 100:
                    desc = desc[:100] + "..."
                response += f"Описание: {desc}\n"

            if product_response.rating:
                response += f"Рейтинг: {product_response.rating}\n"

            response += f"Добавлен: {product_response.created_at}\n"

            logger.info(f"Информация о товаре {product_id} отправлена пользователю {message.from_user.id}")

            await message.answer(response)

        except Exception as e:
            logger.error(f"Ошибка при получении информации о товаре: {str(e)}")
            await message.answer(f"Ошибка при получении информации о товаре: {str(e)}")
        finally:
            await state.clear()


@router.message(Command("price_history"))
async def get_price_history_start(message: Message, state: FSMContext):
    logger.info(f"Пользователь {message.from_user.id} запросил историю цен")
    await message.answer("Введите ID товара для получения истории цен:")
    await state.set_state(ProductStates.waiting_for_product_id_history)


@router.message(ProductStates.waiting_for_product_id_history)
async def get_price_history_finish(message: Message, state: FSMContext):
    try:
        product_id = int(message.text.strip())
        logger.info(f"Пользователь {message.from_user.id} запросил историю цен товара с ID: {product_id}")
    except ValueError:
        logger.warning(f"Пользователь {message.from_user.id} ввел неверный формат ID: {message.text}")
        await message.answer("Неверный формат ID. Введите число.")
        return

    async for db in get_db():
        try:
            result = await db.execute(select(Product).where(Product.id == product_id))
            product = result.scalar_one_or_none()

            if not product:
                logger.warning(f"Запрос истории цен несуществующего товара с ID: {product_id}")
                await message.answer("Товар не найден")
                await state.clear()
                return

            result = await db.execute(
                select(PriceHistory)
                .where(PriceHistory.product_id == product_id)
                .order_by(PriceHistory.created_at.desc())
            )
            prices = result.scalars().all()

            if not prices:
                logger.info(f"Для товара {product_id} нет истории цен")
                await message.answer(f"Для товара {product_id} нет истории цен")
                await state.clear()
                return

            product_response = ProductResponse(
                id=product.id,
                url=product.url,
                name=product.name,
                created_at=product.created_at
            )

            response = f"История цен для товара {product_id}"
            if product_response.name:
                response += f" - {product_response.name}"
            response += ":\n\n"

            for price in prices:
                price_response = PriceHistoryResponse(
                    id=price.id,
                    product_id=price.product_id,
                    price=price.price,
                    created_at=price.created_at
                )
                response += f"{price_response.price} руб. - {price_response.created_at}\n"

            logger.info(f"Отправлена история цен для товара {product_id}, записей: {len(prices)}")

            if len(response) > 4096:
                for i in range(0, len(response), 4096):
                    await message.answer(response[i:i + 4096])
            else:
                await message.answer(response)

        except Exception as e:
            logger.error(f"Ошибка при получении истории цен: {str(e)}")
            await message.answer(f"Ошибка при получении истории цен: {str(e)}")
        finally:
            await state.clear()


@router.message(Command("last_updates"))
async def get_last_price_updates(message: Message):
    logger.info(f"Пользователь {message.from_user.id} запросил последние обновления цен")

    async for db in get_db():
        try:
            result = await db.execute(
                select(PriceHistory, Product)
                .join(Product, PriceHistory.product_id == Product.id)
                .order_by(PriceHistory.created_at.desc())
                .limit(10)
            )
            records = result.all()

            if not records:
                logger.info("Нет данных об изменениях цен")
                await message.answer("Нет данных об изменениях цен")
                return

            response = "Последние изменения цен:\n\n"

            for price_history, product in records:
                price_update = PriceUpdateResponse(
                    product_id=product.id,
                    product_name=product.name or "Без названия",
                    new_price=price_history.price,
                    timestamp=price_history.created_at.strftime("%Y-%m-%d %H:%M:%S")
                )

                response += (
                    f"Товар: {price_update.product_id} - {price_update.product_name}\n"
                    f"Цена: {price_update.new_price} руб.\n"
                    f"Время: {price_update.timestamp}\n\n"
                )

            logger.info(f"Отправлены последние обновления цен, записей: {len(records)}")

            await message.answer(response)

        except Exception as e:
            logger.error(f"Ошибка при получении обновлений цен: {str(e)}")
            await message.answer(f"Ошибка при получении обновлений цен: {str(e)}")


async def wait_for_rabbitmq(url: str, max_retries: int = 30, retry_delay: int = 5):
    logger.info(f"Ожидание подключения к RabbitMQ: {url}")

    for attempt in range(max_retries):
        try:
            connection = await aio_pika.connect_robust(url)
            await connection.close()
            logger.info("RabbitMQ готов к работе")
            return True
        except Exception as e:
            logger.warning(f"RabbitMQ еще не готов (попытка {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Ждем {retry_delay} секунд...")
                await asyncio.sleep(retry_delay)

    raise Exception("Не удалось подключиться к RabbitMQ после всех попыток")


async def on_startup():
    logger.info("Запуск бота для мониторинга цен")
    await create_tables()
    logger.info("База данных инициализирована")

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://rabbitmq:5672/")
    await wait_for_rabbitmq(rabbitmq_url)

    await rabbitmq.connect(rabbitmq_url)
    logger.info("Бот запущен и готов к работе")


async def on_shutdown():
    logger.info("Завершение работы бота")
    await rabbitmq.close()
    logger.info("Соединение с RabbitMQ закрыто")


async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("Запуск поллинга бота")
    await dp.start_polling(bot)


if __name__ == "__main__":
    logger.info("Запуск приложения бота")
    asyncio.run(main())