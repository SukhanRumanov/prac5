import aio_pika
import json
import asyncio
from typing import Optional, Callable, Any


class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self, url: str, max_retries: int = 10, retry_delay: int = 5):
        for attempt in range(max_retries):
            try:
                self.connection = await aio_pika.connect_robust(url)
                self.channel = await self.connection.channel()

                await self.channel.declare_queue("new_products", durable=True)
                await self.channel.declare_queue("price_updates", durable=True)

                print(f"Successfully connected to RabbitMQ (attempt {attempt + 1})")
                return

            except Exception as e:
                print(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise Exception(f"Failed to connect to RabbitMQ after {max_retries} attempts: {e}")

    async def publish(self, queue_name: str, message: dict):
        if self.channel is None:
            raise Exception("Not connected to RabbitMQ")

        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=queue_name
            )
        except Exception as e:
            print(f"Failed to publish message to {queue_name}: {e}")
            raise

    async def consume(self, queue_name: str, callback: Callable[[aio_pika.IncomingMessage], Any]):
        if self.channel is None:
            raise Exception("Not connected to RabbitMQ")

        try:
            queue = await self.channel.declare_queue(queue_name, durable=True)
            await queue.consume(callback)
            print(f"Started consuming from queue: {queue_name}")
        except Exception as e:
            print(f"Failed to start consuming from {queue_name}: {e}")
            raise

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("RabbitMQ connection closed")