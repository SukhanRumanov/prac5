import aio_pika
import json


class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self, url: str):
        self.connection = await aio_pika.connect_robust(url)
        self.channel = await self.connection.channel()

        await self.channel.declare_queue("new_products", durable=True)
        await self.channel.declare_queue("price_updates", durable=True)

    async def publish(self, queue_name: str, message: dict):
        if self.channel is None:
            raise Exception("Not connected to RabbitMQ")

        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=queue_name
        )

    async def consume(self, queue_name: str, callback):
        if self.channel is None:
            raise Exception("Not connected to RabbitMQ")

        queue = await self.channel.declare_queue(queue_name, durable=True)
        await queue.consume(callback)

    async def close(self):
        if self.connection:
            await self.connection.close()