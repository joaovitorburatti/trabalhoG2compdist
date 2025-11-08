import asyncio
import aio_pika


class RabbitBridge:
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None

    async def connect(self):
        """Conecta ao RabbitMQ e cria o canal."""
        if self.connection and not self.connection.is_closed:
            return

        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)
        print("[RABBIT] Conectado com sucesso!")

    async def publish(self, queue_name: str, message: str):
        """Publica uma mensagem na fila especificada."""
        if not self.channel:
            await self.connect()

        queue = await self.channel.declare_queue(queue_name, durable=True)
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key=queue.name
        )

    async def consume(self, queue_name: str, callback):
        """Consome mensagens da fila e executa o callback para cada uma."""
        if not self.channel:
            await self.connect()

        queue = await self.channel.declare_queue(queue_name, durable=True)

        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                body = message.body.decode()
                await callback(body)

        await queue.consume(on_message)
        print(f"[RABBIT] Consumindo mensagens da fila '{queue_name}'...")

    async def close(self):
        """Fecha conexão de forma segura."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            print("[RABBIT] Conexão encerrada.")
