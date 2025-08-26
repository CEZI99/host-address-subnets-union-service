import asyncio
import aio_pika
import aiofiles
import sys
import uuid
import os
from proto.matcher_pb2 import MatchRequest, MatchResponse

class AsyncIPMatcherClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.futures = {}
        self.response_queue = None
        self.rabbit_user = os.getenv("RABBITMQ_USER")
        self.rabbit_pass = os.getenv("RABBITMQ_PASSWORD")

    async def connect(self):
        """Асинхронное подключение к RabbitMQ"""
        try:
            print("CONN HERE")
            print(f"amqp://{self.rabbit_user}:{self.rabbit_pass}@localhost/")
            self.connection = await aio_pika.connect_robust(
                f"amqp://{self.rabbit_user}:{self.rabbit_pass}@localhost/",
                timeout=10
            )
            self.channel = await self.connection.channel()
            
            # Объявляем очереди
            await self.channel.declare_queue('match_requests', durable=True)
            self.response_queue = await self.channel.declare_queue(
                'match_responses', durable=True
            )
            
            # Запускаем потребитель для ответов
            await self.response_queue.consume(self.on_response)
            
            print("Connected to RabbitMQ successfully")
            return True
            
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    async def on_response(self, message: aio_pika.IncomingMessage):
        """Обработка входящих ответов"""
        async with message.process():
            correlation_id = message.correlation_id
            if correlation_id in self.futures:
                self.futures[correlation_id].set_result(message.body)

    async def send_request(self, addresses, timeout=30):
        """Асинхронная отправка запроса"""
        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self.futures[correlation_id] = future

        # Создаем запрос
        request = MatchRequest()
        request.addresses.extend(addresses)

        # Отправляем запрос
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=request.SerializeToString(),
                correlation_id=correlation_id,
                reply_to=self.response_queue.name,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key='match_requests'
        )

        print(f"Sent request with {len(addresses)} addresses")

        try:
            # Ждем ответа с таймаутом
            response_body = await asyncio.wait_for(future, timeout=timeout)
            
            # Десериализуем ответ
            response = MatchResponse()
            response.ParseFromString(response_body)
            
            return response
            
        except asyncio.TimeoutError:
            print("Request timed out")
            raise
        finally:
            if correlation_id in self.futures:
                del self.futures[correlation_id]

    async def read_file_async(self, filename):
        """Асинхронное чтение файла"""
        try:
            async with aiofiles.open(filename, 'r') as f:
                content = await f.read()
                addresses = [
                    line.strip() for line in content.split('\n') 
                    if line.strip() and not line.startswith('#')
                ]
                return addresses
        except Exception as e:
            print(f"Error reading file: {e}")
            raise

    async def write_file_async(self, filename, response):
        """Асинхронная запись файла"""
        try:
            lines = [f"{match.host} - {match.subnet}" for match in response.matches]
            content = '\n'.join(lines)
            
            async with aiofiles.open(filename, 'w') as f:
                await f.write(content)
                
        except Exception as e:
            print(f"Error writing file: {e}")
            raise

    async def process_file(self, input_file, output_file):
        """Асинхронная обработка файла"""
        try:
            # Чтение файла
            print(f"Read {input_file}")
            addresses = await self.read_file_async(input_file)
            
            if not addresses:
                print("No addresses found in input file")
                return
            
            print(f"🔢 Found {len(addresses)} addresses")
            
            # Подключение к RabbitMQ
            connected = await self.connect()
            if not connected:
                print("Failed to connect to RabbitMQ")
                return

            # Отправка запроса
            print("Processing addresses...")
            response = await self.send_request(addresses)

            # Запись результатов
            print(f"Writing results to {output_file}...")
            await self.write_file_async(output_file, response)

            # Вывод результатов
            print("\nResults:")
            for match in response.matches:
                print(f"   {match.host} - {match.subnet}")

            print(f"\nSuccessfully processed {len(response.matches)} matches")

        except Exception as e:
            print(f"Error: {e}")

    async def close(self):
        """Закрытие соединения"""
        if self.connection:
            await self.connection.close()
        print("👋 Client disconnected")

async def main():
    if len(sys.argv) != 3:
        print("Usage: python client.py <input_file> <output_file>")
        print("Example: python client.py input.txt output.txt")
        return
    
    client = AsyncIPMatcherClient()
    
    try:
        await client.process_file(sys.argv[1], sys.argv[2])
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
