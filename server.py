import asyncio
import aio_pika
import ipaddress
import os
from concurrent.futures import ThreadPoolExecutor
from proto.matcher_pb2 import MatchRequest, MatchResponse, HostSubnetPair

class AsyncIPMatcherServer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.running = False
        self.rabbit_user = os.getenv("RABBITMQ_USER")
        self.rabbit_pass = os.getenv("RABBITMQ_PASSWORD")


    async def connect(self):
        """Асинхронное подключение к RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(
                f"amqp://{self.rabbit_user}:{self.rabbit_pass}@rabbitmq/",
                timeout=10
            )
            self.channel = await self.connection.channel()

            # Устанавливаем QoS для балансировки нагрузки
            await self.channel.set_qos(prefetch_count=20)

            # Объявляем очереди
            self.request_queue = await self.channel.declare_queue(
                'match_requests', durable=True, auto_delete=False
            )
            await self.channel.declare_queue(
                'match_responses', durable=True, auto_delete=False
            )

            print("Connected to RabbitMQ successfully")
            return True

        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def find_best_subnet_sync(self, host_ip, subnets):
        """Синхронный поиск подсети (выполняется в thread pool)"""
        best_subnet = None
        best_prefix_len = -1

        for subnet_str in subnets:
            try:
                subnet = ipaddress.ip_network(subnet_str, strict=False)
                if host_ip in subnet and subnet.prefixlen > best_prefix_len:
                    best_subnet = subnet_str
                    best_prefix_len = subnet.prefixlen
            except (ValueError, ipaddress.AddressValueError):
                continue

        return best_subnet if best_subnet else "0"

    async def process_single_host_async(self, host, subnets):
        """Асинхронная обработка одного хоста"""
        loop = asyncio.get_event_loop()
        try:
            host_ip = ipaddress.ip_address(host)
            best_subnet = await loop.run_in_executor(
                self.executor, self.find_best_subnet_sync, host_ip, subnets
            )
            return (host, best_subnet)
        except (ValueError, ipaddress.AddressValueError):
            return (host, "0")

    async def process_message(self, message: aio_pika.IncomingMessage):
        """Асинхронная обработка сообщения"""
        async with message.process():
            try:
                # Десериализуем запрос
                request = MatchRequest()
                request.ParseFromString(message.body)

                print(f"Received request with {len(request.addresses)} addresses")

                # Разделяем адреса
                hosts = []
                subnets = []

                for address in request.addresses:
                    if '/' in address:
                        subnets.append(address)
                    else:
                        hosts.append(address)

                print(f"Processing {len(hosts)} hosts against {len(subnets)} subnets")

                # Создаем асинхронные задачи для каждого хоста
                tasks = [
                    self.process_single_host_async(host, subnets)
                    for host in hosts
                ]

                # Параллельная обработка
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Формируем ответ
                response = MatchResponse()
                for result in results:
                    if not isinstance(result, Exception) and result:
                        host, subnet = result
                        response.matches.append(HostSubnetPair(host=host, subnet=subnet))

                # Отправляем ответ
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=response.SerializeToString(),
                        correlation_id=message.correlation_id,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key='match_responses'
                )

                print(f"✅ Processed {len(response.matches)} matches")

            except Exception as e:
                print(f"Error processing message: {e}")
                # Можно отправить сообщение об ошибке
                error_response = MatchResponse()
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=error_response.SerializeToString(),
                        correlation_id=message.correlation_id
                    ),
                    routing_key='match_responses'
                )

    async def start_consuming(self):
        """Запуск потребителя"""
        await self.request_queue.consume(self.process_message)
        print("Server started consuming messages...")

    async def start(self):
        """Запуск сервера"""
        print("Connecting to RabbitMQ")
        connected = await self.connect()
        if not connected:
            print("Failed to connect, retrying in 5 seconds")
            await asyncio.sleep(5)
            await self.start()
            return

        await self.start_consuming()
        self.running = True
        print("Async IP Matcher Server started successfully!")

    async def stop(self):
        """Остановка сервера"""
        self.running = False
        if self.connection:
            await self.connection.close()
        self.executor.shutdown()
        print("Server stopped")

async def main():
    server = AsyncIPMatcherServer()
    try:
        await server.start()
        # Бесконечное ожидание с проверкой соединения
        while server.running:
            await asyncio.sleep(1)
            if not server.connection or server.connection.is_closed:
                print("🔌 Connection lost, reconnecting...")
                await server.stop()
                await asyncio.sleep(2)
                await server.start()

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())
