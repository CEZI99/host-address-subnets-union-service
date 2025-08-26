FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходные файлы
COPY . .

# Копируем только .proto файл
COPY proto/matcher.proto proto/

# Компилируем protobuf
RUN python -m grpc_tools.protoc \
    -I=./proto \
    --python_out=./proto \
    ./proto/matcher.proto

CMD ["python", "server.py"]